/*
 * MIT License
 *
 * Copyright (c) 2017 Z-Chess
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */
package com.tgx.zq.z.queen.base.classic.task;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import com.tgx.zq.z.queen.base.classic.task.inf.ITaskProgress;
import com.tgx.zq.z.queen.base.classic.task.inf.ITaskResult;
import com.tgx.zq.z.queen.base.classic.task.inf.ITaskRun;
import com.tgx.zq.z.queen.base.classic.task.inf.ITaskTimeout;
import com.tgx.zq.z.queen.base.classic.task.inf.ITaskWakeTimer;

/**
 * Task 的生命周期 <br>
 * 1:{@code initTask()}; -> TaskService.processor 中执行 并在此进行任务执行空间的分发,是否进入Excutor来执行.由isBlock属性来决定 <br>
 * 2:{@code run()};<br>
 * 3:{@code afterRun()};<br>
 * 4:{@code finish()};<br>
 * -- Exception<br>
 * 5:{@code doAfterException()};<br>
 * 6:{@code notifyObserver()}; 尽在 TaskService.processor 中使用<br>
 * 
 *
 */
public abstract class Task
        extends
        AbstractResult
        implements
        ITaskRun
{
    protected final transient ReentrantLock runLock;
    protected final transient Condition     available;
    public volatile int                     timeLimit, timeOut, threadId, priority;
    public Object                           attachment;
    public ITaskTimeout<Task>               timeoutCall;
    public ITaskWakeTimer                   wakeTimer;
    /**
     * isBlocker: the task will block thread isCycle: the task will run again without done isPending: the task is running isSegment: the
     * task will run in multi section isInit: the task is initiliazed isProxy: the task is other task's runner
     */
    protected volatile boolean              isDone, isBlocker, isCycle, isPending, isSegment, isProxy, isInit, discardPre, wakeLock;
    /* milliseconds */
    protected volatile long                 doTime, offTime;
    protected transient TaskService         scheduleService;
    protected Thread                        holdThread;
    protected volatile boolean              needAlarm;
    protected ITaskProgress                 progress;
    volatile int                            inQueueIndex;
    private byte                            retry;
    /** 为timeOut所使用的启动时间 */
    private long                            startTime;
    private volatile boolean                disable = false;
    private volatile int                    toScheduled;

    public Task(int threadId) {
        this(threadId, null);
    }

    public Task(int threadId, ITaskProgress progress) {
        super();
        this.threadId = threadId;
        this.progress = progress;
        runLock = new ReentrantLock();
        available = runLock.newCondition();
    }

    /**
     * 当任务需要处理同步条件的时候将需要此方法进行操作
     *
     * @return
     */
    public final ReentrantLock getLock() {
        if (runLock == null) throw new NullPointerException();
        return runLock;
    }

    public final void setPriority(int priority) {
        if (priority != this.priority) {
            this.priority = priority;
            scheduleService.mainQueue.replace(this);
        }
    }

    /**
     * 任务超时以秒计算,>0代表超时时间有效,0将无视超时设计 当超时操作生效时将执行callback接口
     *
     * @param second
     * @param callBack
     */
    @SuppressWarnings("unchecked")
    public final void timeOut(int second, ITaskTimeout<?> callBack) {
        if (second < 0) throw new IllegalArgumentException("second must > 0");
        timeOut = second;
        timeoutCall = (ITaskTimeout<Task>) callBack;
    }

    /**
     * 当前时间作为任务启动计时行为的时刻
     *
     *
     * @see #setStartTime(long)
     */
    public final void setStartTime() {
        setStartTime(System.currentTimeMillis());
    }

    /**
     *
     * @param RTC_WakeTime
     *            当需要进行绝对时间唤醒时执行此方法 只能在initTask方法之后执行.
     */
    public final void setAlarmTime(long RTC_WakeTime) {
        if (!isInit) return;
        wakeTimer = scheduleService.setTaskAlarmTime(RTC_WakeTime, wakeTimer, this);
        needAlarm = true;
    }

    @Override
    public void interrupt() {
        Thread thread = getHoldThread();
        if (thread != null && thread.isAlive() && !thread.isInterrupted()) {
            try {
                thread.interrupt();
            }
            catch (Exception e) {
                // #debug warn
                e.printStackTrace();
            }
        }
    }

    @Override
    public void wakeUp() {
    }

    @Override
    public boolean needAlarm() {
        return needAlarm;
    }

    /**
     * 设置当前任务的是否可以离开队列
     * 
     *
     * @return >=0允许离开队列
     */
    final boolean isToSchedule() {
        return (toScheduled | (getDelay(TimeUnit.MILLISECONDS) > 0 ? 0x80000000 : 0x40000000)) > 0;
    }

    final boolean isToSchedule(long nano) {
        return (toScheduled | (nano > 0 ? 0x80000000 : 0x40000000)) > 0;
    }

    final void intoScheduleQueue() {
        toScheduled += (toScheduled & 0x00FFFFFF) == 0x00FFFFFF ? 2 : 1;
        toScheduled |= 0x02000000;
        toScheduled &= 0x02FFFFFF;
    }

    final void outScheduleQueue() {
        toScheduled &= 0x00FFFFFF;
    }

    public final boolean isInQueue() {
        return (toScheduled & 0x02000000) != 0;
    }

    /**
     * @param duration
     *            延迟多少时间单位执行
     * @param timeUnit
     *            时间单位
     */
    protected final boolean setDelay(long duration, TimeUnit timeUnit) {
        if (duration <= 0) return false;
        if (isInQueue()) // 不必判断ScheduleQueue的NULL
        {
            if (runLock.tryLock()) try {
                offTime = System.currentTimeMillis() + TimeUnit.MILLISECONDS.convert(duration, timeUnit) - doTime;
                return true;
            }
            finally {
                runLock.unlock();
            }
            return false;
        }
        else
        // 尚未进入调度系统
        {
            doTime = System.currentTimeMillis() + TimeUnit.MILLISECONDS.convert(duration, timeUnit);
            return true;
        }
    }

    public final long getDelay(TimeUnit unit) {
        return unit.convert(doTime - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    }

    public final long getDoTime() {
        return doTime;
    }

    public final void setDone() {
        isDone = true;
    }

    public final boolean isBlocker() {
        return isBlocker;
    }

    public final boolean isDisable() {
        return isCancelled();
    }

    public final boolean isPending() {
        return isPending;
    }

    public final boolean isDiscardPre() {
        return discardPre;
    }

    public final boolean isDone() {
        return isDone;
    }

    public final boolean isProxy() {
        return isProxy;
    }

    public final boolean isInit() {
        return isInit;
    }

    public final boolean isCycle() {
        return isCycle;
    }

    public final void setRetryLimit(int limit) {
        retry &= 0x0F;
        retry |= (limit & 0x0F) << 4;
    }

    public void retry() {
        if (isInQueue()) return;
        else if (!isPending) {
            setError(null);
            doTime = 0;
            retry += (retry & 0x0F) < 0x0F ? 1 : 0;
            isDone = false;
            isPending = false;
            isInit = false;
        }
    }

    public final int getCurRetry() {
        return retry & 0x0F;
    }

    public final boolean canRetry() {
        return (retry >> 4) > (retry & 0x0F);
    }

    public void retry(long duration, TimeUnit timeUnit) {
        retry();
        setDelay(duration, timeUnit);
    }

    public boolean reOpen(Object attach) {
        if (!isDone) return false;
        attach(attach);
        retry();
        return true;
    }

    public final Thread getHoldThread() {
        return holdThread;
    }

    protected void clone(Task task) {
        threadId = task.threadId;
        retry = (byte) (task.retry & 0xF0);
        timeLimit = task.timeLimit;
        progress = task.progress;
        timeOut = task.timeOut;
        timeoutCall = task.timeoutCall;
    }

    public final long getStartTime() {
        return startTime;
    }

    /**
     *
     * @param start
     *            任务启动开始进入timeout计时行为<br>
     *            注意:时间起点由任务的特定超时行为规约,并不等价于任务开始执行的时刻
     */
    public final void setStartTime(long start) {
        startTime = start;
    }

    /**
     * 子类在实现本类时，资源释放不必要全面完成，只需要将自身的资源完成释放即可 其余的资源由资源自身进行释放
     */
    @Override
    public void dispose() {
        if (timeoutCall != null && timeoutCall.isEnabled()) timeoutCall.onInvalid(this);
        timeoutCall = null;
        attachment = null;
        holdThread = null;
        progress = null;
        holdThread = null;
        scheduleService = null;
        if (wakeTimer != null && wakeTimer.isDisposable()) wakeTimer.dispose();
        wakeTimer = null;
        super.dispose();
    }

    protected boolean segmentBreak() {
        return isSegment;
    }

    /**
     * 此方法在Processor线程中执行. isBlocker 特性可以在此方法内设置为true 但是此方法不可进行任何可能导致Throwable的操作
     */
    @Override
    public void initTask() {
        isInit = true;
    }

    /**
     * before task.run(); 当isBlocker为true时 此方法将只执行一次.无论isCycle或isSegment如何设置.
     *
     * @throws Exception
     */
    protected void beforeRun() throws Exception {
        holdThread = Thread.currentThread();
        isPending = true;
    }

    /**
     * after task.finish(); 可能会由于run方法的exception而无法执行到. 当isBlocker为true时，此方法只会执行一次.无论isCycle或isSegment如何设置.
     *
     * @throws Exception
     */
    protected void afterRun() throws Exception {
    }

    public final void commitResult(ITaskResult result, CommitAction action) {
        commitResult(result, action, getListenSerial());
    }

    public final void commitResult(ITaskResult result) {
        commitResult(result, CommitAction.NO_WAKE_UP);
    }

    @Override
    public final void commitResult(ITaskResult result, CommitAction action, int listenerBind) {
        if (scheduleService == null) return;
        if (result.getListenSerial() == 0) result.setListenSerial(listenerBind);
        boolean responsed = scheduleService.responseTask(result);
        if (responsed && isBlocker && CommitAction.WAKE_UP.equals(action)) scheduleService.wakeUp();
    }

    public final void attach(Object object) {
        attachment = object;
    }

    public final Object getAttach() {
        return attachment;
    }

    protected void finish() {
        if (!isProxy && !isCycle && !isSegment) {
            isDone = true;
            isPending = false;
        }
        else if (!isDone && (isSegment || (isCycle && !isBlocker))) isPending = false;
    }

    @Override
    public void finishTask() {
        if (isDisposable()) dispose();
    }

    public boolean isDisposable() {
        return (isDone || isCancelled()) && disposable;
    }

    protected void finishThreadTask() {
    }

    protected void doAfterException() {
        if (progress != null) progress.finishProgress(ITaskProgress.TaskProgressType.error);
    }

    public final ITaskProgress getProgress() {
        return progress;
    }

    public final void setProgress(ITaskProgress progress) {
        this.progress = progress;
    }

    public final int getInQueueIndex() {
        return inQueueIndex;
    }

    @Override
    public final boolean isCancelled() {
        return disable;
    }

    @Override
    public final void cancel() {
        disable = true;
    }

    /**
     * 任务编号 0x80010000-0xFFFE0000 <br>
     * 结果类型 0x00 - 0x7FFFFFFF <br>
     */
    @Override
    public int getSuperSerialNum() {
        return 0xFFFF0000;
    }
}
