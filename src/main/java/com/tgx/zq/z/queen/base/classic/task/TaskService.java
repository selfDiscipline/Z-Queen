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

import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.AbstractQueuedSynchronizer;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import com.tgx.zq.z.queen.base.classic.task.TaskService.Executor.TgxBlockingQueue;
import com.tgx.zq.z.queen.base.classic.task.inf.ITaskListener;
import com.tgx.zq.z.queen.base.classic.task.inf.ITaskProgress;
import com.tgx.zq.z.queen.base.classic.task.inf.ITaskResult;
import com.tgx.zq.z.queen.base.classic.task.inf.ITaskRun;
import com.tgx.zq.z.queen.base.classic.task.inf.ITaskRun.CommitAction;
import com.tgx.zq.z.queen.base.classic.task.inf.ITaskTimeout;
import com.tgx.zq.z.queen.base.classic.task.inf.ITaskWakeTimer;
import com.tgx.zq.z.queen.base.inf.IDisposable;

/**
 * 此处所使用的线程池模型已于2013-2-11日升级为Doug Lea 撰写的新版本代码 依然提供了对特定线程ID进行内部分发的特性,依然未提供shutdown/shutdownNow方法 虽然已经提供了对应的程序管理与控制函数,但未启用 默认线程存活时间为30s
 * 在多Task.threadID!=0条件下会出现线程池的并发线程过多的问题 请自行在业务逻辑中谨慎使用Task.threadID用以避免业务流同步处理事宜
 *
 *
 * @version 0.9.1
 * @since 2013-2-11
 */

public class TaskService
        implements
        Comparator<Task>,
        IDisposable
{

    final static byte                             SERVICE_TASK_INIT      = -1;
    final static byte                             SERVICE_PROCESSING     = SERVICE_TASK_INIT + 1;
    final static byte                             SERVICE_SCHEDULE       = SERVICE_PROCESSING + 1;
    final static byte                             SERVICE_NOTIFYOBSERVER = SERVICE_SCHEDULE + 1;
    final static byte                             SERVICE_DOWN           = -128;
    final static byte                             LISTENER_INITIAL_NUM   = 7;
    private static final int                      COUNT_BITS             = Integer.SIZE - 3;
    private static final int                      CAPACITY               = (1 << COUNT_BITS) - 1;
    private static final int                      RUNNING                = -1 << COUNT_BITS;
    private static final int                      SHUTDOWN               = 0 << COUNT_BITS;
    private static final int                      STOP                   = 1 << COUNT_BITS;
    private static final int                      TIDYING                = 2 << COUNT_BITS;
    private static final int                      TERMINATED             = 3 << COUNT_BITS;
    protected static AtomicReference<TaskService> _AInstance             = new AtomicReference<TaskService>();
    protected final ScheduleQueue<Task>           mainQueue;
    final Processor                               processor;
    final HashMap<Integer, ITaskListener>         listeners;
    final ConcurrentLinkedQueue<ITaskResult>      responseQueue;
    final ReentrantLock                           mainLock               = new ReentrantLock();
    final ReentrantLock                           runLock                = new ReentrantLock();
    // #debug fatal
    String                                        tag                    = "TS";
    private ITaskListener                         recycleListener;

    private TaskService() {
        listeners = new HashMap<>(LISTENER_INITIAL_NUM);
        mainQueue = new ScheduleQueue<>(this, this);
        responseQueue = new ConcurrentLinkedQueue<>();
        processor = new Processor();
    }

    public static TaskService getInstance(boolean newInstance) {
        if (_AInstance.get() == null) {
            if (newInstance) for (;;) {
                if (_AInstance.get() != null) return _AInstance.get();
                if (_AInstance.compareAndSet(null, new TaskService())) return _AInstance.get();
            }
            else new NullPointerException("No create mService!");
        }
        return _AInstance.get();
    }

    public static TaskService getInstance() {
        return getInstance(true);
    }

    private static int runStateOf(int c) {
        return c & ~CAPACITY;
    }

    private static int workerCountOf(int c) {
        return c & CAPACITY;
    }

    private static int ctlOf(int rs, int wc) {
        return rs | wc;
    }

    private static boolean runStateLessThan(int c, int s) {
        return c < s;
    }

    private static boolean runStateAtLeast(int c, int s) {
        return c >= s;
    }

    private static boolean isRunning(int c) {
        return c < SHUTDOWN;
    }

    /**
     * TaskService 启动函数 在此之前需要将listener都加入到监听队列中
     */
    public final void startService() {
        if (processor.processing) return;
        processor.start();
        Thread.yield();
    }

    public final void stopService() {
        processor.processing = false;
        processor.interrupt();
    }

    protected void setScheduleAlarmTime(long RTC_WakeTime) {
    }

    protected void noScheduleAlarmTime() {

    }

    protected void onProcessorStop() {

    }

    protected ITaskWakeTimer setTaskAlarmTime(long RTC_WakeTime, ITaskWakeTimer owner, ITaskRun task) {
        return null;
    }

    /*
     * priority > 0时没有任何可能相等的情况 if (task1.priority > task2.priority) return -1; if (task1.priority < task2.priority) return 1; if
     * (task1.inQueueIndex < task2.inQueueIndex) result = -1; if (task1.inQueueIndex > task2.inQueueIndex) result = 1; if (task1.doTime ==
     * task2.doTime) return result; if (task1.doTime < task2.doTime) return -1; if (task1.doTime > task2.doTime) return 1; return result;
     * @see java.util.Comparator#compare(java.lang.Object, java.lang.Object)
     */
    @Override
    public final int compare(Task task1, Task task2) {
        int result = 0;
        long dT = task1.doTime - task2.doTime;
        result = dT < 0 ? -1 : dT > 0 ? 1 : 0;
        if (result != 0) return result;
        // formatter:off
        result = task1.priority > task2.priority ? -1
                                                 : task1.priority < task2.priority ? 1
                                                                                   : task1.inQueueIndex < task2.inQueueIndex ? -1
                                                                                                                             : task1.inQueueIndex > task2.inQueueIndex ? 1
                                                                                                                                                                       : 0;
        // formatter:on
        if (result == 0) result = task1.hashCode() - task2.hashCode();
        return result;
    }

    public final boolean addListener(ITaskListener listener) {
        if (listener == null) throw new NullPointerException();
        ReentrantLock mainLock = this.mainLock;
        if (mainLock.tryLock()) try {
            Set<Integer> keySet = listeners.keySet();
            if (listener.getBindSerial() == 0 || keySet.contains(listener.getBindSerial())) { return false; }
            listeners.put(listener.getBindSerial(), listener);
            return true;
        }
        finally {
            mainLock.unlock();
        }
        return false;
    }

    public final void forceAddListener(ITaskListener listener) {
        if (listener == null) throw new NullPointerException();
        ReentrantLock mainLock = this.mainLock;
        mainLock.lock();
        try {
            Set<Integer> keySet = listeners.keySet();
            if (listener.getBindSerial() == 0 || keySet.contains(listener.getBindSerial())) {
                System.err.println(listener.getClass().getSimpleName()
                                   + "@"
                                   + listener.hashCode()
                                   + " bindSerial error : "
                                   + listener.getBindSerial());
            }
            listeners.put(listener.getBindSerial(), listener);
        }
        finally {
            mainLock.unlock();
        }
    }

    public final void setRecycle(ITaskListener recycle) {
        this.recycleListener = recycle;
    }

    public final ITaskListener removeListener(int bindSerial) {
        if (bindSerial == 0) return null;
        ReentrantLock mainLock = this.mainLock;
        if (mainLock.tryLock()) try {
            return listeners.remove(bindSerial);
        }
        finally {
            mainLock.unlock();
        }
        return null;
    }

    public final ITaskListener forceRemoveListener(int bindSerial) {
        if (bindSerial == 0) return null;
        ReentrantLock mainLock = this.mainLock;
        mainLock.lock();
        try {
            return listeners.remove(bindSerial);
        }
        finally {
            mainLock.unlock();
        }
    }

    // TODO
    public boolean hasThisTask(Task task) {
        return this.mainQueue.hasThis(task);
    }

    public final boolean requestService(Task task, boolean schedule) {
        return requestService(task, schedule, 0);
    }

    public final boolean requestService(Task task, int bindSerial) {
        return requestService(task, false, bindSerial);
    }

    /**
     * @param task
     *            正在执行和已经完成的任务不能再次进入任务队列
     * @param schedule
     *            是否将任务装入队列头
     * @param listenerSerial
     *            绑定的处理器
     * @return {@code true}成功托管到任务队列 {@code false} 任务已经执行或完成;或者由于队列中已存在相同任务导致加入队列失败
     * @throws NullPointerException
     *             task为{@code null}
     */
    public final boolean requestService(Task task, boolean schedule, int listenerSerial) {
        if (task == null) throw new NullPointerException();
        if (task.isInQueue() || task.isPending || task.isDone || task.isInit) return false;
        task.scheduleService = this;
        task.setListenSerial(listenerSerial);
        ScheduleQueue<Task> mainQueue = this.mainQueue;
        task.priority = schedule ? mainQueue.priorityIncrease.incrementAndGet() : 0;
        boolean success = mainQueue.offer(task);
        return success;
    }

    public final boolean requestService(Task task,
                                        boolean schedule,
                                        int timelimit,
                                        long delayTimeMills,
                                        byte retryLimit,
                                        Object attachment,
                                        ITaskProgress progress,
                                        int timeOutSecound,
                                        ITaskTimeout<?> taskTimeout,
                                        int bindSerial) {
        if (task == null) throw new NullPointerException();
        if (task.isInQueue() || task.isPending || task.isDone || task.isInit) return false;
        task.timeLimit = timelimit;
        task.setRetryLimit(retryLimit);
        task.attachment = attachment;
        task.progress = progress;
        task.setDelay(delayTimeMills, TimeUnit.MILLISECONDS);
        task.timeOut(timeOutSecound, taskTimeout);
        return requestService(task, schedule, bindSerial);
    }

    public final boolean requestServiceRetry(Task task, int timeOutSecond) {
        if (task == null) throw new NullPointerException();
        task.timeOut(timeOutSecond, task.timeoutCall);
        return requestService(task, false, task.getListenSerial());
    }

    public final boolean requestService(Task task, long delayTime, int bindSerial) {
        return requestService(task, false, -1, delayTime, (byte) 0, null, null, 0, null, bindSerial);
    }

    public final long requestTimerService(Task task, long duration, TimeUnit timeUnit, int bindSerial) {
        return requestService(task, timeUnit.convert(duration, TimeUnit.MILLISECONDS), bindSerial) ? task.getDoTime() : -1;
    }

    public final long requestTimerService(Task task, int bindSerial) {
        return requestService(task, bindSerial) ? task.getDoTime() : -1;
    }

    /**
     * @param threadID
     * @param available
     * @param schedule
     * @return
     */
    public final boolean cancelService(int threadID, int available, boolean schedule) {
        if (processor == null || processor.executor == null) return true;// 没有需要关闭的服务，恒返回true
        if (threadID != 0) {
            final ReentrantLock lock = this.mainLock;
            lock.lock();
            try {
                TgxBlockingQueue btQueue = processor.executor.id2Queue.get(threadID);
                if (btQueue != null) btQueue.clear();
            }
            finally {
                lock.unlock();
            }
        }
        return false;
    }

    /**
     * @param taskResult
     *            将要入队的运行结果 检验是否为TaskResult的实现
     * @return 入队成功 结果
     *
     * @since 2011-6-10 接口变更,取消存在性检查,迭代过程本身效能太低
     */
    final boolean responseTask(ITaskResult taskResult) {
        if (taskResult == null) return false;
        if (taskResult.isResponsed() || taskResult.isCancelled()) return false;
        taskResult.lockResponse();
        boolean offered = responseQueue.offer(taskResult);
        if (!offered) taskResult.unlockResponse();
        return offered;
    }

    public final void commitNotify() {
        wakeUp();
    }

    protected final void wakeUp() {
        if (processor != null && !processor.isInterrupted()) try {
            processor.interrupt();
        }
        catch (Exception e) {}
    }

    private final void notifyObserver() {
        final ReentrantLock mainLock = this.mainLock;
        mainLock.lock();
        ITaskResult receive = null;
        boolean isHandled = false;
        try {
            while (!responseQueue.isEmpty()) {
                receive = responseQueue.poll();
                receive.unlockResponse();
                if (receive.isCancelled() || !receive.needHandle()) continue;
                isHandled = false;
                if (!listeners.isEmpty()) {
                    int listenSerial = receive.getListenSerial();
                    if (listenSerial != 0) {
                        ITaskListener listener = listeners.get(listenSerial);
                        if (listener != null && listener.isEnable()) try {
                            isHandled = isHandled(receive, listener);
                        }
                        catch (Exception e) {
                            // #debug warn
                            e.printStackTrace();
                            // 防止回调出错,而导致Processor线程被销毁的风险,忽略任何在回调过程中产生的错误
                        }
                        finally {
                            if (receive.isDisposable() && isHandled) receive.dispose();
                        }
                        else {}
                    }
                    if (!isHandled && receive.canOtherHandle()) for (ITaskListener listener : listeners.values()) {
                        if (!listener.isEnable()) continue;
                        try {
                            isHandled = isHandled(receive, listener);
                        }
                        catch (Exception e) {
                            // #debug warn
                            e.printStackTrace();
                            // 防止回调出错,而导致Processor线程被销毁的风险,忽略任何在回调过程中产生的错误
                        }
                        finally {
                            if (receive.isDisposable() && isHandled) receive.dispose();
                        }
                        if (isHandled) break;
                    }
                }
                if (!isHandled && recycleListener != null) try {
                    isHandled = isHandled(receive, recycleListener);
                }
                catch (Exception e) {
                    // #debug warn
                    e.printStackTrace();
                    // 防止回调出错,而导致Processor线程被销毁的风险,忽略任何在回调过程中产生的错误
                }
                finally {
                    if (receive.isDisposable() || !isHandled) receive.dispose();
                }
            }
        }
        finally {
            mainLock.unlock();
        }
    }

    private final boolean isHandled(ITaskResult receive, ITaskListener listener) {
        int result = receive.getSerialNum();
        if (result == Integer.MIN_VALUE || result == Integer.MAX_VALUE) return false;
        return receive.hasError() ? listener.exCaught(receive, this) : listener.handleResult(receive, this);
    }

    protected IWakeLock getWakeLock() {
        return null;
    }

    @Override
    public void dispose() {
        _AInstance.set(null);
    }

    public interface IWakeLock
    {
        void acquire();

        void release();

        void initialize(String lockName);
    }

    private final class Processor
            extends
            Thread
            implements
            IDisposable
    {
        volatile boolean processing;
        Executor         executor;

        public Processor() {
            setName("taskService-processor");
        }

        @Override
        public final void dispose() {
            executor = null;
        }

        public final void run() {
            processing = true;
            final ScheduleQueue<Task> mainQueue = TaskService.this.mainQueue;
            Task curTask = null;
            byte serviceState = SERVICE_SCHEDULE;
            mainLoop:
            while (processing) {
                processor_switch:
                {
                    switch (serviceState) {
                        case SERVICE_TASK_INIT:
                            curTask.initTask();
                            if (curTask.isBlocker()) {
                                if (executor == null) executor = new Executor(mainQueue);
                                executor.execute(curTask);
                                curTask = null;
                                serviceState = SERVICE_SCHEDULE;
                                break processor_switch;
                            }
                            serviceState = SERVICE_PROCESSING;
                        case SERVICE_PROCESSING:
                            try {
                                curTask.beforeRun();
                                curTask.run();
                                curTask.finish();
                                curTask.afterRun();
                            }
                            catch (Exception e) {
                                // #debug warn
                                e.printStackTrace();
                                curTask.setError(e);
                                curTask.doAfterException();
                                curTask.setDone();
                                curTask.commitResult(curTask);
                            }
                            finally {
                                /*
                                 * 每次任务执行结束之前都检查一下responseQueue是否有内容需要处理
                                 */
                                notifyObserver();
                                if (!curTask.hasError()) curTask.finishTask();// 如果curTask不携带错误信息将执行finishTask操作
                                if (!curTask.isDone && !curTask.isPending) {
                                    curTask.priority = 0;
                                    mainQueue.offer(curTask);// 任务未完成且的重新进入队列执行,
                                }
                                curTask = null;
                                serviceState = SERVICE_SCHEDULE;
                            }
                        case SERVICE_SCHEDULE:
                            try {
                                curTask = mainQueue.take();
                            }
                            catch (InterruptedException ie) {}
                            catch (Exception e) {}
                            if (curTask != null) {
                                if (curTask.isDone || curTask.isCancelled()) {
                                    curTask = null;
                                    continue mainLoop;
                                }
                                serviceState = SERVICE_TASK_INIT;
                                break processor_switch;
                            }
                        case SERVICE_NOTIFYOBSERVER:
                            notifyObserver();
                            serviceState = SERVICE_SCHEDULE;
                            break processor_switch;
                    }
                }
            }
            mainQueue.clear();
            onProcessorStop();
        }
    }

    final class Executor
    {
        static final boolean                     ONLY_ONE    = true;
        final AtomicInteger                      ctl         = new AtomicInteger(ctlOf(RUNNING, 0));
        final AtomicInteger                      qtl         = new AtomicInteger(0);
        final ReentrantLock                      mainLock    = new ReentrantLock();
        final Condition                          termination = mainLock.newCondition();
        final BlockingQueue<Task>                workQueue;
        final ScheduleQueue<Task>                mainQueue;
        final HashSet<Worker>                    workers     = new HashSet<Worker>();
        final HashMap<Integer, TgxBlockingQueue> id2Queue    = new HashMap<Integer, TgxBlockingQueue>();
        long                                     completedTaskCount;
        volatile boolean                         allowCoreThreadTimeOut;
        volatile int                             corePoolSize;
        volatile ThreadFactory                   threadFactory;
        volatile int                             maximumPoolSize;
        volatile long                            keepAliveTime;
        int                                      largestPoolSize;

        public Executor(ScheduleQueue<Task> mainQueue) {
            threadFactory = new WorkerFactory();
            corePoolSize = 0;
            allowCoreThreadTimeOut = true;
            maximumPoolSize = 1024;
            keepAliveTime = TimeUnit.SECONDS.toNanos(100);
            workQueue = new SynchronousQueue<Task>();
            this.mainQueue = mainQueue;
        }

        private boolean compareAndIncrementWorkerCount(int expect) {
            return ctl.compareAndSet(expect, expect + 1);
        }

        private boolean compareAndDecrementWorkerCount(int expect) {
            return ctl.compareAndSet(expect, expect - 1);
        }

        private void decrementWorkerCount() {
            while (!compareAndDecrementWorkerCount(ctl.get()))
                ;
        }

        public void execute(Task task) {
            if (task == null) throw new NullPointerException();
            TgxBlockingQueue tBQueue = null;
            if (task.threadId != 0) {
                final ReentrantLock lock = this.mainLock;
                lock.lock();
                try {
                    tBQueue = id2Queue.get(task.threadId);
                    if (tBQueue == null) {
                        tBQueue = new TgxBlockingQueue();
                        id2Queue.put(task.threadId, tBQueue);
                        qtl.incrementAndGet();
                    }
                    if (tBQueue.worker != null) // ID队列存在worker则向队里中offer task
                    {
                        if (tBQueue.offer(task)) tBQueue.worker.thread.interrupt();
                        else reject(task);
                        return;
                    }
                    else {
                        exec(task, tBQueue);
                    }
                }
                finally {
                    lock.unlock();
                }
            }
            else exec(task, null);
        }

        private void exec(Task task, TgxBlockingQueue tBQueue) {
            int c = ctl.get();
            if (workerCountOf(c) < corePoolSize) {
                if (addWorker(task, tBQueue, true)) return;// 无关性任务可不进入主分发队列直接执行
                c = ctl.get();
            }
            if (isRunning(c) && workQueue.offer(task)) {
                int recheck = ctl.get();
                if (!isRunning(recheck) && remove(task)) reject(task);
                else if (workerCountOf(recheck) == 0 || workerCountOf(recheck) <= qtl.get()) addWorker(null, null, false);// 无worker
                                                                                                                          // 或worker都被已知的ID-Worker占用时
            }
            else if (!addWorker(task, tBQueue, false)) reject(task); // 无关任务直接添加Worker失效时回调reject
        }

        private void reject(Task task) {
            boolean success = mainQueue.offer(task);
            if (!success) System.err.println("reject task failed! " + task + " | " + mainQueue);
        }

        private Task getTask(Worker w) {
            boolean timedOut = false; // Did the last poll() time out?
            retry:
            for (;;) {
                int c = ctl.get();
                int rs = runStateOf(c);

                // Check if queue empty only if necessary.
                if (rs >= SHUTDOWN && (rs >= STOP || (workQueue.isEmpty() && w.taskBlkQueue != null && w.taskBlkQueue.isEmpty()))) {
                    decrementWorkerCount();
                    return null;
                }

                boolean timed; // Are workers subject to culling?

                for (;;) {
                    int wc = workerCountOf(c);
                    timed = allowCoreThreadTimeOut || wc > corePoolSize;
                    if (wc <= maximumPoolSize && !(timedOut && timed)) break;
                    if (compareAndDecrementWorkerCount(c)) return null;
                    c = ctl.get(); // Re-read ctl
                    if (runStateOf(c) != rs) continue retry;
                    // else CAS failed due to workerCount change; retry inner
                    // loop
                }

                try {
                    Task t = null;
                    if (w.taskBlkQueue != null) t = w.taskBlkQueue.poll(keepAliveTime, TimeUnit.NANOSECONDS);
                    if (t == null) t = timed ? workQueue.poll(keepAliveTime, TimeUnit.NANOSECONDS) : workQueue.take();
                    if (t != null) return t;
                    timedOut = true;
                }
                catch (InterruptedException retry) {
                    timedOut = false;
                }
            }
        }

        private void processWorkerExit(Worker w, boolean completedAbruptly) {
            if (completedAbruptly) // If abrupt, then workerCount wasn't
                                   // adjusted
                                  decrementWorkerCount();
            final ReentrantLock mainLock = this.mainLock;
            mainLock.lock();
            try {
                completedTaskCount += w.completedTasks;
                workers.remove(w);
            }
            finally {
                mainLock.unlock();
            }
            tryTerminate();
            int c = ctl.get();
            if (runStateLessThan(c, STOP)) {
                if (!completedAbruptly) {
                    int min = allowCoreThreadTimeOut ? 0 : corePoolSize;
                    if (min == 0 && !workQueue.isEmpty()) min = 1;
                    if (workerCountOf(c) >= min) return; // replacement not
                                                         // needed
                }
                addWorker(null, null, false);
            }
        }

        final void tryTerminate() {
            for (;;) {
                int c = ctl.get();
                if (isRunning(c) || runStateAtLeast(c, TIDYING) || (runStateOf(c) == SHUTDOWN && !workQueue.isEmpty())) return;
                if (workerCountOf(c) != 0) { // Eligible to terminate
                    interruptIdleWorkers(ONLY_ONE);
                    return;
                }

                final ReentrantLock mainLock = this.mainLock;
                mainLock.lock();
                try {
                    if (ctl.compareAndSet(c, ctlOf(TIDYING, 0))) {
                        try {
                            // terminated();
                        }
                        finally {
                            ctl.set(ctlOf(TERMINATED, 0));
                            termination.signalAll();
                        }
                        return;
                    }
                }
                finally {
                    mainLock.unlock();
                }
                // else retry on failed CAS
            }
        }

        public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
            long nanos = unit.toNanos(timeout);
            final ReentrantLock mainLock = this.mainLock;
            mainLock.lock();
            try {
                for (;;) {
                    if (runStateAtLeast(ctl.get(), TERMINATED)) return true;
                    if (nanos <= 0) return false;
                    nanos = termination.awaitNanos(nanos);
                }
            }
            finally {
                mainLock.unlock();
            }
        }

        public boolean remove(Task task) {
            boolean removed = workQueue.remove(task);
            tryTerminate(); // In case SHUTDOWN and now empty
            return removed;
        }

        private boolean addWorker(Task firstTask, TgxBlockingQueue tBQueue, boolean core) {
            retry:
            for (;;) {
                int c = ctl.get();
                int rs = runStateOf(c);

                // Check if queue empty only if necessary.
                if (rs >= SHUTDOWN && !(rs == SHUTDOWN && firstTask == null && !workQueue.isEmpty())) return false;

                for (;;) {
                    int wc = workerCountOf(c);
                    if (wc >= CAPACITY || wc >= (core ? corePoolSize : maximumPoolSize)) return false;
                    if (compareAndIncrementWorkerCount(c)) break retry;
                    c = ctl.get(); // Re-read ctl
                    if (runStateOf(c) != rs) continue retry;
                    // else CAS failed due to workerCount change; retry inner
                    // loop
                }
            }
            boolean workerStarted = false;
            boolean workerAdded = false;
            Worker w = null;
            try {
                final ReentrantLock mainLock = this.mainLock;
                w = new Worker(firstTask, tBQueue);
                final Thread t = w.thread;
                if (t != null) {
                    mainLock.lock();
                    try {
                        // Recheck while holding lock.
                        // Back out on ThreadFactory failure or if
                        // shut down before lock acquired.
                        int c = ctl.get();
                        int rs = runStateOf(c);
                        if (rs < SHUTDOWN || (rs == SHUTDOWN && firstTask == null)) {
                            // precheck that t is startable
                            if (t.isAlive()) throw new IllegalThreadStateException();
                            w.inner4id_task();
                            workers.add(w);
                            int s = workers.size();
                            if (s > largestPoolSize) largestPoolSize = s;
                            workerAdded = true;
                        }
                    }
                    finally {
                        mainLock.unlock();
                    }
                    if (workerAdded) {
                        t.start();
                        workerStarted = true;
                    }
                }
            }
            finally {
                if (!workerStarted) addWorkerFailed(w);
            }
            return workerStarted;
        }

        private void addWorkerFailed(Worker w) {
            final ReentrantLock mainLock = this.mainLock;
            mainLock.lock();
            try {
                if (w != null) workers.remove(w);
                decrementWorkerCount();
                tryTerminate();
            }
            finally {
                mainLock.unlock();
            }
        }

        private void interruptIdleWorkers(boolean onlyOne) {
            final ReentrantLock mainLock = this.mainLock;
            mainLock.lock();
            try {
                for (Worker w : workers) {
                    Thread t = w.thread;
                    if (!t.isInterrupted() && w.tryLock()) {
                        try {
                            t.interrupt();
                        }
                        catch (SecurityException ignore) {}
                        finally {
                            w.unlock();
                        }
                    }
                    if (onlyOne) break;
                }
            }
            finally {
                mainLock.unlock();
            }
        }

        void advanceRunState(int targetState) {
            while (true) {
                int c = ctl.get();
                if (runStateAtLeast(c, targetState) || ctl.compareAndSet(c, ctlOf(targetState, workerCountOf(c)))) break;
            }
        }

        public void shutdown() {
            final ReentrantLock mainLock = this.mainLock;
            mainLock.lock();
            try {
                advanceRunState(SHUTDOWN);
                interruptIdleWorkers();
            }
            finally {
                mainLock.unlock();
            }
            tryTerminate();
        }

        private void interruptIdleWorkers() {
            interruptIdleWorkers(false);
        }

        final void runWorker(Worker w) {
            Thread wt = Thread.currentThread();
            Task task = w.firstTask;
            Task lastTask = task;
            w.firstTask = null;
            w.unlock();
            boolean completedAbruptly = true;
            try {
                workLoop:
                for (;;) {
                    while (task != null || (task = getTask(w)) != null) {
                        lastTask = task;
                        w.lock();
                        if ((runStateAtLeast(ctl.get(), STOP) || (Thread.interrupted() && runStateAtLeast(ctl.get(), STOP)))
                            && !wt.isInterrupted()) wt.interrupt();
                        try {
                            if (task.isCancelled() || task.isDone) continue;
                            if (w.taskThreadId == 0 && task.threadId != 0) {
                                final ReentrantLock lock = mainLock;
                                lock.lock();
                                try {
                                    w.taskThreadId = task.threadId;
                                    TgxBlockingQueue btQueue = id2Queue.get(w.taskThreadId);
                                    btQueue.worker = w;
                                    w.taskBlkQueue = btQueue;
                                }
                                finally {
                                    lock.unlock();
                                }
                            }
                            if (task.wakeLock && w.wakeLock != null) w.wakeLock.acquire();// 并非所有的任务都强制要求cpu
                                                                                          // 唤醒状态保护.
                            try {
                                task.beforeRun();
                                run:
                                for (;;) {
                                    task.run();
                                    task.finish();
                                    if (!task.isDone) {
                                        if (task.getDelay(TimeUnit.MILLISECONDS) > 0 || (task.threadId == 0 && task.isSegment)) {
                                            mainQueue.offer(task);
                                            break run;
                                        }
                                        else {
                                            if (task.isSegment
                                                && task.threadId != 0
                                                && w.taskBlkQueue != null
                                                && !w.taskBlkQueue.isEmpty()) {
                                                w.taskBlkQueue.offer(task);
                                                break run;
                                            }
                                            else continue run;// if
                                                              // (task.isCycle
                                                              // ||
                                                              // task.isSegment)
                                        }
                                    }
                                    break run;
                                }
                                task.afterRun();
                            }
                            catch (Exception x) {
                                task.setError(x);
                                task.doAfterException();
                                task.setDone();
                                task.commitResult(task, CommitAction.WAKE_UP);
                            }
                            finally {
                                if (task.wakeLock && w.wakeLock != null) w.wakeLock.release();
                                if (!task.hasError()) task.finishTask();
                            }
                        }
                        finally {
                            task = null;
                            w.completedTasks++;
                            w.unlock();
                        }
                    }
                    if (w.taskThreadId != 0) {
                        boolean queueEmpty = true;
                        final ReentrantLock lock = this.mainLock;
                        lock.lock();
                        try {
                            queueEmpty = w.taskBlkQueue.isEmpty();
                            if (queueEmpty) {
                                TgxBlockingQueue tBQueue = id2Queue.remove(w.taskThreadId);
                                if (lastTask != null && lastTask.threadId != 0) // finish
                                                                                // this
                                                                                // threadID
                                                                                // --
                                                                                // task
                                                                                // --
                                {
                                    lastTask.finishThreadTask();
                                    lastTask = null;
                                }
                                w.taskThreadId = 0;
                                w.taskBlkQueue = null;
                                if (tBQueue != null) {
                                    tBQueue.worker = null;
                                    qtl.decrementAndGet();
                                }
                            }
                        }
                        finally {
                            lock.unlock();
                        }
                        if (queueEmpty) {
                            retry:
                            for (;;) {
                                int c = ctl.get();
                                int rs = runStateOf(c);

                                for (;;) {
                                    if (compareAndIncrementWorkerCount(c)) break retry;
                                    c = ctl.get(); // Re-read ctl
                                    if (runStateOf(c) != rs) continue retry;
                                    // else CAS failed due to workerCount
                                    // change; retry inner loop
                                }
                            }
                            continue workLoop;
                        }
                    }
                    else break workLoop;
                }
                if (lastTask != null) {
                    lastTask.finishThreadTask();
                    lastTask = null;
                }
                completedAbruptly = false;
            }
            finally {
                processWorkerExit(w, completedAbruptly);
            }
        }

        final class WorkerFactory
                implements
                ThreadFactory
        {
            final AtomicInteger id;

            public WorkerFactory() {
                id = new AtomicInteger(0);
            }

            @Override
            public Thread newThread(Runnable r) {
                if (id.get() == maximumPoolSize) id.set(0);
                return new Thread(r, "Executor-TGX" + "-pool-" + id.getAndIncrement());
            }
        }

        final class TgxBlockingQueue
                extends
                LinkedBlockingQueue<Task>
        {

            private static final long serialVersionUID = 4270902625678142779L;
            Worker                    worker;

            @Override
            public void clear() {
                super.clear();
                worker = null;
            }

        }

        final class Worker
                extends
                AbstractQueuedSynchronizer
                implements
                Runnable
        {
            private static final long serialVersionUID = 3833487996901286223L;

            BlockingQueue<Task>       taskBlkQueue;
            Task                      firstTask;
            Task                      lastTask;
            Task                      _id_task;
            volatile int              taskThreadId;
            volatile long             completedTasks;
            IWakeLock                 wakeLock;
            Thread                    thread;
            boolean                   available;

            public Worker(Task task, TgxBlockingQueue tBQueue) {
                wakeLock = getWakeLock();
                thread = threadFactory.newThread(this);
                if (wakeLock != null && thread != null) wakeLock.initialize("worker#" + thread.getId());
                if (task != null && task.threadId != 0 && tBQueue != null) {
                    taskBlkQueue = tBQueue;
                    tBQueue.worker = this;
                    _id_task = task;
                    firstTask = null;
                }
                else firstTask = task;
            }

            void clear() {
                if (taskBlkQueue != null) taskBlkQueue.clear();
                firstTask = null;
                _id_task = null;
                lastTask = null;
                thread = null;
                wakeLock = null;
            }

            protected boolean isHeldExclusively() {
                return getState() == 1;
            }

            protected boolean tryAcquire(int unused) {
                if (compareAndSetState(0, 1)) {
                    setExclusiveOwnerThread(Thread.currentThread());
                    return true;
                }
                return false;
            }

            protected boolean tryRelease(int unused) {
                setExclusiveOwnerThread(null);
                setState(0);
                return true;
            }

            public void lock() {
                acquire(1);
            }

            public boolean tryLock() {
                return tryAcquire(1);
            }

            public void unlock() {
                release(1);
            }

            public boolean isLocked() {
                return isHeldExclusively();
            }

            @Override
            public void run() {
                runWorker(this);
            }

            public void inner4id_task() {
                if (_id_task != null && taskBlkQueue != null && !taskBlkQueue.offer(_id_task)) throw new IllegalStateException();
                _id_task = null;
            }
        }
    }

}
