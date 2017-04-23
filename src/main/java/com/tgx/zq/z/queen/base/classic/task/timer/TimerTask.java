/*
 *  MIT License
 *
 *  Copyright (c) 2016~2017 Z-Chess
 *
 *  Permission is hereby granted, free of charge, to any person obtaining a copy
 *  of this software and associated documentation files (the "Software"), to deal
 *  in the Software without restriction, including without limitation the rights
 *  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 *  copies of the Software, and to permit persons to whom the Software is
 *  furnished to do so, subject to the following conditions:
 *
 *  The above copyright notice and this permission notice shall be included in all
 *  copies or substantial portions of the Software.
 *
 *  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 *  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 *  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 *  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 *  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 *  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 *  SOFTWARE.
 *
 */
package com.tgx.zq.z.queen.base.classic.task.timer;

import java.util.concurrent.TimeUnit;

import com.tgx.zq.z.queen.base.classic.task.Task;

public abstract class TimerTask
        extends
        Task
{
    public final static int SuperSerialNum = 0x8F000000;
    private volatile long   mWaitMilliSecond;

    public TimerTask(int delaySecond) {
        this(delaySecond, TimeUnit.SECONDS);
    }

    public TimerTask(long duration, TimeUnit timeUnit) {
        super(0);
        setDelay(duration, timeUnit);
        setWaitTime(timeUnit.toMillis(duration));
    }

    public final void setWaitTime(long waitMilliSecond) {
        mWaitMilliSecond = waitMilliSecond;
    }

    @Override
    public final void initTask() {
        isCycle = true;
        super.initTask();
    }

    @Override
    public final void run() throws Exception {
        if (isCancelled() || doTimeMethod()) setDone();
        else setDelay(mWaitMilliSecond, TimeUnit.MILLISECONDS);
    }

    public final void refresh(int delaySecond) {
        setWaitTime(TimeUnit.SECONDS.toMillis(delaySecond));
    }

    public final void reset() {
        setDelay(mWaitMilliSecond, TimeUnit.MILLISECONDS);
    }

    public final long getDelayDuration() {
        return mWaitMilliSecond;
    }

    /**
     * @return 任务已处理完毕
     */
    protected abstract boolean doTimeMethod();

    @Override
    public int getSuperSerialNum() {
        return SuperSerialNum;
    }
}
