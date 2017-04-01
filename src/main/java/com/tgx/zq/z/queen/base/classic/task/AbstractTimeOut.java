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

import com.tgx.zq.z.queen.base.classic.task.inf.ITaskTimeout;

public abstract class AbstractTimeOut<E extends Task>
        implements
        ITaskTimeout<E>
{
    protected boolean enabled = true;

    @Override
    public void cancel() {
        enabled = false;
    }

    @Override
    public boolean isTimeout(long curTime, E task) {
        return TimeUnit.SECONDS.toMillis(task.timeOut) < curTime - task.getStartTime();
    }

    @Override
    public boolean isEnabled() {
        return enabled;
    }

    @Override
    public long toWait(long curTime, E task) {
        return TimeUnit.SECONDS.toMillis(task.timeOut) + task.getStartTime() - curTime;
    }
}