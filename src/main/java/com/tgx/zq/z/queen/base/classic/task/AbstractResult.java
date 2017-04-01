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

import java.util.HashMap;
import java.util.Map;

import com.tgx.zq.z.queen.base.classic.task.inf.ITaskResult;

public abstract class AbstractResult
        implements
        ITaskResult
{
    protected volatile boolean  disposable = true;
    protected byte              handleCount;
    private volatile boolean    isInResponseQueue;
    private int                 bindSerial;
    private volatile Exception  exception;
    private boolean             onceHandle = true;
    private Map<String, Object> attributes;

    /**
     * 任何获得isDisposable()接口许可 或者 在ResponseQueue中未经处理的Result都将默认执行此操作
     */
    @Override
    public void dispose() {
        exception = null;
        if (attributes != null) attributes.clear();
        attributes = null;
    }

    @Override
    public final void lockResponse() {
        isInResponseQueue = true;
        handleCount = 0;
    }

    @Override
    public final void unlockResponse() {
        isInResponseQueue = false;
    }

    @Override
    public final boolean isResponsed() {
        return isInResponseQueue;
    }

    @Override
    public final void onceOnly() {
        onceHandle = true;
    }

    @Override
    public boolean needHandle() {
        return !onceHandle || (onceHandle && ++handleCount == 1);
    }

    @Override
    public final int getListenSerial() {
        return bindSerial;
    }

    @Override
    public final void setListenSerial(int bindSerial) {
        this.bindSerial = bindSerial;
    }

    @Override
    public boolean isDisposable() {
        return disposable;
    }

    @Override
    public final boolean hasError() {
        return exception != null;
    }

    @Override
    public final Exception getError() {
        return exception;
    }

    @Override
    public final void setError(Exception ex) {
        exception = ex;
    }

    @Override
    public final void setAttributes(Map<String, Object> map) {
        attributes = map;
    }

    @Override
    public final Object getAttribute(String key) {
        if (attributes == null || attributes.isEmpty()) return null;
        return attributes.get(key);
    }

    @Override
    public final void setAttribute(String key, Object value) {
        if (attributes == null) attributes = new HashMap<String, Object>(2, 0.5f);
        attributes.put(key, value);
    }

    @Override
    public boolean canOtherHandle() {
        return true;
    }

    /**
     * 任务编号 0x80010000-0xFFFE0000 <br>
     * 结果类型 0x00010000 - 0x7FFE0000 <br>
     */
    @Override
    public int getSuperSerialNum() {
        return 0x7FFF0000;
    }
}
