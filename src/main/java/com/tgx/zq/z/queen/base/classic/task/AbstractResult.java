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
package com.tgx.zq.z.queen.base.classic.task;

import java.util.HashMap;
import java.util.Map;

import com.tgx.zq.z.queen.base.classic.task.inf.ITaskResult;

/**
 * @author William.d.zk
 */
public abstract class AbstractResult
        implements
        ITaskResult
{
    protected volatile boolean  disposable  = true;
    protected byte              handleCount;
    private volatile boolean    vIsInResponseQueue;
    private int                 mBindSerial;
    private volatile Exception  vException;
    private boolean             mOnceHandle = true;
    private Map<String, Object> mAttributes;

    /**
     * 任何获得isDisposable()接口许可 或者 在ResponseQueue中未经处理的Result都将默认执行此操作
     */
    @Override
    public void dispose() {
        vException = null;
        if (mAttributes != null) mAttributes.clear();
        mAttributes = null;
    }

    @Override
    public final void lockResponse() {
        vIsInResponseQueue = true;
        handleCount = 0;
    }

    @Override
    public final void unlockResponse() {
        vIsInResponseQueue = false;
    }

    @Override
    public final boolean isResponsed() {
        return vIsInResponseQueue;
    }

    @Override
    public final void onceOnly() {
        mOnceHandle = true;
    }

    @Override
    public boolean needHandle() {
        return !mOnceHandle || (mOnceHandle && ++handleCount == 1);
    }

    @Override
    public final int getListenSerial() {
        return mBindSerial;
    }

    @Override
    public final void setListenSerial(int bindSerial) {
        this.mBindSerial = bindSerial;
    }

    @Override
    public boolean isDisposable() {
        return disposable;
    }

    @Override
    public final boolean hasError() {
        return vException != null;
    }

    @Override
    public final Exception getError() {
        return vException;
    }

    @Override
    public final void setError(Exception ex) {
        vException = ex;
    }

    @Override
    public final void setAttributes(Map<String, Object> map) {
        mAttributes = map;
    }

    @Override
    public final Object getAttribute(String key) {
        if (mAttributes == null || mAttributes.isEmpty()) return null;
        return mAttributes.get(key);
    }

    @Override
    public final void setAttribute(String key, Object value) {
        if (mAttributes == null) mAttributes = new HashMap<>(2, 0.5f);
        mAttributes.put(key, value);
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
