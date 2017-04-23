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
package com.tgx.zq.z.queen.base.classic.task.inf;

import java.util.Map;

import com.tgx.zq.z.queen.base.inf.IDisposable;
import com.tgx.zq.z.queen.base.inf.ISerialTick;

public interface ITaskResult
        extends
        IDisposable,
        ISerialTick
{
    void lockResponse();

    void unlockResponse();

    /**
     * 当前存在队列中的状态
     * 
     *
     * @return true 已进入响应队列
     */
    boolean isResponsed();

    /**
     * 设置当前回调结果只能处理一次
     * 
     * @author william
     */
    void onceOnly();

    /**
     * @author william
     * @return 当前结果是否要被进行处理
     */
    boolean needHandle();

    /**
     *
     * @return 订阅此任务的Listener SerialNum <br>
     *         <code> 0</code> 将顺序提交到所有监听者
     */
    int getListenSerial();

    /**
     *
     * @param bindSerial
     *            当前任务结果绑定的Listener SerialNum <br>
     *            <code> 0</code> 将顺序提交到所有监听者
     */
    void setListenSerial(int bindSerial);

    boolean hasError();

    Exception getError();

    void setError(Exception ex);

    boolean isCancelled();

    void cancel();

    void setAttributes(Map<String, Object> map);

    void setAttribute(String key, Object value);

    Object getAttribute(String key);

    boolean canOtherHandle();
}
