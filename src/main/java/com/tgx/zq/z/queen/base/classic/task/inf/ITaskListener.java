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
package com.tgx.zq.z.queen.base.classic.task.inf;

import com.tgx.zq.z.queen.base.classic.task.TaskService;

public interface ITaskListener
{
    /**
     * @param taskOrResult
     * @param service
     * @return true:将停止继续分发
     */
    default boolean handleResult(ITaskResult taskOrResult, TaskService service) {
        return true;
    }

    /**
     * task 将携带错误信息，task内含的资源需要在此处进行手工释放
     * 
     * @param task
     * @param service
     * @return true:将停止继续分发
     */
    default boolean exCaught(ITaskResult task, TaskService service) {
        return true;
    }

    /**
     * 当前Listener是否处于可用状态,此状态的由具体实现决定
     * 
     * @return
     */
    default boolean isEnable() {
        return true;
    }

    int getBindSerial();

    /**
     * 设置当前Listener的串号,为进行分发确定唯一标识
     *
     * @param bindSerial
     */
    default void setBindSerial(int bindSerial) {
    }
}