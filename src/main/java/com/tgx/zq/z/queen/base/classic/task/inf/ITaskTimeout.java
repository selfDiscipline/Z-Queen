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

import com.tgx.zq.z.queen.base.classic.task.Task;

/**
 * 某一类型的Task存在超时过程时所需要执行的行为规范
 * 
 *
 * @see #doTimeout(Task)
 * @see #onInvalid(Task)
 * @see #cancel()
 * @see #isTimeout(long, Task)
 * @see #isEnabled()
 */
public interface ITaskTimeout<T extends Task>
{
    /**
     * 发生timeout后的行为
     * 
     * @param task
     *            当前发生timeout的任务
     *
     */
    void doTimeout(T task);

    /**
     * 规范了当前超时过程已失效时的操作,包括超时计算已无使用必要时，以及任务已经被关闭时
     * 
     * @param task
     *            失效过程所影响的任务
     *
     */
    void onInvalid(T task);

    /**
     * 注销当前的超时回调
     */
    void cancel();

    /**
     * 当前时间是否已符合超时条件
     * 
     * @param curTime
     *            当前时间,由时间暂存提供以提高性能,并不需要绝对校准当前时间.
     * @param task
     *            包含了所关注的任务.内部提供了{@code startTime}和{@code timeOut}两项用于判定的时间戳
     * @return true 已出发超时<br>
     *         false 尚未触发超时<br>
     *         回调处于disable时恒返回false
     * @see Task#startTime
     * @see Task#timeOut
     * @see Task#setStartTime(long)
     * @see Task#timeOut(int, ITaskTimeout)
     */
    boolean isTimeout(long curTime, T task);

    /**
     * 返回当前时间距离超时触发 尚需等待的时间
     * 
     * @param curTime
     *            当前时间,由时间暂存提供以提高性能,并不需要绝对校准当前时间.
     * @param task
     *            包含了所关注的任务.内部提供了{@code startTime}和{@code timeOut}两项用于判定的时间戳
     * @return 还需等待多久将会触发超时
     * @see Task#startTime
     * @see Task#timeOut
     * @see Task#setStartTime(long)
     * @see Task#timeOut(int, ITaskTimeout)
     */
    long toWait(long curTime, T task);

    /**
     *
     * @return 回调是否可用
     * @see ITaskTimeout#cancel()
     */
    boolean isEnabled();

}
