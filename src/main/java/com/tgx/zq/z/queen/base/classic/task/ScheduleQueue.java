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
import java.util.NoSuchElementException;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class ScheduleQueue<E extends Task>
{
    final static long             AbsoluteTimeAwait    = 0;
    final static long             AbsoluteTimeNowait   = -1;
    transient final ReentrantLock lock                 = new ReentrantLock();
    transient final Condition     available            = lock.newCondition();
    final TreeSet<E>              tasksTree;
    final TaskService             mService;
    final AtomicInteger           priorityIncrease     = new AtomicInteger(0);
    final AtomicLong              toWakeUpAbsoluteTime = new AtomicLong(-1);
    private int                   offerIndex;

    public ScheduleQueue(Comparator<? super E> comparator, TaskService service) {
        tasksTree = new TreeSet<E>(comparator);
        this.mService = service;
    }

    /**
     *
     * @param e
     *            {@code ?super Task}
     * @return <tt>True 成功插入 </tt> 由于此Queue使用Set特性,所以必须是!contain(<tt>param</tt>) 否则<tt>False</tt>
     */
    public final boolean offer(E e) {
        if (e == null) return false;
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            E first = peek();
            e.inQueueIndex = ++offerIndex;
            if (!tasksTree.add(e)) return false;
            e.intoScheduleQueue();
            if (first == null || tasksTree.comparator().compare(e, first) < 0) available.signalAll();
            return true;
        }
        finally {
            lock.unlock();
        }
    }

    private final boolean treeOffer(E e) {
        if (e == null) return false;
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            E first = peek();
            e.inQueueIndex = ++offerIndex;
            if (!tasksTree.add(e)) return false;
            if (first == null || tasksTree.comparator().compare(e, first) < 0) available.signalAll();
            return true;
        }
        finally {
            lock.unlock();
        }
    }

    private E peek() {
        try {
            return tasksTree.first();
        }
        catch (NoSuchElementException e) {
            return null;
        }
    }

    private final E pollFirst() {
        E first = peek();
        if (first == null) return null;
        else if (tasksTree.remove(first)) {
            first.outScheduleQueue();
            return first;// remove操作应该会导致树的自平衡操作
        }
        return null;
    }

    private final E treePoll() {
        E first = peek();
        if (first == null) return null;
        else if (tasksTree.remove(first)) return first;
        return null;
    }

    /**
     * 移除并返回队列的头部元素,队列为空或Task.isToSchedule < 0 时 忽略此操作
     * 
     * @return queue 的头部 , or <tt>null</tt> 如果队列为空的话
     */
    public E poll() {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            E first = peek();
            if (first == null || !first.isToSchedule()) return null;
            else {
                E x = pollFirst();
                assert x != null;
                if (!isEmpty()) available.signalAll();
                return x;
            }
        }
        finally {
            lock.unlock();
        }
    }

    public final boolean replace(E e) {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            if (tasksTree.contains(e) && tasksTree.remove(e)) {
                e.outScheduleQueue();
                return offer(e);
            }
            return false;
        }
        finally {
            lock.unlock();
        }
    }

    final boolean isEmpty() {
        final ReentrantLock lock = this.lock;
        lock.lock();
        try {
            return tasksTree.isEmpty();
        }
        finally {
            lock.unlock();
        }
    }

    final int size() {
        return tasksTree.size();
    }

    public final void clear() {
        tasksTree.clear();
    }

    public final E take() throws InterruptedException {
        final ReentrantLock lock = this.lock;
        lock.lockInterruptibly();
        try {
            while (true) {
                E first = peek();
                if (first == null) {
                    priorityIncrease.set(1);
                    offerIndex = 0;
                    mService.noScheduleAlarmTime();
                    toWakeUpAbsoluteTime.set(AbsoluteTimeAwait);
                    available.await();
                }
                else {
                    if (first.isCancelled()) {
                        pollFirst();
                        System.out.println("remove task  when take()  " + first.hashCode());
                        continue;
                    }
                    long delay = first.getDelay(TimeUnit.NANOSECONDS);
                    boolean imReturn = first.isDone;
                    if (first.isToSchedule(delay) || imReturn) {
                        toWakeUpAbsoluteTime.set(AbsoluteTimeNowait);
                        if (first.isToSchedule(delay)) {
                            ReentrantLock runLock = first.getLock();
                            runLock.lock();
                            try {
                                if (first.offTime > 0) {
                                    // update delay time
                                    E x = treePoll();
                                    assert x != null;
                                    if (x == null || x != first) throw new NullPointerException("Check TreeSet.remove -> Compare(t1,t2)!");
                                    first.doTime += first.offTime;
                                    first.offTime = 0;
                                    runLock.unlock();
                                    treeOffer(first);
                                    continue;
                                }
                            }
                            finally {
                                runLock.unlock();
                            }
                        }
                        E x = pollFirst();
                        assert x != null;
                        if (x == null || x != first) throw new NullPointerException("Check TreeSet.remove -> Compare(t1,t2)!");
                        if (!isEmpty()) available.signalAll();// 当此Queue作为单例并未由多个消费者进行并发操作时
                                                              // 本块代码无价值~
                        return x;
                    }
                    else {
                        priorityIncrease.set(1);
                        toWakeUpAbsoluteTime.set(first.doTime);
                        mService.setScheduleAlarmTime(first.doTime);
                        available.awaitNanos(delay);
                    }
                }
            }
        }
        finally {
            lock.unlock();
        }
    }

    public boolean hasThis(E e) {
        return tasksTree.contains(e);
    }
}
