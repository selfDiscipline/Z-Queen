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

package com.tgx.zq.z.queen.base.disruptor;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;

import com.lmax.disruptor.AlertException;
import com.lmax.disruptor.DataProvider;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.EventProcessor;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.SequenceBarrier;
import com.lmax.disruptor.TimeoutException;

public class MultiBufferBatchEventProcessor<T>
        implements
        EventProcessor
{
    private final AtomicBoolean     isRunning = new AtomicBoolean(false);
    private final DataProvider<T>[] providers;
    private final SequenceBarrier[] barriers;
    private final EventHandler<T>   handler;
    private final Sequence[]        sequences;
    private long                    count;
    private String                  threadName;

    public MultiBufferBatchEventProcessor(DataProvider<T>[] providers, SequenceBarrier[] barriers, EventHandler<T> handler) {
        if (providers.length != barriers.length) { throw new IllegalArgumentException(); }

        this.providers = providers;
        this.barriers = barriers;
        this.handler = handler;

        this.sequences = new Sequence[providers.length];
        for (int i = 0; i < sequences.length; i++) {
            sequences[i] = new Sequence(-1);
        }
    }

    public void setThreadName(String name) {
        threadName = name;
    }

    @Override
    public void run() {
        if (!isRunning.compareAndSet(false, true)) { throw new RuntimeException("Already running"); }
        if (threadName != null) Thread.currentThread().setName(threadName);
        for (SequenceBarrier barrier : barriers) {
            barrier.clearAlert();
        }

        final int barrierLength = barriers.length;
        int tcount;
        long delta;
        while (true) {
            tcount = 0;
            try {
                for (int i = 0; i < barrierLength; i++) {

                    long available = barriers[i].waitFor(-1);
                    Sequence sequence = sequences[i];

                    long nextSequence = sequence.get() + 1;
                    for (long l = nextSequence; l <= available; l++) {
                        handler.onEvent(providers[i].get(l), l, nextSequence == available);
                    }

                    sequence.set(available);
                    delta = available - nextSequence + 1;
                    count += delta;
                    tcount += delta;
                }
                if (tcount == 0) LockSupport.parkNanos(50000L);
                else if (tcount < 100) Thread.yield();
            }
            catch (AlertException e) {
                if (!isRunning()) {
                    break;
                }
            }
            catch (InterruptedException e) {
                e.printStackTrace();
            }
            catch (TimeoutException e) {
                e.printStackTrace();
            }
            catch (Exception e) {
                e.printStackTrace();
                break;
            }
        }
    }

    @Override
    public Sequence getSequence() {
        throw new UnsupportedOperationException();
    }

    public long getCount() {
        return count;
    }

    public Sequence[] getSequences() {
        return sequences;
    }

    @Override
    public void halt() {
        isRunning.set(false);
        barriers[0].alert();
    }

    @Override
    public boolean isRunning() {
        return isRunning.get();
    }
}
