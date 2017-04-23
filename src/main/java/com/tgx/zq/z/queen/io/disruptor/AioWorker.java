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
package com.tgx.zq.z.queen.io.disruptor;

import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.lmax.disruptor.RingBuffer;
import com.tgx.zq.z.queen.base.disruptor.QEvent;
import com.tgx.zq.z.queen.base.disruptor.inf.IError;
import com.tgx.zq.z.queen.base.disruptor.inf.IEventOp;
import com.tgx.zq.z.queen.base.inf.IDisposable;
import com.tgx.zq.z.queen.base.util.Pair;
import com.tgx.zq.z.queen.io.impl.AioPackage;
import com.tgx.zq.z.queen.io.inf.IConnectActive;
import com.tgx.zq.z.queen.io.inf.IConnectMode;
import com.tgx.zq.z.queen.io.inf.IConnected;
import com.tgx.zq.z.queen.io.inf.IPoS;
import com.tgx.zq.z.queen.io.inf.ISession;

class AioWorker
        extends
        Thread
        implements
        IDisposable
{

    final Logger                        logger = Logger.getLogger(getClass().getSimpleName());
    private final ExecutorCore<?, ?, ?> mCore;
    private final WorkType              mType;
    RingBuffer<QEvent>                  bindProducer;

    public AioWorker(Runnable r, final ExecutorCore<?, ?, ?> core, String name, WorkType type) {
        super(r, name);
        this.mCore = core;
        this.mType = type;
        switch (type) {
            case SERVICE:
                this.bindProducer = core.getProducerBuffer();
                break;
            case CLUSTER:
                this.bindProducer = core.getClusterBuffer();
                break;
        }
    }

    @Override
    public void run() {
        try {
            super.run();
        }
        catch (Error e) {
            logger.log(Level.FINER, "AioWorker Work UncatchEx", e);
            switch (mType) {
                case SERVICE:
                    mCore.reWorkAvailable(bindProducer);
                    break;
                case CLUSTER:
                    mCore.reClusterAvailable(bindProducer);
                    break;
            }
            dispose();
        }
    }

    @Override
    public void dispose() {
        bindProducer = null;
    }

    public <T, A> void publish(final IEventOp<T, A> op, final IError.Type eType, final IEventOp.Type type, final T t, final A a) {
        long sequence = bindProducer.next();
        try {
            QEvent event = bindProducer.get(sequence);
            if (eType.equals(IError.Type.NO_ERROR)) {
                event.produce(type, t, a, op);
            }
            else {
                event.error(eType, t, a, op);
            }

        }
        finally {
            bindProducer.publish(sequence);
        }
    }

    public void publishRead(final IEventOp<IPoS, ISession> op, final ByteBuffer buff, final ISession session) {
        publish(op, IError.Type.NO_ERROR, IEventOp.Type.READ, new AioPackage(buff), session);
    }

    public void publishWrote(final IEventOp<Integer, ISession> op, final int wroteCnt, final ISession session) {
        publish(op, IError.Type.NO_ERROR, IEventOp.Type.WROTE, wroteCnt, session);
    }

    public <T> void publishWroteError(final IEventOp<T, ISession> op, final IError.Type eType, final T t, final ISession session) {
        publish(op, eType, IEventOp.Type.NULL, t, session);
    }

    public <T extends IConnected> void publishConnected(final IEventOp<Pair<T, IConnectMode.OPERATION_MODE>, AsynchronousSocketChannel> op,
                                                        final T callback,
                                                        final IConnectMode.OPERATION_MODE mode,
                                                        final AsynchronousSocketChannel channel) {
        publish(op, IError.Type.NO_ERROR, IEventOp.Type.CONNECTED, new Pair<>(callback, mode), channel);
    }

    public <T> void publishConnectingError(final IEventOp<Pair<T, Throwable>, IConnectActive> op,
                                           final Throwable e,
                                           final T t,
                                           final IConnectActive cActive) {
        publish(op, IError.Type.CONNECT_FAILED, IEventOp.Type.NULL, new Pair<>(t, e), cActive);
    }

    public <T> void publishReadError(final IEventOp<T, ISession> op, final IError.Type eType, final T t, final ISession session) {
        publish(op, eType, IEventOp.Type.NULL, t, session);
    }

    public enum WorkType
    {
        SERVICE,
        CLUSTER
    }

}
