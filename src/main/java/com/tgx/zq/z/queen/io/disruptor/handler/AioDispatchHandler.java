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
package com.tgx.zq.z.queen.io.disruptor.handler;

import static com.tgx.zq.z.queen.base.disruptor.inf.IEventOp.Type.CLOSE;
import static com.tgx.zq.z.queen.base.disruptor.inf.IEventOp.Type.CONNECTED;
import static com.tgx.zq.z.queen.base.disruptor.inf.IEventOp.Type.LOCAL;
import static com.tgx.zq.z.queen.base.disruptor.inf.IEventOp.Type.TRANSFER;
import static com.tgx.zq.z.queen.base.disruptor.inf.IEventOp.Type.WROTE;

import java.nio.channels.AsynchronousSocketChannel;

import com.lmax.disruptor.RingBuffer;
import com.tgx.zq.z.queen.base.disruptor.QEvent;
import com.tgx.zq.z.queen.base.disruptor.inf.IError;
import com.tgx.zq.z.queen.base.disruptor.inf.IEventOp;
import com.tgx.zq.z.queen.base.disruptor.inf.IEventOp.Type;
import com.tgx.zq.z.queen.base.disruptor.inf.IPipeEventHandler;
import com.tgx.zq.z.queen.base.util.Pair;
import com.tgx.zq.z.queen.io.disruptor.operations.CLOSE_ERROR_OPERATOR;
import com.tgx.zq.z.queen.io.inf.ICommand;
import com.tgx.zq.z.queen.io.inf.IConnectActive;
import com.tgx.zq.z.queen.io.inf.IConnectError;
import com.tgx.zq.z.queen.io.inf.IConnectMode;
import com.tgx.zq.z.queen.io.inf.ICreatorFactory;
import com.tgx.zq.z.queen.io.inf.IPoS;
import com.tgx.zq.z.queen.io.inf.ISession;
import com.tgx.zq.z.queen.io.inf.ISessionManager;

public class AioDispatchHandler
        implements
        IPipeEventHandler<QEvent, QEvent>
{
    private final RingBuffer<QEvent>[] mIoReadRB;
    private final RingBuffer<QEvent>   mIoWroteRB;
    private final RingBuffer<QEvent>   mLinkRB;
    private final RingBuffer<QEvent>   mClusterRB;
    private final int                  mReadIndexMask;

    @SafeVarargs
    public AioDispatchHandler(RingBuffer<QEvent> clusterBuffer,
                              RingBuffer<QEvent> linkBuffer,
                              RingBuffer<QEvent> wroteBuffer,
                              RingBuffer<QEvent>... buffers) {
        mClusterRB = clusterBuffer;
        mLinkRB = linkBuffer;
        mIoReadRB = buffers;
        mIoWroteRB = wroteBuffer;
        if (Integer.bitCount(mIoReadRB.length) != 1) throw new IllegalArgumentException("bufferSize must be a power of 2");
        mReadIndexMask = mIoReadRB.length - 1;
    }

    private <V, A> void dispatch(IConnectMode.OPERATION_MODE mode, Type type, V v, A a, IEventOp<V, A> op) {
        switch (mode) {
            case CONNECT_CLUSTER:
            case ACCEPT_CLUSTER:
                publish(mClusterRB, type, v, a, op);// NODE_CONNECTED
                break;
            case SYMMETRY:
            case CONNECT_MQ:
            case ACCEPT_MQ:
            case ACCEPT_SERVER:
            case ACCEPT_SERVER_SSL:
                publish(mLinkRB, type, v, a, op);
                break;
            default:
                // ignore
                break;
        }
    }

    private <V, A> void dispatchError(IConnectMode.OPERATION_MODE mode, IError.Type type, V v, A a, IEventOp<V, A> op) {
        switch (mode) {
            case CONNECT_CLUSTER:
            case ACCEPT_CLUSTER:
                error(mClusterRB, type, v, a, op);
                break;
            case SYMMETRY:
            case CONNECT_MQ:
            case ACCEPT_MQ:
            case ACCEPT_SERVER:
            case ACCEPT_SERVER_SSL:
                error(mLinkRB, type, v, a, op);
                break;
            default:
                // ignore
                break;
        }
    }

    @Override
    public void onEvent(QEvent event, long sequence, boolean batch) throws Exception {
        if (event.noError()) {
            switch (event.getEventType()) {
                case CONNECTED:
                    IEventOp<Pair<ICreatorFactory, IConnectMode.OPERATION_MODE>, AsynchronousSocketChannel> cOperator = event.getEventOp();
                    Pair<Pair<ICreatorFactory, IConnectMode.OPERATION_MODE>, AsynchronousSocketChannel> cContent = event.getContent();
                    IConnectMode.OPERATION_MODE mode = cContent.first().second();
                    dispatch(mode, CONNECTED, cContent.first(), cContent.second(), cOperator);
                    break;
                case READ:
                    Pair<IPoS, ISession> rContent = event.getContent();
                    ISession session = rContent.second();
                    int rIndex = session.getHashKey() & mReadIndexMask;
                    publish(mIoReadRB[rIndex], TRANSFER, rContent.first(), rContent.second(), event.getEventOp());
                    break;
                case WROTE:
                    Pair<Integer, ISession> wContent = event.getContent();
                    publish(mIoWroteRB, WROTE, wContent.first(), wContent.second(), event.getEventOp());
                    break;
                case CLOSE:// CLOSE_OPERATOR
                    IEventOp<Throwable, ISession> ccOperator = event.getEventOp();
                    Pair<Throwable, ISession> ccContent = event.getContent();
                    Throwable t = ccContent.first();
                    session = ccContent.second();
                    if (session == null) {
                        log.severe("close dispatch error,session is null");
                        break;
                    }
                    mode = session.getMode();
                    dispatch(mode, CLOSE, t, session, ccOperator);
                    break;
                case LOCAL:
                    Pair<ICommand, ISessionManager> lContent = event.getContent();
                    ICommand cmd = lContent.first();
                    ISessionManager sm = lContent.second();
                    if (sm.checkMode(IConnectMode.OPERATION_MODE.SYMMETRY,
                                     IConnectMode.OPERATION_MODE.CONNECT_MQ,
                                     IConnectMode.OPERATION_MODE.ACCEPT_MQ,
                                     IConnectMode.OPERATION_MODE.CONNECT_CONSUMER,
                                     IConnectMode.OPERATION_MODE.CONNECT_CONSUMER_SSL,
                                     IConnectMode.OPERATION_MODE.ACCEPT_SERVER,
                                     IConnectMode.OPERATION_MODE.ACCEPT_SERVER_SSL)) {
                        publish(mLinkRB, LOCAL, cmd, sm, event.getEventOp());
                    }
                    if (sm.checkMode(IConnectMode.OPERATION_MODE.CONNECT_CLUSTER, IConnectMode.OPERATION_MODE.ACCEPT_CLUSTER)) {
                        publish(mClusterRB, LOCAL, cmd, sm, event.getEventOp());
                    }
                    break;
                default:
                    break;
            }
        }
        else {
            IError.Type errorType = event.getErrorType();
            switch (errorType) {
                case CONNECT_FAILED:
                    IEventOp<Pair<IConnectError, Throwable>, IConnectActive> cOperator = event.getEventOp();
                    Pair<Pair<IConnectError, Throwable>, IConnectActive> cContent = event.getContent();
                    dispatchError(cContent.second().getMode(), errorType, cContent.first(), cContent.second(), cOperator);
                    break;
                case CLOSED:// CLOSE_ERROR_OPERATOR
                case READ_ZERO:// READ_ERROR_OPERATOR.INSTANCE
                case READ_EOF:// READ_ERROR_OPERATOR.INSTANCE
                case READ_FAILED:// READ_ERROR_OPERATOR.INSTANCE
                case WRITE_EOF:// WROTE_ERROR_OPERATOR.INSTANCE
                case WRITE_FAILED:// WROTE_ERROR_OPERATOR.INSTANCE
                case WRITE_ZERO:// WROTE_ERROR_OPERATOR.INSTANCE
                case TIME_OUT://
                    IEventOp<Throwable, ISession> sOperator = event.getEventOp();
                    Pair<Throwable, ISession> sContent = event.getContent();
                    ISession session = sContent.second();
                    Throwable throwable = sContent.first();
                    sOperator.handle(throwable, session);
                    dispatchError(session.getMode(), IError.Type.CLOSED, throwable, session, CLOSE_ERROR_OPERATOR.INSTANCE);
                    break;
                default:
                    break;
            }
        }
        event.reset();
    }
}
