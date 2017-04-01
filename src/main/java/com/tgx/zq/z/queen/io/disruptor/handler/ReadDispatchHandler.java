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

import java.util.logging.Logger;

import com.lmax.disruptor.RingBuffer;
import com.tgx.zq.z.queen.base.disruptor.QEvent;
import com.tgx.zq.z.queen.base.disruptor.inf.IError;
import com.tgx.zq.z.queen.base.disruptor.inf.IEventOp;
import com.tgx.zq.z.queen.base.disruptor.inf.IEventOp.Type;
import com.tgx.zq.z.queen.base.disruptor.inf.IPipeEventHandler;
import com.tgx.zq.z.queen.base.util.Pair;
import com.tgx.zq.z.queen.base.util.Triple;
import com.tgx.zq.z.queen.io.disruptor.operations.CLOSE_ERROR_OPERATOR;
import com.tgx.zq.z.queen.io.inf.ICommand;
import com.tgx.zq.z.queen.io.inf.IDestine;
import com.tgx.zq.z.queen.io.inf.IDestine.RESULT;
import com.tgx.zq.z.queen.io.inf.ISession;

public class ReadDispatchHandler
        implements
        IPipeEventHandler<QEvent, QEvent>
{

    private final RingBuffer<QEvent>   mLinkRB;
    private final RingBuffer<QEvent>   mClusterRB;
    private final RingBuffer<QEvent>[] mLogicRB;
    private final int                  mLogicIndexMask;
    private final IDestine             mClusterDestine;
    private final IDestine             mLinkDestine;
    Logger                             log = Logger.getLogger(getClass().getName());

    @SafeVarargs
    public ReadDispatchHandler(IDestine clusterDestine,
                               IDestine linkDestine,
                               RingBuffer<QEvent> clusterBuffer,
                               RingBuffer<QEvent> linkBuffer,
                               RingBuffer<QEvent>... logicBuffers) {

        mClusterDestine = clusterDestine;
        mLinkDestine = linkDestine;
        mClusterRB = clusterBuffer;
        mLinkRB = linkBuffer;
        mLogicRB = logicBuffers;

        if (Integer.bitCount(mLogicRB.length) != 1) { throw new IllegalArgumentException("bufferSize must be a power of 2"); }
        mLogicIndexMask = mLogicRB.length - 1;
    }

    @Override
    public void onEvent(QEvent event, long sequence, boolean endOfBatch) throws Exception {
        if (event.noError()) {
            switch (event.getEventType()) {
                case DISPATCH:
                    Pair<ICommand[], ISession> pContent = event.getContent();
                    IEventOp<ICommand[], ISession> pOperator = event.getEventOp();
                    Triple<ICommand, ISession, IEventOp<ICommand, ISession>>[] pResult = pOperator.handleResultArray(pContent.first(),
                                                                                                                     pContent.second());
                    if (pResult != null) for (Triple<ICommand, ISession, IEventOp<ICommand, ISession>> triple : pResult) {
                        ICommand cmd = triple.first();
                        ISession session = triple.second();
                        RESULT cResult = mClusterDestine.trial(cmd, session.getMode());
                        RESULT lResult = mLinkDestine.trial(cmd, session.getMode());
                        if (cResult.equals(RESULT.HANDLE)) {
                            publish(mClusterRB, Type.LOGIC, cmd, session, triple.third());
                        }
                        else if (lResult.equals(RESULT.HANDLE)) {
                            publish(mLinkRB, Type.LOGIC, cmd, session, triple.third());
                        }
                        else if (cResult.equals(RESULT.IGNORE) && lResult.equals(RESULT.IGNORE)) {
                            log.info("wait to handle: " + cmd);
                            tryPublish(mLogicRB[(int) (sequence & mLogicIndexMask)], Type.LOGIC, cmd, session, triple.third());
                        }
                        else {
                            log.warning("cmd: " + cmd + " no handler do with it ! dropped");
                        }
                    }
                    break;
                case TRANSFER:
                    break;// ignore-drop event
                default:
                    log.warning("invalid event type: " + event.getEventType().name());
                    break;
            }

        }
        else {
            IError.Type errorType = event.getErrorType();
            Pair<Throwable, ISession> pContent = event.getContent();
            IEventOp<Throwable, ISession> pOperator = event.getEventOp();
            ISession session = pContent.second();
            Throwable throwable = pContent.first();
            pOperator.handle(throwable, session);
            switch (errorType) {
                case FILTER_DECODE:
                    switch (session.getMode()) {
                        case CONNECT_CLUSTER:
                        case ACCEPT_CLUSTER:
                        case SYMMETRY:
                            error(mClusterRB, IError.Type.CLOSED, throwable, session, CLOSE_ERROR_OPERATOR.INSTANCE);
                            break;
                        case CONNECT_MQ:
                        case ACCEPT_MQ:
                        case CONNECT_CONSUMER:
                        case ACCEPT_SERVER:
                        case CONNECT_CONSUMER_SSL:
                        case ACCEPT_SERVER_SSL:
                            error(mLinkRB, IError.Type.CLOSED, throwable, session, CLOSE_ERROR_OPERATOR.INSTANCE);
                            break;
                    }
                default:
                    log.warning("Read Dispatch error type can't be handled: " + errorType);
                    break;
            }
        }
        event.reset();
    }

}
