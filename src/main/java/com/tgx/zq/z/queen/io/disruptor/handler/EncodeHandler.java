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
package com.tgx.zq.z.queen.io.disruptor.handler;

import java.util.logging.Level;
import java.util.logging.Logger;

import com.lmax.disruptor.EventHandler;
import com.tgx.zq.z.queen.base.disruptor.QEvent;
import com.tgx.zq.z.queen.base.disruptor.inf.IError;
import com.tgx.zq.z.queen.base.disruptor.inf.IEventOp;
import com.tgx.zq.z.queen.base.util.Pair;
import com.tgx.zq.z.queen.base.util.Triple;
import com.tgx.zq.z.queen.io.disruptor.operations.CLOSE_ERROR_OPERATOR;
import com.tgx.zq.z.queen.io.disruptor.operations.IGNORE_IPOS_OPERATOR;
import com.tgx.zq.z.queen.io.disruptor.operations.LOG_OPERATOR;
import com.tgx.zq.z.queen.io.inf.ICommand;
import com.tgx.zq.z.queen.io.inf.IContext;
import com.tgx.zq.z.queen.io.inf.IContext.EncodeState;
import com.tgx.zq.z.queen.io.inf.ISession;

/**
 * @author William.d.zk
 */
public class EncodeHandler
        implements
        EventHandler<QEvent>
{
    Logger log = Logger.getLogger(getClass().getSimpleName());

    @Override
    public void onEvent(QEvent event, long sequence, boolean batch) throws Exception {
        if (event.noError()) {
            switch (event.getEventType()) {
                case WRITE:
                    IEventOp<ICommand, ISession> pOperator = event.getEventOp();
                    Pair<ICommand, ISession> pContent = event.getContent();
                    ISession session = pContent.second();
                    IContext context = session.getContext();
                    ICommand cmd = pContent.first();
                    if (!context.getEncodeState().equals(EncodeState.ENCODE_ERROR)) try {
                        Triple<Throwable, ISession, IEventOp<Throwable, ISession>> result = pOperator.handle(cmd, session);
                        if (result == null) event.produce(IEventOp.Type.IGNORE, null, session, IGNORE_IPOS_OPERATOR.INSTANCE);
                        else {
                            log.log(Level.FINE, "write encode inner error" + session.toString(), result.first());
                            event.error(IError.Type.FILTER_ENCODE, result.first(), session, CLOSE_ERROR_OPERATOR.INSTANCE);
                            context.setEncodeState(EncodeState.ENCODE_ERROR);
                        }
                    }
                    catch (Exception e) {
                        log.log(Level.FINE, "write encode error" + session.toString(), e);
                        event.error(IError.Type.FILTER_DECODE, e, session, LOG_OPERATOR.WARNING);
                        context.setEncodeState(EncodeState.ENCODE_ERROR);
                    }
                    cmd.dispose();
                    break;
                case WROTE:
                    IEventOp<Integer, ISession> fOperator = event.getEventOp();// WROTE_OPERATOR
                    Pair<Integer, ISession> fContent = event.getContent();
                    Integer wrote = fContent.first();
                    session = fContent.second();
                    context = session.getContext();
                    if (!context.getEncodeState().equals(EncodeState.ENCODE_ERROR)) try {
                        Triple<Throwable, ISession, IEventOp<Throwable, ISession>> result = fOperator.handle(wrote, session);
                        if (result == null) event.produce(IEventOp.Type.IGNORE, null, session, IGNORE_IPOS_OPERATOR.INSTANCE);
                        else {
                            log.log(Level.FINE, "wrote encode inner error" + session.toString(), result.first());
                            event.error(IError.Type.WRITE_FAILED, result.first(), session, CLOSE_ERROR_OPERATOR.INSTANCE);
                            context.setEncodeState(EncodeState.ENCODE_ERROR);
                        }
                    }
                    catch (Exception e) {
                        log.log(Level.FINE, "wrote encode error" + session.toString(), e);
                        event.error(IError.Type.WRITE_FAILED, e, session, CLOSE_ERROR_OPERATOR.INSTANCE);
                        context.setEncodeState(EncodeState.ENCODE_ERROR);
                    }
                    break;
                default:
                    break;
            }
        }
        else {
            log.finest("write encode handler error type: " + event.getErrorType().name());
            /*
             * Ignore 此处只应该有 IError.Type.CLOSED 直接交给 Dispatch 处理器向 Link 或者 Cluster 管理器投递 Close 事件
             */
        }
    }
}
