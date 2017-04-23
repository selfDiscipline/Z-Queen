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
import com.tgx.zq.z.queen.io.crypt.EncryptHandler;
import com.tgx.zq.z.queen.io.disruptor.operations.LOG_OPERATOR;
import com.tgx.zq.z.queen.io.inf.ICommand;
import com.tgx.zq.z.queen.io.inf.IContext;
import com.tgx.zq.z.queen.io.inf.IContext.DecodeState;
import com.tgx.zq.z.queen.io.inf.IPoS;
import com.tgx.zq.z.queen.io.inf.ISession;

/**
 * @author William.d.zk
 */
public class DecodeHandler
        implements
        EventHandler<QEvent>
{
    private final EncryptHandler _EncryptHandler = new EncryptHandler();
    Logger                       log             = Logger.getLogger(getClass().getName());

    @Override
    public void onEvent(QEvent event, long sequence, boolean batch) throws Exception {
        if (event.noError()) {
            if (!event.getEventType().equals(IEventOp.Type.TRANSFER)) {
                log.warning("Event type error:" + event.getEventType());
                return;
            }
            IEventOp<IPoS, ISession> pOperator = event.getEventOp();
            Pair<IPoS, ISession> pContent = event.getContent();
            ISession session = pContent.second();
            IContext context = session.getContext();
            context.setEncryptHandler(_EncryptHandler);
            IPoS _InPoS = pContent.first();
            if (!context.getDecodeState().equals(DecodeState.DECODE_ERROR)) try {
                Triple<ICommand[], ISession, IEventOp<ICommand[], ISession>> result = pOperator.handle(_InPoS, session);
                event.produce(IEventOp.Type.DISPATCH, result.first(), session, result.third());
            }
            catch (Exception e) {
                log.log(Level.FINE, "read decode error" + session.toString(), e);
                event.error(IError.Type.FILTER_DECODE, e, session, LOG_OPERATOR.WARNING);
                context.setDecodeState(DecodeState.DECODE_ERROR);
            }
        }

    }

}
