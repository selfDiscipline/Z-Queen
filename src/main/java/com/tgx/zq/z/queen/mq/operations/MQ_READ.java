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
package com.tgx.zq.z.queen.mq.operations;

import java.util.logging.Logger;

import com.tgx.zq.z.queen.base.disruptor.inf.IEventOp;
import com.tgx.zq.z.queen.base.util.Triple;
import com.tgx.zq.z.queen.io.disruptor.operations.ws.DEFAULT_TRANSFER_DISPATCH;
import com.tgx.zq.z.queen.io.inf.ICommand;
import com.tgx.zq.z.queen.io.inf.IPipeRead;
import com.tgx.zq.z.queen.io.inf.IPoS;
import com.tgx.zq.z.queen.io.inf.ISession;
import com.tgx.zq.z.queen.io.ws.filter.WsControlFilter;
import com.tgx.zq.z.queen.io.ws.filter.WsFrameFilter;
import com.tgx.zq.z.queen.io.ws.filter.ZCommandFilter;
import com.tgx.zq.z.queen.io.ws.protocol.WsContext;
import com.tgx.zq.z.queen.io.ws.protocol.bean.mq.X80_MqTopicReg;

public enum MQ_READ
        implements
        IEventOp<IPoS, ISession>,
        IPipeRead
{
    INSTANCE;
    Logger                log = Logger.getLogger(getClass().getSimpleName());
    private WsFrameFilter wsFilter;

    MQ_READ() {
        wsFilter = new WsFrameFilter();
        wsFilter.linkFront(new ZCommandFilter(command -> {
            switch (command) {
                case X80_MqTopicReg.COMMAND:
                    return new X80_MqTopicReg();
                default:
                    log.warning("command is not cluster consistentHandle: X" + Integer.toHexString(command).toUpperCase());
                    break;
            }
            return null;
        })).linkFront(new WsControlFilter());
    }

    @SuppressWarnings("unchecked")
    @Override
    public Triple<ICommand[], ISession, IEventOp<ICommand[], ISession>> handle(IPoS _InPoS, ISession session) {
        return new Triple<>(filterRead(_InPoS, wsFilter, (WsContext) session.getContext()),
                            session,
                            DEFAULT_TRANSFER_DISPATCH.PLAIN_SYMMETRY);
    }

}
