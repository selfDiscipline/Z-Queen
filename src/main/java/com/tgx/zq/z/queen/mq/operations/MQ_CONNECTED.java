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

import java.io.IOException;
import java.nio.channels.AsynchronousSocketChannel;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.tgx.zq.z.queen.base.disruptor.inf.IEventOp;
import com.tgx.zq.z.queen.base.util.IoUtil;
import com.tgx.zq.z.queen.base.util.Pair;
import com.tgx.zq.z.queen.base.util.Triple;
import com.tgx.zq.z.queen.biz.template.BizNode;
import com.tgx.zq.z.queen.io.bean.XF000_NULL;
import com.tgx.zq.z.queen.io.disruptor.AioReader;
import com.tgx.zq.z.queen.io.disruptor.operations.IGNORE_ICOMMAND_OPERATOR;
import com.tgx.zq.z.queen.io.disruptor.operations.ws.WRITE_OPERATOR;
import com.tgx.zq.z.queen.io.inf.ICommand;
import com.tgx.zq.z.queen.io.inf.IConnectMode.OPERATION_MODE;
import com.tgx.zq.z.queen.io.inf.IConnected;
import com.tgx.zq.z.queen.io.inf.ISession;
import com.tgx.zq.z.queen.io.inf.ISessionCreator;
import com.tgx.zq.z.queen.io.ws.protocol.bean.control.X104_ExchangeIdentity;
import com.tgx.zq.z.queen.mq.server.MqServer;

public enum MQ_CONNECTED
        implements
        IEventOp<Pair<IConnected, OPERATION_MODE>, AsynchronousSocketChannel>
{
    INSTANCE;

    Logger log = Logger.getLogger(getClass().getName());

    @SuppressWarnings({ "rawtypes",
                        "unchecked" })
    @Override
    public Triple<ICommand, ISession, IEventOp<ICommand, ISession>> handle(Pair v, AsynchronousSocketChannel channel) {
        Pair<IConnected, OPERATION_MODE> p = v;
        OPERATION_MODE mode = p.second();
        IConnected connected = p.first();
        ISession session;
        BizNode bizNode = null;
        try {
            ISessionCreator creator = connected.getCreator(channel.getLocalAddress(), mode);
            session = creator.createSession(channel, connected.getConnectActive());
            switch (mode) {
                case ACCEPT_MQ:
                    bizNode = ((MqServer) connected).getBizNode();
                    bizNode.addSession(session);
                    bizNode.onCreate(session);
                    session.readNext(AioReader.INSTANCE);
                    byte[] x104_payload = new byte[8];
                    long addrIpv4 = 0;
                    addrIpv4 = IoUtil.writeInet4Addr(session.getLocalAddress().getAddress().getAddress(),
                                                     session.getLocalAddress().getPort());
                    IoUtil.writeLong(addrIpv4 | (bizNode.getClusterNode().getIdentity()), x104_payload, 0);
                    X104_ExchangeIdentity x104 = new X104_ExchangeIdentity(x104_payload);
                    return new Triple<>(x104, session, WRITE_OPERATOR.PLAIN_SYMMETRY);
                default:
                    return new Triple<>(XF000_NULL.INSTANCE, session, IGNORE_ICOMMAND_OPERATOR.INSTANCE);
            }
        }
        catch (IOException e) {
            log.log(Level.WARNING, "Connection create failed, mode: " + mode.name(), e);
            return new Triple<>(XF000_NULL.INSTANCE, null, IGNORE_ICOMMAND_OPERATOR.INSTANCE);
        }
    }

}
