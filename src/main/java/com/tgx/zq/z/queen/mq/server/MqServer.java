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

package com.tgx.zq.z.queen.mq.server;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.StandardSocketOptions;
import java.nio.channels.AsynchronousChannelGroup;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import com.tgx.zq.z.queen.base.disruptor.QEvent;
import com.tgx.zq.z.queen.base.disruptor.inf.IEventOp;
import com.tgx.zq.z.queen.base.util.Pair;
import com.tgx.zq.z.queen.base.util.Triple;
import com.tgx.zq.z.queen.biz.template.BizNode;
import com.tgx.zq.z.queen.db.bdb.inf.IBizDao;
import com.tgx.zq.z.queen.db.bdb.inf.IDbStorageProtocol;
import com.tgx.zq.z.queen.io.bean.XF000_NULL;
import com.tgx.zq.z.queen.io.disruptor.AioAcceptor;
import com.tgx.zq.z.queen.io.disruptor.operations.ws.WRITE_OPERATOR;
import com.tgx.zq.z.queen.io.impl.AioCreator;
import com.tgx.zq.z.queen.io.impl.AioSession;
import com.tgx.zq.z.queen.io.inf.IAioServer;
import com.tgx.zq.z.queen.io.inf.ICommand;
import com.tgx.zq.z.queen.io.inf.IConnectActive;
import com.tgx.zq.z.queen.io.inf.IConnected;
import com.tgx.zq.z.queen.io.inf.IContext;
import com.tgx.zq.z.queen.io.inf.IDestine;
import com.tgx.zq.z.queen.io.inf.ISession;
import com.tgx.zq.z.queen.io.inf.ISessionCreator;
import com.tgx.zq.z.queen.io.inf.ISessionOption;
import com.tgx.zq.z.queen.io.ws.protocol.WsContext;
import com.tgx.zq.z.queen.io.ws.protocol.bean.control.X101_Close;
import com.tgx.zq.z.queen.io.ws.protocol.bean.control.X104_ExchangeIdentity;
import com.tgx.zq.z.queen.io.ws.protocol.bean.mq.X80_MqTopicReg;
import com.tgx.zq.z.queen.io.ws.protocol.bean.mq.X81_MqTopicRegResult;
import com.tgx.zq.z.queen.mq.MqMode;
import com.tgx.zq.z.queen.mq.operations.MQ_CONNECTED;
import com.tgx.zq.z.queen.mq.operations.MQ_CONNECT_ERROR;
import com.tgx.zq.z.queen.mq.operations.MQ_READ;

public abstract class MqServer<E extends IDbStorageProtocol, D extends IBizDao<E>>
        implements
        IAioServer,
        IDestine
{
    private final BizNode<E, D>                              _BizNode;
    private final Map<Integer, Pair<MqMode, ArrayList<NCP>>> _ConsumerMap = new HashMap<>();
    private ISessionCreator                                  mqCreator    = new AioCreator()
                                                                          {
                                                                              @Override
                                                                              public IContext createContext(ISessionOption option,
                                                                                                            OPERATION_MODE mode) {
                                                                                  return new WsContext(option, mode);
                                                                              }

                                                                              @Override
                                                                              public ISession createSession(AsynchronousSocketChannel socketChannel,
                                                                                                            IConnectActive active) throws IOException {
                                                                                  return new AioSession(socketChannel,
                                                                                                        active,
                                                                                                        this,
                                                                                                        this,
                                                                                                        _BizNode,
                                                                                                        MQ_READ.INSTANCE);
                                                                              }
                                                                          };
    private InetSocketAddress                                mLocalAddress;
    private AsynchronousServerSocketChannel                  mServerChannel;

    protected MqServer(BizNode<E, D> bizNode) {
        _BizNode = bizNode;
    }

    public BizNode<E, D> getBizNode() {
        return _BizNode;
    }

    @Override
    public InetSocketAddress getLocalAddress() {
        return mLocalAddress;
    }

    @Override
    public void setLocalAddress(InetSocketAddress address) {
        mLocalAddress = address;
    }

    @Override
    public void bindAddress(InetSocketAddress address, AsynchronousChannelGroup channelGroup) throws IOException {
        mServerChannel = AsynchronousServerSocketChannel.open(channelGroup);
        mServerChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
        mServerChannel.bind(address, 1 << 6);
    }

    @Override
    public void pendingAccept() {
        if (mServerChannel.isOpen()) mServerChannel.accept(this, AioAcceptor.MQ_SERVER);
    }

    @SuppressWarnings("unchecked")
    @Override
    public IEventOp<Pair<IConnected, OPERATION_MODE>, AsynchronousSocketChannel> createNormal() {
        return MQ_CONNECTED.INSTANCE;
    }

    @SuppressWarnings("unchecked")
    @Override
    public IEventOp<Pair<IConnectActive, Throwable>, IConnectActive> createError() {
        return MQ_CONNECT_ERROR.INSTANCE;
    }

    @Override
    public ISessionCreator getCreator(SocketAddress address, OPERATION_MODE mode) {
        return mqCreator;
    }

    @Override
    public OPERATION_MODE getMode() {
        return OPERATION_MODE.ACCEPT_MQ;
    }

    public Triple<ICommand, ISession, IEventOp<ICommand, ISession>> consistentHandle(ICommand cmd, ISession session) {
        ICommand out = XF000_NULL.INSTANCE;
        switch (cmd.getSerialNum()) {
            case X80_MqTopicReg.COMMAND:
                X80_MqTopicReg x80 = (X80_MqTopicReg) cmd;
                X81_MqTopicRegResult x81 = new X81_MqTopicRegResult();
                x81.channel = x80.channel;
                List<Integer> errorTopics = null;
                if (x80.topicKeys != null) {
                    for (short topicKey : x80.topicKeys) {
                        int topic = topicKey & 0xFF;
                        MqMode mode = MqMode.parse(topicKey >>> 8);
                        Pair<MqMode, ArrayList<NCP>> pair = _ConsumerMap.get(topic);
                        NCP ncp = new NCP(x80.nodeId, x80.channel);
                        if (pair == null) {
                            ArrayList<NCP> list = new ArrayList<>();
                            list.add(ncp);
                            pair = new Pair<>(mode, list);
                            _ConsumerMap.put(topic, pair);
                        }
                        else {
                            if (!mode.equals(pair.first())) {
                                if (errorTopics == null) errorTopics = new LinkedList<>();
                                errorTopics.add(topic);
                                continue;
                            }
                            ArrayList<NCP> list = pair.second();
                            int pos = Collections.binarySearch(list, ncp);
                            if (pos < 0) pos = -1 - pos;
                            list.add(pos, ncp);
                        }
                    }
                }
                if (errorTopics != null) {
                    x81.topicErrors = new short[errorTopics.size()];
                    int i = 0;
                    for (int topic : errorTopics)
                        x81.topicErrors[i++] = (short) topic;
                }
                out = x81;
                break;
        }
        return new Triple<>(out, session, WRITE_OPERATOR.PLAIN_SYMMETRY);
    }

    public void dispatchEvent(QEvent event) {

    }

    @Override
    public RESULT trial(ICommand cmd, OPERATION_MODE mode) {
        if (mode.equals(OPERATION_MODE.ACCEPT_MQ)) {
            switch (cmd.getSerialNum()) {
                case X80_MqTopicReg.COMMAND:
                case X101_Close.COMMAND:
                case X104_ExchangeIdentity.COMMAND:
                    return RESULT.HANDLE;
            }
        }
        return null;
    }

    /* ClusterNode Channel Path */
    class NCP
            implements
            Comparable<NCP>
    {
        final long _NodeId;
        int        mChannel;

        NCP(long nodeId, int channel) {
            _NodeId = nodeId;
            mChannel = channel;
        }

        @Override
        public int compareTo(NCP o) {
            return _NodeId > o._NodeId ? 1 : _NodeId < o._NodeId ? -1 : 0;
        }
    }
}
