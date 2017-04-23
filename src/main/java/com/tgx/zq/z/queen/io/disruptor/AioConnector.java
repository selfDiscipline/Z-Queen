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

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.concurrent.TimeUnit;

import com.tgx.zq.z.queen.base.classic.task.timer.TimerTask;
import com.tgx.zq.z.queen.base.constant.QueenCode;
import com.tgx.zq.z.queen.base.disruptor.inf.IEventOp;
import com.tgx.zq.z.queen.base.util.IPParser;
import com.tgx.zq.z.queen.base.util.Pair;
import com.tgx.zq.z.queen.io.inf.IAioClient;
import com.tgx.zq.z.queen.io.inf.IConnectActive;
import com.tgx.zq.z.queen.io.inf.IConnected;
import com.tgx.zq.z.queen.io.inf.ISessionCreator;

public class AioConnector
        implements
        IConnectActive,
        IConnected,
        CompletionHandler<Void, AsynchronousSocketChannel>
{

    public final static int[]       RETRY_FIB_RATIO = { 1,
                                                        2,
                                                        3,
                                                        5,

                                                        8,
                                                        13,
                                                        21,
                                                        34,

                                                        55,
                                                        89,
                                                        144,
                                                        233,

                                                        377,
                                                        610,
                                                        987,
                                                        1597 };
    private static final int        RETRY_LIMIT     = RETRY_FIB_RATIO.length - 1;
    private final IAioClient        _Client;
    private final InetSocketAddress _RemoteAddress;
    private final InetSocketAddress _LocalAddress;
    private final String[]          _RemoteAddressHA;
    private final OPERATION_MODE    _Mode;
    private final int               _HaIndex;
    private final int               _PortIndex;
    private int                     retry;

    public AioConnector(final IAioClient client,
                        final OPERATION_MODE mode,
                        String localAddressStr,
                        int haIndex,
                        int portIndex,
                        String... remoteAddressStr) {

        if (haIndex < 0 || haIndex >= remoteAddressStr.length) throw new ArrayIndexOutOfBoundsException();
        _Client = client;
        _HaIndex = haIndex;
        _PortIndex = portIndex;
        _Mode = mode;
        Pair<InetAddress, Integer> local = IPParser.parse(localAddressStr);
        Pair<InetAddress, Integer> remote = IPParser.parse(remoteAddressStr[haIndex]);
        _LocalAddress = new InetSocketAddress(local.first(), local.second());
        _RemoteAddress = new InetSocketAddress(remote.first(), remote.second());
        _RemoteAddressHA = remoteAddressStr;
    }

    public AioConnector(final IAioClient client, final OPERATION_MODE mode, String localAddressStr, String remoteAddressStr) {
        this(client, mode, localAddressStr, 0, 0, remoteAddressStr);
    }

    public AioConnector(final IAioClient client, int haIndex, String... remoteAddressStr) {
        this(client, OPERATION_MODE.CONNECT_CONSUMER, "0.0.0.0:0", haIndex, 0, remoteAddressStr);
    }

    public AioConnector(final IAioClient client, OPERATION_MODE mode, int haIndex, String... remoteAddressStr) {
        this(client, mode, "0.0.0.0:0", haIndex, 0, remoteAddressStr);
    }

    public AioConnector(final IAioClient client, OPERATION_MODE mode, int haIndex, int portIndex, String... remoteAddressStr) {
        this(client, mode, "0.0.0.0:0", haIndex, portIndex, remoteAddressStr);
    }

    public AioConnector(final IAioClient client, String remoteAddressStr) {
        this(client, OPERATION_MODE.CONNECT_CONSUMER, "0.0.0.0:0", 0, 0, remoteAddressStr);
    }

    @Override
    public InetSocketAddress getLocalAddress() {
        return _LocalAddress;
    }

    @Override
    public InetSocketAddress getRemoteAddress() {
        return _RemoteAddress;
    }

    public IAioClient getClient() {
        return _Client;
    }

    @Override
    public IConnectActive getConnectActive() {
        return this;
    }

    public int haSpan() {
        return ((_HaIndex + 1) & QueenCode._HA_ROUTER_REMOTE_ADDRESS_INDEX_MASK) % _RemoteAddressHA.length;
    }

    public boolean canRetry(int _RetryLimit) {
        return retry < Math.min(RETRY_LIMIT, _RetryLimit);
    }

    public int getRetry() {
        return retry;
    }

    public void setRetry() {
        if (Integer.bitCount(RETRY_FIB_RATIO.length) != 1) throw new IllegalArgumentException(" Fibonacci-Ratio array's length is not power of 2");
        retry = (retry + 1) & RETRY_LIMIT;
    }

    @Override
    public int getPortIndex() {
        return _PortIndex;
    }

    @Override
    public int getHaIndex() {
        return _HaIndex;
    }

    @Override
    public OPERATION_MODE getMode() {
        return _Mode;
    }

    @Override
    public void completed(Void result, AsynchronousSocketChannel channel) {
        AioWorker worker = (AioWorker) Thread.currentThread();
        worker.publishConnected(_Client.createNormal(), this, getMode(), channel);
    }

    @Override
    public void failed(Throwable exc, AsynchronousSocketChannel channel) {
        AioWorker worker = (AioWorker) Thread.currentThread();
        worker.publishConnectingError(_Client.createError(), exc, _Client, this);
    }

    public String[] getRemoteAddressHA() {
        return _RemoteAddressHA;
    }

    @Override
    public ISessionCreator getCreator(SocketAddress address, OPERATION_MODE mode) {
        return _Client.getCreator(address, mode);
    }

    @Override
    public <V, A> IEventOp<V, A> createNormal() {
        return _Client.createNormal();
    }

    @Override
    public <E, H> IEventOp<E, H> createError() {
        return _Client.createError();
    }

    public class Timer
            extends
            TimerTask
    {
        public final static int SerialNum = SuperSerialNum + 401;
        /* 存在伪共享访问，由于使用频率较低，即使在大量地址不可用时，依然不会出现 N 次/秒，忽略 */
        private final long[]    _HaConnectTimeTickArray;

        public Timer(int delaySecond, long[] timeTickArray) {
            super(delaySecond);
            _HaConnectTimeTickArray = timeTickArray;
        }

        @Override
        public int getSerialNum() {
            return SerialNum;
        }

        @Override
        protected boolean doTimeMethod() {
            if (_HaConnectTimeTickArray[_HaIndex] > doTime) return true;
            try {
                _Client.connect(AioConnector.this);
            }
            catch (IOException e) {
                setError(e);
                commitResult(this);
            }
            return true;
        }
    }

    public class NoHaTimer
            extends
            TimerTask
    {
        public final static int SerialNum = SuperSerialNum + 402;
        private final long[]    _PortConnectTimeTickArray;

        public NoHaTimer(int delaySecond, long[] timeTickArray) {
            super(delaySecond);
            _PortConnectTimeTickArray = timeTickArray;
        }

        public NoHaTimer(long delayMills, long[] timeTickArray) {
            super(delayMills, TimeUnit.MILLISECONDS);
            _PortConnectTimeTickArray = timeTickArray;
        }

        @Override
        public int getSerialNum() {
            return SerialNum;
        }

        @Override
        protected boolean doTimeMethod() {
            if (_PortConnectTimeTickArray[_PortIndex] > doTime) return true;
            try {
                _Client.connect(AioConnector.this);
            }
            catch (IOException e) {
                setError(e);
                commitResult(this);
            }
            return true;
        }
    }

}
