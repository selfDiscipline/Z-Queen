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
package com.tgx.zq.z.queen.io.impl;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.nio.channels.NotYetConnectedException;
import java.nio.channels.ShutdownChannelGroupException;
import java.nio.channels.WritePendingException;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import com.tgx.zq.z.queen.base.disruptor.inf.IEventOp;
import com.tgx.zq.z.queen.base.util.ArrayUtil;
import com.tgx.zq.z.queen.base.util.TimeUtil;
import com.tgx.zq.z.queen.io.inf.IConnectActive;
import com.tgx.zq.z.queen.io.inf.IContext;
import com.tgx.zq.z.queen.io.inf.IContextCreator;
import com.tgx.zq.z.queen.io.inf.IPoS;
import com.tgx.zq.z.queen.io.inf.ISession;
import com.tgx.zq.z.queen.io.inf.ISessionDismiss;
import com.tgx.zq.z.queen.io.inf.ISessionOption;

/**
 * @author William.d.zk
 */
public class AioSession
        implements
        ISession
{
    static Logger                           log       = Logger.getLogger(AioSession.class.getSimpleName());
    /*--------------------------------------------------------------------------------------------------------------*/

    private final int                       _ReadTimeOut;
    private final int                       _HeartBeatSap;
    private final int                       _WriteTimeOut;
    private final AsynchronousSocketChannel _Channel;
    private final InetSocketAddress         _RemoteAddress, _LocalAddress;
    /*
     * 与系统的 SocketOption 的 RecvBuffer 相等大小， 至少可以一次性将系统 Buffer 中的数据全部转存
     */
    private final ByteBuffer                _RecvBuf;
    private final int                       _HashKey;
    private final IContext                  _Ctx;
    private final OPERATION_MODE            _Mode;
    private final ISessionDismiss           _DismissCallback;
    private final IEventOp<IPoS, ISession>  _InOperator;
    private final Queue<IPoS>               _SendQueue;
    private final int                       _QueueSizeMax;
    private final int                       _HaIndex, _PortIndex;
    /*----------------------------------------------------------------------------------------------------------------*/

    private long                            mIndex    = _DEFAULT_INDEX;
    /*
     * 此处并不会进行空间初始化，完全依赖于 Context 的 WrBuf 初始化大小
     */
    private ByteBuffer                      mSending;
    /*
     * Session close 只能出现在 SessionManager 的工作线程中 所以关闭操作只需要做到全域线程可见即可，不需要处理写冲突
     */
    private volatile boolean                vClosed;
    private long[]                          mPortChannels;

    private int                             mWroteExpect;
    private int                             mSendingBlank;
    private int                             mWaitWrite;
    private boolean                         bWrFinish = true;
    private int                             mQueueSize;
    private long                            mReadTimeStamp;

    public AioSession(final AsynchronousSocketChannel channel,
                      final IConnectActive active,
                      final IContextCreator contextCreator,
                      final ISessionOption sessionOption,
                      final ISessionDismiss sessionDismiss,
                      final IEventOp<IPoS, ISession> operator)
            throws IOException {

        _Channel = channel;
        _Mode = active.getMode();
        _RemoteAddress = (InetSocketAddress) channel.getRemoteAddress();
        _LocalAddress = (InetSocketAddress) channel.getLocalAddress();
        _DismissCallback = sessionDismiss;
        _SendQueue = new LinkedList<>();
        _HashKey = hashCode();
        _PortIndex = active.getPortIndex();
        _HaIndex = active.getHaIndex();
        if (sessionOption == null) throw new NullPointerException("SessionOption is null");
        sessionOption.setOptions(channel);
        _Ctx = contextCreator.createContext(sessionOption, _Mode);
        _ReadTimeOut = sessionOption.setReadTimeOut();
        _HeartBeatSap = (_ReadTimeOut >> 1) - 1;
        _WriteTimeOut = sessionOption.setWriteTimeOut();
        _RecvBuf = ByteBuffer.allocate(sessionOption.setRCV());
        _QueueSizeMax = sessionOption.setQueueMax();
        _InOperator = operator;
        mSending = _Ctx.getWrBuffer();
        mSending.flip();
        mSendingBlank = mSending.capacity() - mSending.limit();
    }

    @Override
    public boolean isValid() {
        return !isClosed();
    }

    @Override
    public InetSocketAddress getLocalAddress() {
        return _LocalAddress;
    }

    @Override
    public void setLocalAddress(InetSocketAddress address) {
        throw new UnsupportedOperationException(" final member!");
    }

    @Override
    public InetSocketAddress getRemoteAddress() {
        return _RemoteAddress;
    }

    @Override
    public void setRemoteAddress(InetSocketAddress address) {
        throw new UnsupportedOperationException(" final member!");
    }

    @Override
    public int getHaIndex() {
        return _HaIndex;
    }

    @Override
    public int getPortIndex() {
        return _PortIndex;
    }

    @Override
    public void reset() {
        mIndex = -1;
    }

    @Override
    public final void dispose() {
        _Ctx.dispose();
        mSending = null;
        _SendQueue.clear();
        mPortChannels = null;
        reset();
    }

    @Override
    public final void close() throws IOException {
        if (isClosed()) return;
        vClosed = true;
        _Ctx.close();
        _Channel.close();
    }

    @Override
    public final boolean isClosed() {
        return vClosed;
    }

    @Override
    public final long getIndex() {
        return mIndex;
    }

    @Override
    public final void setIndex(long index) {
        this.mIndex = index;
    }

    @Override
    public final void bindport2channel(long channelport) {
        mPortChannels = mPortChannels == null ? new long[] { channelport } : ArrayUtil.setSortAdd(channelport, mPortChannels);
    }

    @Override
    public final long[] getPortChannels() {
        return mPortChannels;
    }

    @Override
    public final int getHashKey() {
        return _HashKey;
    }

    @Override
    public final AsynchronousSocketChannel getChannel() {
        return _Channel;
    }

    @Override
    public final int getReadTimeOut() {
        return _ReadTimeOut;
    }

    @Override
    public final int getHeartBeatSap() {
        return _HeartBeatSap;
    }

    @Override
    public final void readNext(CompletionHandler<Integer, ISession> readHandler) throws NotYetConnectedException,
                                                                                 ShutdownChannelGroupException {
        if (isClosed()) return;
        _RecvBuf.clear();
        _Channel.read(_RecvBuf, _ReadTimeOut, TimeUnit.SECONDS, this, readHandler);
    }

    public final ByteBuffer read(int length) {
        mReadTimeStamp = TimeUtil.CURRENT_TIME_MILLIS_CACHE;
        if (length < 0) throw new IllegalArgumentException();
        if (length != _RecvBuf.position()) throw new ArrayIndexOutOfBoundsException();
        ByteBuffer read = ByteBuffer.allocate(length);
        _RecvBuf.flip();
        read.put(_RecvBuf);
        read.flip();
        return read;
    }

    @Override
    public long nextBeat() {
        long delta = TimeUtil.CURRENT_TIME_MILLIS_CACHE - mReadTimeStamp;
        return delta >= TimeUnit.SECONDS.toMillis(_HeartBeatSap) ? -1 : TimeUnit.SECONDS.toMillis(_HeartBeatSap) - delta;
    }

    @SuppressWarnings("unchecked")
    @Override
    public IEventOp<IPoS, ISession> getReadOperator() {
        return _InOperator;
    }

    public IContext getContext() {
        return _Ctx;
    }

    private boolean qOffer(IPoS ps) {
        boolean result;
        mQueueSize += (result = _SendQueue.offer(ps)) ? 1 : 0;
        return result;
    }

    private void qRemove(IPoS ps) {
        mQueueSize -= _SendQueue.remove(ps) ? 1 : 0;
    }

    @Override
    public WRITE_STATUS write(IPoS ps, CompletionHandler<Integer, ISession> handler) throws WritePendingException,
                                                                                     NotYetConnectedException,
                                                                                     ShutdownChannelGroupException,
                                                                                     RejectedExecutionException {
        if (isClosed()) return WRITE_STATUS.CLOSED;
        ps.waitSend();
        if (_SendQueue.isEmpty()) {
            WRITE_STATUS status = writeChannel(ps);
            switch (status) {
                case IGNORE:
                    return status;
                case UNFINISHED:
                    qOffer(ps);
                case IN_SENDING:
                    ps.send();
                default:
                    if (mWaitWrite > 0 && bWrFinish) flush(handler);
                    return status;
            }
        }
        else if (mQueueSize > _QueueSizeMax) throw new RejectedExecutionException();
        else {
            IPoS fps = _SendQueue.peek();
            switch (writeChannel(fps)) {
                case IGNORE:
                    break;
                case UNFINISHED:
                case IN_SENDING:
                    fps.send();
                default:
                    if (mWaitWrite > 0 && bWrFinish) flush(handler);
                    break;
            }
            qOffer(ps);
            return WRITE_STATUS.UNFINISHED;
        }
    }

    @Override
    public WRITE_STATUS writeNext(int wroteCnt, CompletionHandler<Integer, ISession> handler) throws WritePendingException,
                                                                                              NotYetConnectedException,
                                                                                              ShutdownChannelGroupException,
                                                                                              RejectedExecutionException {
        if (isClosed()) return WRITE_STATUS.CLOSED;
        mWroteExpect -= wroteCnt;
        bWrFinish = true;
        if (mWroteExpect == 0) {
            mSending.clear();
            mSending.flip();
        }
        if (_SendQueue.isEmpty()) return WRITE_STATUS.IGNORE;
        IPoS fps;
        Loop:
        do {
            fps = _SendQueue.peek();
            if (fps != null) switch (writeChannel(fps)) {
                case IGNORE:
                    continue;
                case UNFINISHED:
                    fps.send();
                    break Loop;
                case IN_SENDING:
                    fps.sent();
                default:
                    break;
            }
        }
        while (fps == null);
        if (mWaitWrite > 0) {
            flush(handler);
            return WRITE_STATUS.FLUSHED;
        }
        return WRITE_STATUS.IGNORE;
    }

    private WRITE_STATUS writeChannel(IPoS ps) {
        ByteBuffer buf = ps.getBuffer();
        if (buf != null && buf.hasRemaining()) {
            mWroteExpect += buf.remaining();
            mWaitWrite = mSending.remaining();
            mSendingBlank = mSending.capacity() - mSending.limit();
            int pos = mSending.limit();
            int size = Math.min(mSendingBlank, buf.remaining());
            mSending.limit(pos + size);
            for (int i = 0; i < size; i++, mSendingBlank--, mWaitWrite++, pos++)
                mSending.put(pos, buf.get());
            if (buf.hasRemaining()) return WRITE_STATUS.UNFINISHED;
            else {
                qRemove(ps);
                return WRITE_STATUS.IN_SENDING;
            }
        }
        else qRemove(ps);
        return WRITE_STATUS.IGNORE;
    }

    private void flush(CompletionHandler<Integer, ISession> handler) throws WritePendingException,
                                                                     NotYetConnectedException,
                                                                     ShutdownChannelGroupException {
        bWrFinish = false;
        _Channel.write(mSending, _WriteTimeOut, TimeUnit.MILLISECONDS, this, handler);
    }

    @Override
    public boolean isWroteFinish() {
        return bWrFinish;
    }

    @Override
    public OPERATION_MODE getMode() {
        return _Mode;
    }

    @Override
    public ISessionDismiss getDismissCallback() {
        return _DismissCallback;
    }

}
