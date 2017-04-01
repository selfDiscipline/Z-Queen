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
package com.tgx.zq.z.queen.io.manager;

import java.io.IOException;
import java.nio.channels.AsynchronousChannelGroup;
import java.util.Map;
import java.util.concurrent.ThreadFactory;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.lmax.disruptor.InsufficientCapacityException;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.tgx.zq.z.queen.base.disruptor.QEvent;
import com.tgx.zq.z.queen.base.disruptor.inf.IEventOp.Type;
import com.tgx.zq.z.queen.base.util.Pair;
import com.tgx.zq.z.queen.io.disruptor.operations.CLOSE_OPERATOR;
import com.tgx.zq.z.queen.io.impl.AioSessionManager;
import com.tgx.zq.z.queen.io.inf.ICreatorFactory;
import com.tgx.zq.z.queen.io.inf.ISession;

public abstract class QueenManager
        extends
        AioSessionManager
        implements
        ICreatorFactory
{
    public static Logger                         log = Logger.getLogger(QueenManager.class.getName());

    private final RingBuffer<QEvent>             mLocalBackBuffer, mLocalSendBuffer;
    // default route path map capacity is 1M.
    private final Map<Long, Pair<Long, Boolean>> _RouteMap;
    private boolean                              initialized;
    private AsynchronousChannelGroup             channelGroup;

    protected QueenManager(final Map<Long, ISession> sessionMap, final Map<Long, Pair<Long, Boolean>> routeMap) throws IOException {
        super(sessionMap);
        _RouteMap = routeMap;
        mLocalBackBuffer = RingBuffer.createSingleProducer(QEvent.EVENT_FACTORY, 1 << 13, new YieldingWaitStrategy());
        mLocalSendBuffer = RingBuffer.createSingleProducer(QEvent.EVENT_FACTORY, 1 << 18, new YieldingWaitStrategy());
    }

    public final AsynchronousChannelGroup getChannelGroup() {
        if (!initialized) throw new IllegalStateException("state error: mananger hasn't been initialized!");
        return channelGroup;
    }

    public final QueenManager initialize(final int channelGroupPoolSize, final ThreadFactory threadFactory) throws IOException {
        channelGroup = AsynchronousChannelGroup.withFixedThreadPool(channelGroupPoolSize, threadFactory);
        initialized = true;
        return this;
    }

    public final RingBuffer<QEvent> getLocalBackBuffer() {
        return mLocalBackBuffer;
    }

    public final RingBuffer<QEvent> getLocalSendBuffer() {
        return mLocalSendBuffer;
    }

    public final void localClose(ISession session) {
        if (session != null) try {
            long sequence = mLocalBackBuffer.tryNext();
            try {
                QEvent event = mLocalBackBuffer.get(sequence);
                event.produce(Type.CLOSE, null, session, CLOSE_OPERATOR.INSTANCE);
            }
            finally {
                mLocalBackBuffer.publish(sequence);
            }

        }
        catch (InsufficientCapacityException e) {
            log.log(Level.FINER, "local close -> full", e);
        }
    }

    public final void localClose(long _index) {
        localClose(findSessionByIndex(_index));
    }

    // in link handler do this
    public final void routeBind(long identify, long port) {
        if (_RouteMap.containsKey(identify)) {
            Pair<Long, Boolean> p = _RouteMap.get(identify);
            p.setFirst(port);
            p.setSecond(true);
        }
        else _RouteMap.put(identify, new Pair<>(port, true));
    }

    // in logic handler do this
    public final ISession getRoutePath(long identify) {
        Pair<Long, Boolean> p = _RouteMap.get(identify);
        if (p == null || !p.second()) return null;
        ISession session;
        session = p.first() == identify ? findSessionByIndex(identify) : findSessionByPort(p.first());
        if (session == null || session.isClosed()) {
            p.setSecond(false);
            return null;
        }
        return session;
    }

}
