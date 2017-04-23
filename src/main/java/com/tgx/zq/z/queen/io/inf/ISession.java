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

package com.tgx.zq.z.queen.io.inf;

import java.io.Closeable;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;

import com.tgx.zq.z.queen.base.disruptor.inf.IEventOp;
import com.tgx.zq.z.queen.base.inf.IDisposable;
import com.tgx.zq.z.queen.base.inf.IReset;

/**
 * @author William.d.zk
 */
public interface ISession
        extends
        IReset,
        Closeable,
        IDisposable,
        IConnectActive,
        IValid,
        IReadable<CompletionHandler<Integer, ISession>>,
        IWritable<CompletionHandler<Integer, ISession>>
{
    long _DEFAULT_INDEX = -1;

    long getIndex();

    void setIndex(long _Index);

    int getHashKey();

    AsynchronousSocketChannel getChannel();

    int getReadTimeOut();

    int getHeartBeatSap();

    long nextBeat();

    <P> IEventOp<P, ISession> getReadOperator();

    IContext getContext();

    ISessionDismiss getDismissCallback();

    long[] getPortChannels();

    void bindport2channel(long portId);

    boolean isClosed();

}
