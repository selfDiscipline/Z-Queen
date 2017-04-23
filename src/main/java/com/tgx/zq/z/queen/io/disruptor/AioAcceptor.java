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

import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;

import com.tgx.zq.z.queen.io.inf.IAioServer;
import com.tgx.zq.z.queen.io.inf.IConnectMode;

public enum AioAcceptor
        implements
        CompletionHandler<AsynchronousSocketChannel, IAioServer>,
        IConnectMode
{
    CLUSTER(OPERATION_MODE.ACCEPT_CLUSTER),
    SERVER(OPERATION_MODE.ACCEPT_SERVER),
    SYMMETRY(OPERATION_MODE.SYMMETRY),
    MQ_SERVER(OPERATION_MODE.ACCEPT_MQ),
    SSL_SERVER(OPERATION_MODE.ACCEPT_SERVER_SSL);

    private final OPERATION_MODE mode;

    AioAcceptor(OPERATION_MODE mode) {
        this.mode = mode;
    }

    @Override
    public void completed(AsynchronousSocketChannel channel, IAioServer server) {
        AioWorker worker = (AioWorker) Thread.currentThread();
        worker.publishConnected(server.createNormal(), server, getMode(), channel);
        server.pendingAccept();
    }

    @Override
    public void failed(Throwable exc, IAioServer server) {
        AioWorker worker = (AioWorker) Thread.currentThread();
        worker.publishConnectingError(server.createError(), exc, server, server);
    }

    @Override
    public OPERATION_MODE getMode() {
        return mode;
    }

}
