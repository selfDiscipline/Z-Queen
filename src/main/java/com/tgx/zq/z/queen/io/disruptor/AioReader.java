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

import java.io.EOFException;
import java.nio.ByteBuffer;
import java.nio.channels.CompletionHandler;
import java.nio.channels.NotYetConnectedException;
import java.nio.channels.ShutdownChannelGroupException;
import java.util.logging.Logger;

import com.tgx.zq.z.queen.base.disruptor.inf.IError;
import com.tgx.zq.z.queen.io.disruptor.operations.READ_ERROR_OPERATOR;
import com.tgx.zq.z.queen.io.inf.ISession;

/**
 * @author William.d.zk
 */
public enum AioReader
        implements
        CompletionHandler<Integer, ISession>
{

    INSTANCE;
    Logger log = Logger.getLogger(AioReader.class.getSimpleName());

    @Override
    public void completed(Integer read, ISession session) {
        AioWorker worker = (AioWorker) Thread.currentThread();
        switch (read) {
            case -1:
                worker.publishReadError(READ_ERROR_OPERATOR.INSTANCE, IError.Type.READ_EOF, new EOFException("Read Negative"), session);
                break;
            case 0:
                worker.publishReadError(READ_ERROR_OPERATOR.INSTANCE,
                                        IError.Type.READ_ZERO,
                                        new IllegalStateException("Read Zero"),
                                        session);
                session.readNext(this);
                break;
            default:
                log.warning("READ: " + read);

                ByteBuffer recvBuf = session.read(read);
                worker.publishRead(session.getReadOperator(), recvBuf, session);
                try {
                    session.readNext(this);
                }
                catch (NotYetConnectedException |
                       ShutdownChannelGroupException e) {
                    worker.publishReadError(READ_ERROR_OPERATOR.INSTANCE, IError.Type.READ_FAILED, e, session);
                }
                break;
        }
    }

    @Override
    public void failed(Throwable exc, ISession session) {
        AioWorker worker = (AioWorker) Thread.currentThread();
        worker.publishReadError(READ_ERROR_OPERATOR.INSTANCE, IError.Type.READ_FAILED, exc, session);
    }

}
