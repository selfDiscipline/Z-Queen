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
import java.nio.channels.CompletionHandler;

import com.tgx.zq.z.queen.base.disruptor.inf.IError;
import com.tgx.zq.z.queen.io.disruptor.operations.WROTE_ERROR_OPERATOR;
import com.tgx.zq.z.queen.io.disruptor.operations.WROTE_OPERATOR;
import com.tgx.zq.z.queen.io.inf.ISession;

public enum AioWriter
        implements
        CompletionHandler<Integer, ISession>
{
    INSTANCE;
    @Override
    public void completed(Integer result, ISession session) {
        AioWorker worker = (AioWorker) Thread.currentThread();
        switch (result) {
            case -1:
                worker.publishWroteError(WROTE_ERROR_OPERATOR.INSTANCE, IError.Type.WRITE_EOF, new EOFException("wrote -1!"), session);
                break;
            case 0:
                worker.publishWroteError(WROTE_ERROR_OPERATOR.INSTANCE,
                                         IError.Type.WRITE_ZERO,
                                         new IllegalArgumentException("wrote zero!"),
                                         session);
                break;
            default:
                worker.publishWrote(WROTE_OPERATOR.INSTANCE, result, session);
                break;
        }
    }

    @Override
    public void failed(Throwable exc, ISession session) {
        AioWorker worker = (AioWorker) Thread.currentThread();
        worker.publishWroteError(WROTE_ERROR_OPERATOR.INSTANCE, IError.Type.WRITE_FAILED, exc, session);
    }

}
