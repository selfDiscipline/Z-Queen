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
package com.tgx.zq.z.queen.io.disruptor.handler;

import java.util.logging.Level;
import java.util.logging.Logger;

import com.lmax.disruptor.RingBuffer;
import com.tgx.zq.z.queen.base.disruptor.QEvent;
import com.tgx.zq.z.queen.base.disruptor.inf.IError;
import com.tgx.zq.z.queen.base.disruptor.inf.IEventOp;
import com.tgx.zq.z.queen.base.disruptor.inf.IPipeEventHandler;
import com.tgx.zq.z.queen.base.util.Pair;
import com.tgx.zq.z.queen.io.impl.AioSession;

public class WriteEndHandler
        implements
        IPipeEventHandler<QEvent, QEvent>
{

    private final RingBuffer<QEvent> writeErrBuf;
    Logger                           log = Logger.getLogger(getClass().getName());

    public WriteEndHandler(RingBuffer<QEvent> buffer) {
        writeErrBuf = buffer;
    }

    @Override
    public void onEvent(QEvent event, long sequence, boolean endOfBatch) throws Exception {
        if (!event.noError()) {
            IEventOp<Throwable, AioSession> sOperator = event.getEventOp();// CLOSE_ERROR_OPERATOR
            Pair<Throwable, AioSession> sContent = event.getContent();
            error(writeErrBuf, IError.Type.CLOSED, sContent.first(), sContent.second(), sOperator);
            /*-------------------------------------------------------------------------------------*/
            log.log(Level.INFO, "ErrorType : " + event.getEventType().name(), sContent.first());
        }
        else {
            Pair<?, AioSession> content = event.getContent();
            AioSession session = content.second();
            if (session.isClosed()) session.dispose();
        }
        event.reset();
    }
}
