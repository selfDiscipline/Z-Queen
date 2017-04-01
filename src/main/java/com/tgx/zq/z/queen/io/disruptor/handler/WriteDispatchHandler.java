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

import static com.tgx.zq.z.queen.base.disruptor.inf.IEventOp.Type.WROTE;

import java.util.List;

import com.lmax.disruptor.RingBuffer;
import com.tgx.zq.z.queen.base.disruptor.QEvent;
import com.tgx.zq.z.queen.base.disruptor.inf.IEventOp;
import com.tgx.zq.z.queen.base.disruptor.inf.IEventOp.Type;
import com.tgx.zq.z.queen.base.disruptor.inf.IPipeEventHandler;
import com.tgx.zq.z.queen.base.util.Pair;
import com.tgx.zq.z.queen.base.util.Triple;
import com.tgx.zq.z.queen.io.impl.AioSession;
import com.tgx.zq.z.queen.io.inf.ICommand;

public class WriteDispatchHandler
        implements
        IPipeEventHandler<QEvent, QEvent>
{

    private final RingBuffer<QEvent>[] encodeBuffers;
    private final int                  mEncoderMask;
    private int                        roundRobin;

    @SafeVarargs
    public WriteDispatchHandler(RingBuffer<QEvent>... buffers) {
        encodeBuffers = buffers;
        if (Integer.bitCount(encodeBuffers.length) != 1) throw new IllegalArgumentException("bufferSize must be a power of 2");
        mEncoderMask = buffers.length - 1;
    }

    @Override
    public void onEvent(QEvent event, long sequence, boolean endOfBatch) throws Exception {
        switch (event.getEventType()) {
            case DISPATCH:
                IEventOp<ICommand, AioSession> pOperator = event.getEventOp();
                Pair<ICommand, AioSession> pContent = event.getContent();
                AioSession session = pContent.second();
                pContent.first().setSequence(sequence);
                boolean dispatched;
                RingBuffer<QEvent> encodeBuffer;
                do {
                    encodeBuffer = encodeBuffers[(session.isWroteFinish() ? roundRobin++ : session.getHashKey()) & mEncoderMask];
                    dispatched = tryPublish(encodeBuffer, Type.WRITE, pContent.first(), session, pOperator);// 队列满且未写完的链路上将会抛弃消息
                }
                while (!dispatched && session.isWroteFinish());
                if (!dispatched && !session.isWroteFinish()) log.warning("content drop!:" + pContent.first());
                break;
            case MULTI_DISPATCH:
                List<Triple<ICommand, AioSession, IEventOp<ICommand, AioSession>>> mContentList = event.getContentList();
                for (Triple<ICommand, AioSession, IEventOp<ICommand, AioSession>> triple : mContentList) {
                    do {
                        session = triple.second();
                        encodeBuffer = encodeBuffers[session.isWroteFinish() ? roundRobin++ : session.getHashKey() & mEncoderMask];
                        dispatched = tryPublish(encodeBuffer, Type.WRITE, triple.first(), session, triple.third());
                    }
                    while (!dispatched && session.isWroteFinish());
                    if (!dispatched && !session.isWroteFinish()) log.warning("content drop!:" + triple.first());
                }
                break;
            case WROTE:
                IEventOp<Integer, AioSession> wOperator = event.getEventOp();
                Pair<Integer, AioSession> wContent = event.getContent();
                session = wContent.second();
                encodeBuffer = encodeBuffers[session.getHashKey() & mEncoderMask];
                tryPublish(encodeBuffer, WROTE, wContent.first(), session, wOperator);
                break;
            default:
                log.warning("invalid event type: " + event.getEventType().name());
                break;
        }
        event.reset();
    }
}
