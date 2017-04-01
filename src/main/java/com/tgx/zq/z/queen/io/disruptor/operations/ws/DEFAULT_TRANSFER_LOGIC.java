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

package com.tgx.zq.z.queen.io.disruptor.operations.ws;

import com.tgx.zq.z.queen.base.disruptor.inf.IEventOp;
import com.tgx.zq.z.queen.base.util.Triple;
import com.tgx.zq.z.queen.io.inf.ICommand;
import com.tgx.zq.z.queen.io.inf.ISession;
import com.tgx.zq.z.queen.io.ws.protocol.bean.control.X101_Close;
import com.tgx.zq.z.queen.io.ws.protocol.bean.control.X102_Ping;
import com.tgx.zq.z.queen.io.ws.protocol.bean.control.X103_Pong;

public enum DEFAULT_TRANSFER_LOGIC
        implements
        IEventOp<ICommand, ISession>

{

    PLAIN_SYMMETRY(WRITE_OPERATOR.PLAIN_SYMMETRY),
    CIPHER_SYMMETRY(WRITE_OPERATOR.CIPHER_SYMMETRY),
    CIPHER_ACCEPT(WRITE_OPERATOR.CIPHER_ACCEPT),
    CIPHER_CONSUMER(WRITE_OPERATOR.CIPHER_CONSUMER),
    CIPHER_HANDSHAKE_ACCEPT(WRITE_OPERATOR.CIPHER_HANDSHAKE_ACCEPT),
    CIPHER_HANDSHAKE_CONSUMER(WRITE_OPERATOR.CIPHER_HANDSHAKE_CONSUMER);
    private final WRITE_OPERATOR _WriteOperator;

    DEFAULT_TRANSFER_LOGIC(WRITE_OPERATOR writeOperator) {
        _WriteOperator = writeOperator;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Triple<ICommand, ISession, IEventOp<ICommand, ISession>> handle(ICommand inCommand, ISession session) {
        ICommand outCommand;
        switch (inCommand.getSerialNum()) {
            case X102_Ping.COMMAND:
                outCommand = session.getIndex() == ISession._DEFAULT_INDEX ? new X101_Close() : new X103_Pong();
                break;
            default:
                outCommand = inCommand;
                break;
        }
        return new Triple<>(outCommand, session, _WriteOperator);

    }
}
