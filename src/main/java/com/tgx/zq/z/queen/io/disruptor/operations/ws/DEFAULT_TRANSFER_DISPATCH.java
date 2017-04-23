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

package com.tgx.zq.z.queen.io.disruptor.operations.ws;

import com.tgx.zq.z.queen.base.disruptor.inf.IEventOp;
import com.tgx.zq.z.queen.base.util.Triple;
import com.tgx.zq.z.queen.io.inf.ICommand;
import com.tgx.zq.z.queen.io.inf.ISession;

public enum DEFAULT_TRANSFER_DISPATCH
        implements
        IEventOp<ICommand[], ISession>
{
    PLAIN_SYMMETRY(DEFAULT_TRANSFER_LOGIC.PLAIN_SYMMETRY),
    CIPHER_SYMMETRY(DEFAULT_TRANSFER_LOGIC.CIPHER_SYMMETRY),
    CIPHER_ACCEPT(DEFAULT_TRANSFER_LOGIC.CIPHER_ACCEPT),
    CIPHER_CONSUMER(DEFAULT_TRANSFER_LOGIC.CIPHER_CONSUMER),
    CIPHER_HANDSHAKE_ACCEPT(DEFAULT_TRANSFER_LOGIC.CIPHER_HANDSHAKE_ACCEPT),
    CIPHER_HANDSHAKE_CONSUMER(DEFAULT_TRANSFER_LOGIC.CIPHER_HANDSHAKE_CONSUMER);
    private final IEventOp<ICommand, ISession> _Logic;

    DEFAULT_TRANSFER_DISPATCH(IEventOp<ICommand, ISession> logic) {
        _Logic = logic;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Triple<ICommand, ISession, IEventOp<ICommand, ISession>>[] handleResultArray(ICommand[] inCommands, ISession session) {
        if (inCommands == null) { return null; }

        Triple<ICommand, ISession, IEventOp<ICommand, ISession>>[] result = new Triple[inCommands.length];

        for (int i = 0, size = inCommands.length; i < size; i++) {
            result[i] = new Triple<>(inCommands[i], session, _Logic);
        }
        return result;
    }
}
