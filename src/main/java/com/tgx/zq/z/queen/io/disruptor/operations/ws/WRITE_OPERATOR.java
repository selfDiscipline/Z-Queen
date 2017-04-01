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
import com.tgx.zq.z.queen.io.disruptor.AioWriter;
import com.tgx.zq.z.queen.io.disruptor.operations.CLOSE_ERROR_OPERATOR;
import com.tgx.zq.z.queen.io.inf.ICommand;
import com.tgx.zq.z.queen.io.inf.IConnectMode;
import com.tgx.zq.z.queen.io.inf.IFilterChain;
import com.tgx.zq.z.queen.io.inf.IPipeWrite;
import com.tgx.zq.z.queen.io.inf.ISession;
import com.tgx.zq.z.queen.io.ws.filter.WsControlFilter;
import com.tgx.zq.z.queen.io.ws.filter.WsFrameFilter;
import com.tgx.zq.z.queen.io.ws.filter.WsHandShakeFilter;
import com.tgx.zq.z.queen.io.ws.filter.ZCommandFilter;
import com.tgx.zq.z.queen.io.ws.filter.ZTlsFilter;
import com.tgx.zq.z.queen.io.ws.protocol.WsContext;

public enum WRITE_OPERATOR
        implements
        IEventOp<ICommand, ISession>,
        IPipeWrite
{
    PLAIN_SYMMETRY(IConnectMode.OPERATION_MODE.SYMMETRY, false, false),
    CIPHER_SYMMETRY(IConnectMode.OPERATION_MODE.SYMMETRY, false, true),
    CIPHER_ACCEPT(IConnectMode.OPERATION_MODE.ACCEPT_SERVER, false, true),
    CIPHER_CONSUMER(IConnectMode.OPERATION_MODE.CONNECT_CONSUMER, false, true),
    CIPHER_HANDSHAKE_ACCEPT(IConnectMode.OPERATION_MODE.ACCEPT_SERVER, true, true),
    CIPHER_HANDSHAKE_CONSUMER(IConnectMode.OPERATION_MODE.CONNECT_CONSUMER, true, true);
    private final IFilterChain<WsContext> _Filter;

    @SuppressWarnings("unchecked")
    WRITE_OPERATOR(IConnectMode.OPERATION_MODE mode, boolean handshake, boolean encrypt) {
        IFilterChain<?> tlsFilter = encrypt ? new ZTlsFilter() : null;

        _Filter = handshake ? new WsHandShakeFilter(mode).linkAfter(encrypt ? (IFilterChain<WsContext>) tlsFilter : null)
                                                         .linkFront(new WsFrameFilter())
                            : new WsFrameFilter().linkAfter(encrypt ? (IFilterChain<WsContext>) tlsFilter : null);
        _Filter.linkFront(new ZCommandFilter()).linkFront(new WsControlFilter());
    }

    @SuppressWarnings("unchecked")
    @Override
    public Triple<Throwable, ISession, IEventOp<Throwable, ISession>> handle(ICommand _inCommand, ISession session) {
        Throwable t = sessionWrite(_inCommand, _Filter, session, AioWriter.INSTANCE);
        return t == null ? null : new Triple<>(t, session, CLOSE_ERROR_OPERATOR.INSTANCE);
    }
}
