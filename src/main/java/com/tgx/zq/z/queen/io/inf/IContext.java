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
import java.nio.ByteBuffer;

import com.tgx.zq.z.queen.base.inf.IDisposable;
import com.tgx.zq.z.queen.base.inf.IReset;

/**
 * @author William.d.zk
 */
public interface IContext
        extends
        ITlsContext,
        IReset,
        IDisposable,
        Closeable
{
    boolean isCrypt();

    void handshake();

    void tls();

    void transfer();

    boolean hasHandshake();

    int lackLength(int length, int target);

    int position();

    int lack();

    void finish();

    EncodeState getEncodeState();

    IContext setEncodeState(EncodeState state);

    DecodeState getDecodeState();

    IContext setDecodeState(DecodeState state);

    ChannelState getChannelState();

    IContext setChannelState(ChannelState state);

    ByteBuffer getWrBuffer();

    ByteBuffer getRvBuffer();

    int getSendMaxSize();

    long getNetTransportDelay();

    /**
     *
     * @return negative client ahead server , otherwise client behind server
     */
    long getDeltaTime();

    void ntp(long clientStart, long serverArrived, long serverResponse, long clientArrived);

    long getNtpArrivedTime();

    enum DecodeState
    {
        NULL,
        DECODED_TLS,
        DECODED_TLS_ERROR,
        DECODE_HANDSHAKE,
        DECODING_FRAME,
        DECODE_ERROR
    }

    enum EncodeState
    {
        NULL,
        ENCODED_TLS,
        ENCODED_TLS_ERROR,
        ENCODE_HANDSHAKE,
        ENCODING_FRAME,
        ENCODE_ERROR
    }

    enum ChannelState
    {
        CONNECTING,
        NORMAL,
        CLOSE_WAIT,
        CLOSED,
        PORTBINDING
    }
}
