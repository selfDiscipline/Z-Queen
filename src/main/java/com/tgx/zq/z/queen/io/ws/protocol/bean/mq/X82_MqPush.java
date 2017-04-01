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

package com.tgx.zq.z.queen.io.ws.protocol.bean.mq;

import com.tgx.zq.z.queen.io.inf.ISerialProtocol;

public class X82_MqPush
        extends
        X8X_MqExchange
{
    public final static int           COMMAND = 0x82;
    private byte[]                    mPayload;
    private transient ISerialProtocol mSProtocol;

    public X82_MqPush() {
        super(COMMAND);
    }

    @Override
    public void dispose() {
        super.dispose();
    }

    @Override
    public int decodec(byte[] data, int pos) {

        return pos;
    }

    @Override
    public int encodec(byte[] data, int pos) {

        return pos;
    }

    @Override
    public int dataLength() {
        return mPayload == null ? 0 : mPayload.length;
    }

    @Override
    public int getPriority() {
        return QOS_03_CLUSTER_EXCHANGE;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(super.toString());
        sb.append(CRLFTAB).append("payload-> ").append(mSProtocol.toString()).append(CRLF);
        return sb.toString();
    }
}
