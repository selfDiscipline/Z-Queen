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
package com.tgx.zq.z.queen.io.ws.protocol.bean.tls;

import com.tgx.zq.z.queen.base.util.IoUtil;
import com.tgx.zq.z.queen.io.ws.protocol.Command;
import com.tgx.zq.z.queen.io.ws.protocol.WsContext;

/**
 * @author William.d.zk
 */
public class X01_EncryptRequest
        extends
        Command<WsContext>
{
    public final static int COMMAND  = 0x01;
    public int              pubKeyId = -1;

    public X01_EncryptRequest() {
        super(COMMAND, false);
    }

    @Override
    public int decodec(byte[] data, int pos) {
        if (isEncrypt()) {
            pubKeyId = IoUtil.readInt(data, pos);
            pos += 4;
        }
        return pos;
    }

    @Override
    public int encodec(byte[] data, int pos) {
        if (isEncrypt()) pos += IoUtil.writeInt(pubKeyId, data, pos);
        return pos;
    }

    @Override
    public int dataLength() {
        return super.dataLength() + (isEncrypt() ? 4 : 0);
    }

    @Override
    public int getPriority() {
        return QOS_00_NETWORK_CONTROL;
    }

    @Override
    public String toString() {
        return super.toString() + "public-key-id:" + pubKeyId + CRLF_TAB;
    }
}
