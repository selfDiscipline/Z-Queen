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

import com.tgx.zq.z.queen.base.constant.QueenCode;
import com.tgx.zq.z.queen.base.util.IoUtil;
import com.tgx.zq.z.queen.io.ws.protocol.Command;
import com.tgx.zq.z.queen.io.ws.protocol.ZContext;

/**
 * @author William.d.zk
 */
public class X04_EncryptConfirm
        extends
        Command<ZContext>
{
    public final static int COMMAND = 0x04;
    public int              response;
    public int              symmetricKeyId;
    /* SHA256 */
    private byte[]          mSign;

    public X04_EncryptConfirm() {
        super(COMMAND, false);
    }

    public byte[] getSign() {
        return mSign;
    }

    public void setSign(byte[] sign) {
        mSign = sign;
    }

    @Override
    public int dataLength() {
        return super.dataLength() + 36;
    }

    @Override
    public int decodec(byte[] data, int pos) {
        response = IoUtil.readShort(data, pos);
        pos += 2;
        symmetricKeyId = IoUtil.readUnsignedShort(data, pos);
        pos += 2;
        mSign = new byte[32];
        pos = IoUtil.read(data, pos, mSign);
        return pos;
    }

    @Override
    public int encodec(byte[] data, int pos) {
        pos += IoUtil.writeShort(response, data, pos);
        pos += IoUtil.writeShort(symmetricKeyId, data, pos);
        pos += IoUtil.write(mSign, data, pos);
        return pos;
    }

    @Override
    public void afterEncode(ZContext ctx) {
        ctx.updateKeyOut();
    }

    @Override
    public void afterDecode(ZContext ctx) {
        ctx.updateKeyIn();
    }

    @Override
    public int getPriority() {
        return QOS_00_NETWORK_CONTROL;
    }

    @Override
    public String toString() {
        return super.toString()
               + "code: "
               + QueenCode.parseRCode(response)
               + CRLF_TAB
               + "rc4key-id: "
               + symmetricKeyId
               + CRLF_TAB
               + "sign: "
               + IoUtil.bin2Hex(mSign)
               + CRLF;
    }
}
