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

package com.tgx.zq.z.queen.io.ws.protocol.bean.mq;

import java.util.Arrays;

import com.tgx.zq.z.queen.base.util.IoUtil;
import com.tgx.zq.z.queen.io.ws.protocol.Command;
import com.tgx.zq.z.queen.io.ws.protocol.WsContext;

public class X82_MqPullResult
        extends
        Command<WsContext>
{
    public final static int COMMAND = 0x82;
    public byte[]           msgDetails;         // {long msgUID, int payloadLength, byte[] payload}为一组

    public X82_MqPullResult() {
        super(COMMAND, false);
    }

    @Override
    public void dispose() {
        super.dispose();
    }

    public byte[] getMsgDetails() {
        return msgDetails;
    }

    public void setMsgDetails(byte[] msgDetails) {
        IoUtil.write(msgDetails, 0, this.msgDetails, 0, this.msgDetails.length);
    }

    @Override
    public int decodec(byte[] data, int pos) {
        pos = IoUtil.read(data, pos, msgDetails, 0, msgDetails.length);
        return pos;
    }

    @Override
    public int encodec(byte[] data, int pos) {
        pos += IoUtil.write(msgDetails, 0, data, pos, msgDetails.length);
        return pos;
    }

    @Override
    public int dataLength() {
        return msgDetails.length;
    }

    @Override
    public int getPriority() {
        return QOS_12_PUSH_MESSAGE;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(super.toString());
        sb.append("msgDetails: ").append(Arrays.toString(msgDetails)).append(CRLF);
        return sb.toString();
    }
}
