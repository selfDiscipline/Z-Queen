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

import com.tgx.zq.z.queen.base.util.IoUtil;
import com.tgx.zq.z.queen.io.ws.protocol.Command;
import com.tgx.zq.z.queen.io.ws.protocol.WsContext;
import com.tgx.zq.z.queen.io.ws.protocol.WsFrame;

/**
 * @author William.d.zk
 */
public abstract class X8X_MqExchange
        extends
        Command<WsContext>
{
    public final static int SerialNum = 0x8F;
    public int              channel;

    public X8X_MqExchange(int command) {
        super(command, false);
    }

    @Override
    public int getSuperSerialNum() {
        return SerialNum;
    }

    @Override
    public int decodec(byte[] data, int pos) {
        channel = IoUtil.readInt(data, pos);
        pos += 4;
        return pos;
    }

    @Override
    public byte getControl() {
        return WsFrame.frame_op_code_no_ctrl_zq;
    }

    @Override
    public int encodec(byte[] data, int pos) {
        pos += IoUtil.writeInt(channel, data, pos);
        return pos;
    }

    @Override
    public int dataLength() {
        return super.dataLength() + 4;
    }

    @Override
    public String toString() {
        return new StringBuilder(super.toString()).append("channel:").append(channel).append(CRLF_TAB).toString();
    }
}
