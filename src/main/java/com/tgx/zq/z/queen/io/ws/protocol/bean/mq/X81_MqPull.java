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

import com.tgx.zq.z.queen.base.inf.ISerialTick;
import com.tgx.zq.z.queen.base.util.IoUtil;
import com.tgx.zq.z.queen.io.inf.IQoS;
import com.tgx.zq.z.queen.io.ws.protocol.Command;
import com.tgx.zq.z.queen.io.ws.protocol.WsContext;

/**
 * @author William.d.zk
 */
public class X81_MqPull
        extends
        Command<WsContext>
{
    public final static int COMMAND = 0x81;
    public long             original;
    public long             target;
    public long             thread;
    public long             deviceIdx;
    private byte[]          payload = new byte[1024];

    public X81_MqPull() {
        super(COMMAND, false);
    }

    @Override
    public void dispose() {
        super.dispose();
    }

    public byte[] getPayload() {
        return payload;
    }

    public void setPayload(byte[] payload) {
        IoUtil.write(payload, 0, this.payload, 0, this.payload.length);
    }

    @Override
    public int decodec(byte[] data, int pos) {
        original = IoUtil.readLong(data, pos);
        pos += 8;
        target = IoUtil.readLong(data, pos);
        pos += 8;
        thread = IoUtil.readLong(data, pos);
        pos += 8;
        deviceIdx = IoUtil.readLong(data, pos);
        pos += 8;
        pos = IoUtil.read(data, pos, payload, 0, payload.length);
        return pos;
    }

    @Override
    public int encodec(byte[] data, int pos) {
        pos += IoUtil.writeLong(original, data, pos);
        pos += IoUtil.writeLong(target, data, pos);
        pos += IoUtil.writeLong(thread, data, pos);
        pos += IoUtil.writeLong(deviceIdx, data, pos);
        pos += IoUtil.write(payload, 0, data, pos, payload.length);
        return pos;
    }

    @Override
    public int dataLength() {
        return 32 + payload.length;
    }

    @Override
    public int getPriority() {
        return IQoS.QOS_12_PUSH_MESSAGE;
    }

    @Override
    public String toString() {
        return new StringBuilder(super.toString()).append("original:")
                                                  .append(original)
                                                  .append(ISerialTick.CRLF_TAB)
                                                  .append("target:")
                                                  .append(target)
                                                  .append(ISerialTick.CRLF_TAB)
                                                  .append("thread:")
                                                  .append(thread)
                                                  .append(ISerialTick.CRLF_TAB)
                                                  .append("deviceIdx:")
                                                  .append(Long.toHexString(deviceIdx).toUpperCase())
                                                  .append(ISerialTick.CRLF_TAB)
                                                  .append("payload:")
                                                  .append(IoUtil.bin2Hex(getPayload()))
                                                  .append(ISerialTick.CRLF)
                                                  .toString();
    }
}
