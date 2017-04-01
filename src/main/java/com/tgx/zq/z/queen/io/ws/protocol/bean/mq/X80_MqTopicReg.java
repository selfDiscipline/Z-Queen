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

import com.tgx.zq.z.queen.base.util.ArrayUtil;
import com.tgx.zq.z.queen.base.util.IoUtil;

public class X80_MqTopicReg
        extends
        X8X_MqExchange
{

    public final static int COMMAND = 0x80;
    public long             nodeId;
    public short[]          topicKeys;

    public X80_MqTopicReg() {
        super(COMMAND);
    }

    @Override
    public void dispose() {
        super.dispose();
    }

    @Override
    public int decodec(byte[] data, int pos) {
        pos = super.decodec(data, pos);
        nodeId = IoUtil.readLong(data, pos);
        pos += 8;
        int keyLength = data[pos++] & 0xFF;
        topicKeys = new short[keyLength];
        pos = IoUtil.readShortArray(data, pos, topicKeys);
        return pos;
    }

    @Override
    public int encodec(byte[] data, int pos) {
        pos += IoUtil.writeLong(nodeId, data, pos);
        pos += IoUtil.writeByte(topicKeys == null ? 0 : topicKeys.length, data, pos);
        if (topicKeys != null) pos += IoUtil.writeShortArray(topicKeys, data, pos);
        return pos;
    }

    @Override
    public int dataLength() {
        return super.dataLength() + 9 + (topicKeys == null ? 0 : topicKeys.length << 1);
    }

    @Override
    public int getPriority() {
        return QOS_02_MQ_CONTROL;
    }

    @Override
    public String toString() {
        return new StringBuilder(super.toString()).append("MQ-Client:")
                                                  .append(nodeId)
                                                  .append(CRLFTAB)
                                                  .append("topic:")
                                                  .append(ArrayUtil.toHexString(topicKeys))
                                                  .append(CRLF)
                                                  .toString();
    }
}
