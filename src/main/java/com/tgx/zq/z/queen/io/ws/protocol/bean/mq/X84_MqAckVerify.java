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

import java.util.Arrays;

import com.tgx.zq.z.queen.base.util.IoUtil;
import com.tgx.zq.z.queen.io.ws.protocol.Command;
import com.tgx.zq.z.queen.io.ws.protocol.WsContext;

public class X84_MqAckVerify
        extends
        Command<WsContext>
{
    public final static int COMMAND = 0x84;
    public long             deviceIdx;
    public int              succeed;                                          // ack是否成功,0:成功,1:失败
    public long[]           ackMsgUIDs;                              // 确认的msgUIDs
    public long[]           gapMsgUIDs;                              // 空缺的msgUIDs
    private int             mAckMsgUIDsLength;
    private int             mGapMsgUIDsLength;

    public X84_MqAckVerify() {
        super(COMMAND, false);
    }

    @Override
    public void dispose() {
        super.dispose();
    }

    public void setAckMsgUIDs(long[] msgUIDs) {
        mAckMsgUIDsLength = msgUIDs == null ? 0 : msgUIDs.length;
        this.ackMsgUIDs = msgUIDs;
    }

    public void addAckMsgUIDs(long msgUID) {
        if (msgUID != 0) {
            if (ackMsgUIDs == null) ackMsgUIDs = new long[] { msgUID };
            else {
                long[] old = ackMsgUIDs;
                ackMsgUIDs = new long[mAckMsgUIDsLength + 1];
                ackMsgUIDs[mAckMsgUIDsLength] = msgUID;
                System.arraycopy(old, 0, ackMsgUIDs, 0, mAckMsgUIDsLength);
            }
            mAckMsgUIDsLength = ackMsgUIDs.length;
        }
    }

    public void setGapMsgUIDs(long[] msgUIDs) {
        mGapMsgUIDsLength = msgUIDs == null ? 0 : msgUIDs.length;
        this.gapMsgUIDs = msgUIDs;
    }

    public void addGapMsgUIDs(long msgUID) {
        if (msgUID != 0) {
            if (gapMsgUIDs == null) gapMsgUIDs = new long[] { msgUID };
            else {
                long[] old = gapMsgUIDs;
                gapMsgUIDs = new long[mGapMsgUIDsLength + 1];
                gapMsgUIDs[mGapMsgUIDsLength] = msgUID;
                System.arraycopy(old, 0, gapMsgUIDs, 0, mGapMsgUIDsLength);
            }
            mGapMsgUIDsLength = gapMsgUIDs.length;
        }
    }

    @Override
    public int decodec(byte[] data, int pos) {
        deviceIdx = IoUtil.readLong(data, pos);
        pos += 8;
        succeed = IoUtil.readInt(data, pos);
        pos += 2;
        mAckMsgUIDsLength = IoUtil.readUnsignedShort(data, pos);
        pos += 2;
        mGapMsgUIDsLength = IoUtil.readUnsignedShort(data, pos);
        pos += 2;
        if (mAckMsgUIDsLength > 0) {
            ackMsgUIDs = new long[mAckMsgUIDsLength];
            IoUtil.readLongArray(data, pos, ackMsgUIDs);
        }
        if (mGapMsgUIDsLength > 0) {
            gapMsgUIDs = new long[mGapMsgUIDsLength];
            IoUtil.readLongArray(data, pos, gapMsgUIDs);
        }
        return pos;
    }

    @Override
    public int encodec(byte[] data, int pos) {
        pos += IoUtil.writeLong(deviceIdx, data, pos);
        pos += IoUtil.writeShort(succeed, data, pos);
        pos += IoUtil.writeShort(mAckMsgUIDsLength, data, pos);
        pos += IoUtil.writeShort(mGapMsgUIDsLength, data, pos);
        if (mAckMsgUIDsLength > 0) {
            pos += IoUtil.writeLongArray(ackMsgUIDs, data, pos);
        }
        if (mGapMsgUIDsLength > 0) {
            pos += IoUtil.writeLongArray(gapMsgUIDs, data, pos);
        }
        return pos;
    }

    @Override
    public int dataLength() {
        return 4 * 3 + (1 + mAckMsgUIDsLength + mGapMsgUIDsLength) << 3;
    }

    @Override
    public int getPriority() {
        return QOS_12_PUSH_MESSAGE;
    }

    @Override
    public String toString() {
        return new StringBuilder(super.toString()).append("deviceIdx:")
                                                  .append(deviceIdx)
                                                  .append(CRLFTAB)
                                                  .append("succeed:")
                                                  .append(succeed)
                                                  .append(CRLFTAB)
                                                  .append("mAckMsgUIDsLength:")
                                                  .append(mAckMsgUIDsLength)
                                                  .append(CRLFTAB)
                                                  .append("mGapMsgUIDsLength:")
                                                  .append(mGapMsgUIDsLength)
                                                  .append(CRLFTAB)
                                                  .append("ackMsgUIDs:")
                                                  .append(Arrays.toString(ackMsgUIDs))
                                                  .append(CRLFTAB)
                                                  .append("gapMsgUIDs:")
                                                  .append(Arrays.toString(gapMsgUIDs))
                                                  .append(CRLF)
                                                  .toString();
    }
}
