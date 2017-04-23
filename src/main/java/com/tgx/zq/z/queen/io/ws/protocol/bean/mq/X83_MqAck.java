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
import com.tgx.zq.z.queen.io.inf.IQoS;
import com.tgx.zq.z.queen.io.ws.protocol.Command;
import com.tgx.zq.z.queen.io.ws.protocol.WsContext;

public class X83_MqAck
        extends
        Command<WsContext>
{
    public final static int COMMAND = 0x83;
    public long             deviceIdx;
    public long             startMsgUID;             // 包含本身
    public long             endMsgUID;                     // 包含本身
    public byte[]           msgUIDs;                             // 9byte一组:0-8byte为msgUID,9byte为status
    private int             mMsgUIDsLength;

    public X83_MqAck() {
        super(COMMAND, false);
    }

    public static byte[] long2Bytes(long num) {
        byte[] byteNum = new byte[8];
        for (int ix = 0; ix < 8; ++ix) {
            int offset = 64 - (ix + 1) * 8;
            byteNum[ix] = (byte) ((num >> offset) & 0xff);
        }
        return byteNum;
    }

    @Override
    public void dispose() {
        super.dispose();
    }

    public void addMsgUIDs(long msgUID, byte status) {
        if (msgUID != 0) {
            if (msgUIDs == null) msgUIDs = new byte[9];
            else {
                byte[] old = msgUIDs;
                msgUIDs = new byte[mMsgUIDsLength + 9];

                byte[] msgUIDBytes = long2Bytes(msgUID);
                msgUIDs[mMsgUIDsLength + 0] = msgUIDBytes[0];
                msgUIDs[mMsgUIDsLength + 1] = msgUIDBytes[1];
                msgUIDs[mMsgUIDsLength + 2] = msgUIDBytes[2];
                msgUIDs[mMsgUIDsLength + 3] = msgUIDBytes[3];
                msgUIDs[mMsgUIDsLength + 4] = msgUIDBytes[4];
                msgUIDs[mMsgUIDsLength + 5] = msgUIDBytes[5];
                msgUIDs[mMsgUIDsLength + 6] = msgUIDBytes[6];
                msgUIDs[mMsgUIDsLength + 7] = msgUIDBytes[7];
                msgUIDs[mMsgUIDsLength + 8] = status;
                System.arraycopy(old, 0, msgUIDs, 0, mMsgUIDsLength);
            }
            mMsgUIDsLength = msgUIDs.length;
        }
    }

    @Override
    public int decodec(byte[] data, int pos) {
        deviceIdx = IoUtil.readLong(data, pos);
        pos += 8;
        mMsgUIDsLength = IoUtil.readUnsignedShort(data, pos);
        pos += 2;
        if (mMsgUIDsLength > 0) {
            startMsgUID = IoUtil.readLong(data, pos);
            pos += 8;
            endMsgUID = IoUtil.readLong(data, pos);
            pos += 8;
            msgUIDs = new byte[mMsgUIDsLength * 9];
            return IoUtil.read(data, pos, msgUIDs);
        }
        return pos;
    }

    @Override
    public int encodec(byte[] data, int pos) {
        pos += IoUtil.writeLong(deviceIdx, data, pos);
        pos += IoUtil.writeShort(mMsgUIDsLength, data, pos);
        if (mMsgUIDsLength > 0) {
            pos += IoUtil.writeLong(startMsgUID, data, pos);
            pos += IoUtil.writeLong(endMsgUID, data, pos);
            pos += IoUtil.write(msgUIDs, data, pos);
        }
        return pos;
    }

    @Override
    public int dataLength() {
        return 8 + 2 + (mMsgUIDsLength > 0 ? mMsgUIDsLength * 9 + 16 : 0);
    }

    @Override
    public int getPriority() {
        return IQoS.QOS_12_PUSH_MESSAGE;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(super.toString());
        sb.append("device register index: ")
          .append(deviceIdx)
          .append(CRLF_TAB)
          .append("message count: ")
          .append(mMsgUIDsLength)
          .append(CRLF_TAB)
          .append("start msgUID: ")
          .append(startMsgUID)
          .append(CRLF_TAB)
          .append("end msgUID: ")
          .append(endMsgUID)
          .append(CRLF_TAB)
          .append(Arrays.toString(msgUIDs))
          .append(CRLF);
        return sb.toString();
    }
}
