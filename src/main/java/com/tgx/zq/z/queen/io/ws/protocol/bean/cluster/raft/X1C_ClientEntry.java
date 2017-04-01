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

package com.tgx.zq.z.queen.io.ws.protocol.bean.cluster.raft;

import com.tgx.zq.z.queen.base.util.IoUtil;
import com.tgx.zq.z.queen.cluster.replication.bean.raft.LogEntry;

public class X1C_ClientEntry
        extends
        X1X_ClusterExchange
{
    public final static int COMMAND = 0x1C;
    private int             mPayloadLength;
    private byte[]          mPayload;

    public X1C_ClientEntry(long mMsgUID, long nodeId, long termId, long slotIndex) {
        super(COMMAND, mMsgUID, nodeId, termId, slotIndex);
    }

    public X1C_ClientEntry() {
        super(COMMAND);
    }

    @Override
    public int getSerialNum() {
        return COMMAND;
    }

    @Override
    public int dataLength() {
        return super.dataLength() + 2 + (mPayload == null ? 0 : mPayload.length);
    }

    @Override
    public int decodec(byte[] data, int pos) {
        mPayloadLength = IoUtil.readUnsignedShort(data, pos);
        pos += 2;
        if (mPayloadLength > 0) {
            mPayload = new byte[mPayloadLength];
            pos = IoUtil.read(data, pos, mPayload, 0, mPayloadLength);
        }
        return super.decodec(data, pos);
    }

    @Override
    public int encodec(byte[] data, int pos) {
        pos += IoUtil.writeShort(mPayloadLength, data, pos);
        if (mPayloadLength > 0) pos += IoUtil.write(mPayload, 0, data, pos, mPayloadLength);
        return super.encodec(data, pos);
    }

    public byte[] getPayload() {
        return mPayload;
    }

    public void setPayload(LogEntry<?> entry) {
        mPayloadLength = entry.payloadLength();
        mPayload = entry.getPayloadAsByteArray();
    }

    @Override
    public void dispose() {
        mPayload = null;
        mPayloadLength = 0;
    }

    @Override
    public boolean isOrder() {
        return false;
    }
}
