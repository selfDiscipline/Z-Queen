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

import com.tgx.zq.z.queen.base.inf.ISerialTick;
import com.tgx.zq.z.queen.base.util.IoUtil;
import com.tgx.zq.z.queen.cluster.replication.bean.raft.LogEntry;

public class X14_RSyncEntry
        extends
        X1X_ClusterExchange
{
    public final static int COMMAND = 0x14;
    public long             lastCommittedSlotIndex;
    private int             mPayloadLength;
    private byte[]          mPayload;

    public X14_RSyncEntry() {
        super(COMMAND);
    }

    public X14_RSyncEntry(long _uid, long leaderId, long termId, long slotIndex, long lastCommittedSlotIndex) {
        super(COMMAND, _uid, leaderId, termId, slotIndex);
        this.lastCommittedSlotIndex = lastCommittedSlotIndex;
    }

    public LogEntry<?> getEntry() {
        if (mPayloadLength == 0) return null;
        LogEntry<?> entry = new LogEntry<>();
        entry.decode(mPayload, 0, mPayloadLength);
        return entry;
    }

    public X14_RSyncEntry setEntry(LogEntry<?> logEntry) {
        if (logEntry != null) {
            mPayloadLength += logEntry.dataLength();
            mPayload = new byte[mPayloadLength];
            int pos = 0;
            pos += IoUtil.writeInt(logEntry.dataLength(), mPayload, pos);
            IoUtil.write(logEntry.encode(), 0, mPayload, pos, logEntry.dataLength());
        }
        return this;
    }

    @Override
    public int dataLength() {
        return super.dataLength() + 12 + mPayloadLength;
    }

    @Override
    public int encodec(byte[] data, int pos) {
        pos += IoUtil.writeLong(lastCommittedSlotIndex, data, pos);
        pos += IoUtil.writeInt(mPayloadLength, data, pos);
        if (mPayloadLength > 0) pos += IoUtil.write(mPayload, 0, data, pos, mPayloadLength);
        return super.encodec(data, pos);
    }

    @Override
    public int decodec(byte[] data, int pos) {
        lastCommittedSlotIndex = IoUtil.readLong(data, pos);
        pos += 8;
        mPayloadLength = IoUtil.readInt(data, pos);
        pos += 4;
        if (mPayloadLength > 0) {
            mPayload = new byte[mPayloadLength];
            pos = IoUtil.read(data, pos, mPayload, 0, mPayloadLength);
        }
        return super.decodec(data, pos);
    }

    @Override
    public String toString() {
        return super.toString()
               + CRLF_TAB
               + "Last committed slot index: "
               + lastCommittedSlotIndex
               + CRLF_TAB
               + "LogEntry: "
               + getEntry().toString()
               + CRLF;
    }

    public byte[] getPayload() {
        return mPayload;
    }

    public void setPayload(byte[] payload) {
        this.mPayload = payload;
        mPayloadLength = payload == null ? 0 : payload.length;
    }
}
