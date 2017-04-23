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

package com.tgx.zq.z.queen.cluster.replication.bean.raft;

import com.sleepycat.bind.EntryBinding;
import com.sleepycat.je.DatabaseEntry;
import com.tgx.zq.z.queen.base.inf.IDisposable;
import com.tgx.zq.z.queen.base.util.IoUtil;
import com.tgx.zq.z.queen.db.bdb.inf.IDbStorageProtocol;

public class SnapShotEntry<E extends IDbStorageProtocol>
        implements
        IDisposable,
        IDbStorageProtocol
{

    public long    slotIndex, termId;
    private byte[] mStorePayload;
    private int    mEntityCount;
    private int    mPayloadLength;

    private SnapShotEntry() {
    }

    public SnapShotEntry(long termId, long slotIndex, LogEntry<E>[] entries) {
        this.termId = termId;
        this.slotIndex = slotIndex;
        setPayload(entries);
    }

    public byte[][] getPayload() {
        if (mEntityCount > 0) {
            byte[][] result = new byte[mEntityCount][];
            for (int i = 0, pos = 0, len; i < mEntityCount; i++) {
                len = IoUtil.readInt(mStorePayload, pos);
                result[i] = new byte[len];
                pos = IoUtil.read(mStorePayload, pos, result[i], 0, len);
            }
            return result;
        }
        return null;
    }

    public void setPayload(LogEntry<E>[] logEntities) {
        mEntityCount = logEntities == null ? 0 : logEntities.length;
        if (mEntityCount > 0) {
            for (LogEntry<E> entity : logEntities)
                mPayloadLength += entity.payloadLength() + 4;
            mStorePayload = new byte[mPayloadLength];
            for (int i = 0, pos = 0; i < mEntityCount; i++) {
                LogEntry<E> entity = logEntities[i];
                pos += IoUtil.writeInt(entity.payloadLength(), mStorePayload, pos);
                pos += IoUtil.write(entity.getPayloadAsByteArray(), 0, mStorePayload, pos, entity.payloadLength());
            }
        }
    }

    @Override

    public int dataLength() {
        return 24 + (mPayloadLength = mStorePayload == null ? 0 : mStorePayload.length);
    }

    @Override
    public int decodec(byte[] data, int offset) {
        termId = IoUtil.readLong(data, offset);
        offset += 8;
        slotIndex = IoUtil.readLong(data, offset);
        offset += 8;
        mEntityCount = IoUtil.readInt(data, offset);
        offset += 4;
        mPayloadLength = IoUtil.readInt(data, offset);
        offset += 4;
        mStorePayload = new byte[mPayloadLength];
        offset = IoUtil.read(data, offset, mStorePayload, 0, mPayloadLength);
        return offset;
    }

    @Override
    public int encodec(byte[] data, int offset) {
        offset += IoUtil.writeLong(termId, data, offset);
        offset += IoUtil.writeLong(slotIndex, data, offset);
        offset += IoUtil.writeInt(mEntityCount, data, offset);
        offset += IoUtil.writeInt(mPayloadLength, data, offset);
        offset += IoUtil.write(mStorePayload, 0, data, offset, mPayloadLength);
        return offset;
    }

    public static class SnapShotEntryBinding<E extends IDbStorageProtocol>
            implements
            EntryBinding<SnapShotEntry<E>>
    {

        @Override
        public SnapShotEntry<E> entryToObject(DatabaseEntry entry) {
            SnapShotEntry<E> sse = new SnapShotEntry<>();
            sse.decode(entry.getData());
            return sse;
        }

        @Override
        public void objectToEntry(SnapShotEntry<E> object, DatabaseEntry entry) {
            entry.setData(object.encode());
        }

    }
}
