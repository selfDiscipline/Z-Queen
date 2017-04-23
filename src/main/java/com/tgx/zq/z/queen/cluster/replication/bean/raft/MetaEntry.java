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

public class MetaEntry
        implements
        IDisposable,
        IDbStorageProtocol
{
    public long nodeId;
    public long termId                 = -1L;
    public long ballotId;
    public long slotIndex              = -1L;
    public long lastCommittedSlotIndex = -1L;
    public long lastCommittedTermId    = -1L;
    public long lastClientSlotIndex    = -1L;

    public MetaEntry(long nodeId,
                     long termId,
                     long slotIndex,
                     long ballotId,
                     long lastCommittedTermId,
                     long lastCommittedSlotIndex,
                     long lastClientSlotIndex) {
        this.nodeId = nodeId;
        this.ballotId = ballotId;
        this.termId = termId;
        this.slotIndex = slotIndex;
        this.lastCommittedTermId = lastCommittedTermId;
        this.lastCommittedSlotIndex = lastCommittedSlotIndex;
        this.lastClientSlotIndex = lastClientSlotIndex;
    }

    public MetaEntry(long nodeId) {
        this.nodeId = nodeId;
    }

    public MetaEntry(long nodeId, long termId) {
        this.nodeId = nodeId;
        this.termId = termId;
    }

    public MetaEntry(long nodeId, long ballotId, long clientSlotIndex) {
        this.nodeId = nodeId;
        this.ballotId = ballotId;
        this.lastClientSlotIndex = clientSlotIndex;
    }

    private MetaEntry() {
    }

    @Override
    public int dataLength() {
        return 56;
    }

    @Override
    public Operation getOperation() {
        return Operation.OP_MODIFY;
    }

    @Override
    public void setOperation(Operation op) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getPrimaryKey() {
        return nodeId;
    }

    @Override
    public byte[] getSecondaryByteArrayKey() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getSecondaryLongKey() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int decodec(byte[] data, int offset) {
        nodeId = IoUtil.readLong(data, offset);
        offset += 8;
        ballotId = IoUtil.readLong(data, offset);
        offset += 8;
        termId = IoUtil.readLong(data, offset);
        offset += 8;
        slotIndex = IoUtil.readLong(data, offset);
        offset += 8;
        lastCommittedTermId = IoUtil.readLong(data, offset);
        offset += 8;
        lastCommittedSlotIndex = IoUtil.readLong(data, offset);
        offset += 8;
        lastClientSlotIndex = IoUtil.readLong(data, offset);
        offset += 8;
        return offset;
    }

    @Override
    public int encodec(byte[] data, int offset) {
        offset += IoUtil.writeLong(nodeId, data, offset);
        offset += IoUtil.writeLong(ballotId, data, offset);
        offset += IoUtil.writeLong(termId, data, offset);
        offset += IoUtil.writeLong(slotIndex, data, offset);
        offset += IoUtil.writeLong(lastCommittedTermId, data, offset);
        offset += IoUtil.writeLong(lastCommittedSlotIndex, data, offset);
        offset += IoUtil.writeLong(lastClientSlotIndex, data, offset);
        return offset;
    }

    public static class MetaEntryBinding
            implements
            EntryBinding<MetaEntry>
    {

        @Override
        public MetaEntry entryToObject(DatabaseEntry entry) {
            MetaEntry me = new MetaEntry();
            me.decode(entry.getData());
            return me;
        }

        @Override
        public void objectToEntry(MetaEntry object, DatabaseEntry entry) {
            entry.setData(object.encode());
        }

    }
}
