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

package com.tgx.zq.z.queen.cluster.replication.bean.raft;

import com.sleepycat.bind.EntryBinding;
import com.sleepycat.je.DatabaseEntry;
import com.tgx.zq.z.queen.base.inf.IDisposable;
import com.tgx.zq.z.queen.base.util.IoUtil;
import com.tgx.zq.z.queen.cluster.replication.inf.ILogEntry;
import com.tgx.zq.z.queen.db.bdb.inf.IDbStorageProtocol;

public class LogEntry<E extends IDbStorageProtocol>
        implements
        IDisposable,
        Comparable<LogEntry<E>>,
        ILogEntry<E>
{

    public long      termId;
    public long      slotIndex;
    public long      idempotent;
    public Status    status;
    public Operation operation;
    private byte     mStatus;
    private byte     mOperation;
    private byte[]   mPayload;
    private E        mEntity;
    private long[]   mOldConfig, mNewConfig;

    public LogEntry(long termId, long slotIndex, long idempotent) {
        this.slotIndex = slotIndex;
        this.termId = termId;
        this.idempotent = idempotent;
        status = Status.OP_CLIENT;
        operation = Operation.OP_INSERT;
    }

    public LogEntry(long termId, long slotIndex) {
        this(termId, slotIndex, 0);
    }

    public LogEntry() {
        status = Status.OP_NULL;
        operation = Operation.OP_NULL;
    }

    @Override
    public int dataLength() {
        return 32 + (mPayload == null ? 0 : mPayload.length);
    }

    public int payloadLength() {
        return mPayload == null ? 0 : mPayload.length;
    }

    @Override
    public int decodec(byte[] data, int offset) {
        termId = IoUtil.readLong(data, offset);
        offset += 8;
        slotIndex = IoUtil.readLong(data, offset);
        offset += 8;
        idempotent = IoUtil.readLong(data, offset);
        offset += 8;
        mStatus = data[offset++];
        switch (mStatus) {
            case 1:
                status = Status.OP_CLIENT;
                break;
            case 2:
                status = Status.OP_UNCOMMITTED;
                break;
            case 3:
                status = Status.OP_COMMITTED;
                break;
            case 4:
                status = Status.OP_UNCOMMITTED_JOINT_CONSENSUS;
                break;
            case 5:
                status = Status.OP_COMMITTED_NEW_CONFIG;
                break;
            default:
                status = Status.OP_NULL;
                break;
        }
        mOperation = data[offset++];
        getOperation();
        int oldConfigLength = IoUtil.readUnsignedByte(data, offset);
        if (oldConfigLength > 0) {
            mOldConfig = new long[oldConfigLength];
            offset = IoUtil.readLongArray(data, offset, mOldConfig);
        }
        int newConfigLength = IoUtil.readUnsignedByte(data, offset);
        if (newConfigLength > 0) {
            mNewConfig = new long[newConfigLength];
            offset = IoUtil.readLongArray(data, offset, mNewConfig);
        }
        int payloadLength = IoUtil.readInt(data, offset);
        offset += 4;
        if (payloadLength > 0) {
            mPayload = new byte[payloadLength];
            offset = IoUtil.read(data, offset, mPayload, 0, payloadLength);
        }
        return offset;
    }

    @Override
    public int encodec(byte[] data, int offset) {
        offset += IoUtil.writeLong(termId, data, offset);
        offset += IoUtil.writeLong(slotIndex, data, offset);
        offset += IoUtil.writeLong(idempotent, data, offset);
        offset += IoUtil.writeByte(status.ordinal(), data, offset);
        offset += IoUtil.writeByte(operation.ordinal(), data, offset);
        int oldConfigLength = mOldConfig == null ? 0 : mOldConfig.length;
        offset += IoUtil.writeByte(oldConfigLength, data, offset);
        offset += IoUtil.writeLongArray(mOldConfig, data, offset);
        int newConfigLength = mNewConfig == null ? 0 : mNewConfig.length;
        offset += IoUtil.writeByte(newConfigLength, data, offset);
        offset += IoUtil.writeLongArray(mNewConfig, data, offset);
        int payloadLength = (mPayload == null) ? 0 : mPayload.length;
        offset += IoUtil.writeInt(payloadLength, data, offset);
        offset += IoUtil.write(mPayload, 0, data, offset, payloadLength);
        return offset;
    }

    @Override
    public long getSecondaryLongKey() {
        return idempotent;
    }

    @Override
    public Status getStatus() {
        return status;
    }

    @Override
    public long getTermId() {
        return termId;
    }

    @Override
    public long getSlotIndex() {
        return slotIndex;
    }

    @Override
    public void append() {
        status = Status.OP_UNCOMMITTED;
    }

    @Override
    public void commit() {
        status = status.equals(Status.OP_UNCOMMITTED_JOINT_CONSENSUS) ? Status.OP_COMMITTED_NEW_CONFIG
                                                                      : status.equals(Status.OP_UNCOMMITTED) ? Status.OP_COMMITTED : status;
    }

    @Override
    public boolean isCommitted() {
        return status.equals(Status.OP_COMMITTED) || status.equals(Status.OP_COMMITTED_NEW_CONFIG);
    }

    @Override
    public long[] getNewConfig() {
        return mNewConfig;
    }

    @Override
    public long[] getOldConfig() {
        return mOldConfig;
    }

    @Override
    public void joinConsensus(long[] oldConfig, long[] newConfig) {
        status = Status.OP_UNCOMMITTED_JOINT_CONSENSUS;
        mOldConfig = oldConfig;
        mNewConfig = newConfig;
    }

    @Override
    public boolean isJoinConsensus() {
        return mNewConfig != null || mOldConfig != null;
    }

    @Override
    public void commitNewConfig(long[] newConfig) {
        status = Status.OP_COMMITTED_NEW_CONFIG;
        mNewConfig = newConfig;
    }

    @Override
    public int compareTo(LogEntry<E> o) {
        return slotIndex > o.slotIndex ? 1 : slotIndex < o.slotIndex ? -1 : 0;
    }

    @Override
    public byte[] getPayloadAsByteArray() {
        return mPayload;
    }

    @Override
    public void setPayloadAsByteArray(byte[] payload) {
        mPayload = payload;
        mEntity.decode(payload);
    }

    @Override
    public E getPayload() {
        return mEntity;
    }

    @Override
    public void setPayload(E entity) {
        mEntity = entity;
        mPayload = mEntity.encode();
    }

    @Override
    public void dispose() {
        mPayload = null;
        mEntity = null;
        status = null;
        operation = null;
    }

    @Override
    public Operation getOperation() {
        switch (mOperation) {
            case IDbStorageProtocol._OP_INVALID:
                return operation = Operation.OP_INVALID;
            case IDbStorageProtocol._OP_INSERT:
                return operation = Operation.OP_INSERT;
            case IDbStorageProtocol._OP_MODIFY:
                return operation = Operation.OP_MODIFY;
            case IDbStorageProtocol._OP_REMOVE:
                return operation = Operation.OP_REMOVE;
            case IDbStorageProtocol._OP_APPEND:
                return operation = Operation.OP_APPEND;
            case IDbStorageProtocol._OP_DELETE:
                return operation = Operation.OP_DELETE;
            default:
                return operation = Operation.OP_NULL;
        }
    }

    @Override
    public void setOperation(IDbStorageProtocol.Operation op) {
        operation = op;
        mOperation = parseOperation(op);
    }

    @Override
    public String toString() {
        String CRLF = "\r\n";
        String CRLFTAB = "\r\n\t";
        StringBuilder sb = new StringBuilder("LogEntity term id: ").append(termId)
                                                                   .append(CRLFTAB)
                                                                   .append("slot index: ")
                                                                   .append(slotIndex)
                                                                   .append(CRLFTAB)
                                                                   .append("status: ")
                                                                   .append(status.name())
                                                                   .append(CRLFTAB)
                                                                   .append(mPayload != null ? IoUtil.bin2Hex(mPayload) : "no data")
                                                                   .append(CRLFTAB)
                                                                   .append(mEntity != null ? mEntity.toString() : "no entity")
                                                                   .append(CRLF);
        return sb.toString();
    }

    public static class LogEntryBinding<T extends IDbStorageProtocol>
            implements
            EntryBinding<LogEntry<T>>
    {

        @Override
        public LogEntry<T> entryToObject(DatabaseEntry entry) {
            LogEntry<T> le = new LogEntry<>();
            le.decode(entry.getData());
            return le;
        }

        @Override
        public void objectToEntry(LogEntry<T> object, DatabaseEntry entry) {
            entry.setData(object.encode());
        }

    }
}
