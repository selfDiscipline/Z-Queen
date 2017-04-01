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

public class X15_JointConsensus
        extends
        X1X_ClusterExchange
{
    public final static int COMMAND = 0x15;
    public long             lastCommittedSlotIndex;
    private int             oldNodeCount;
    private int             newNodeCount;
    private long[]          mOldConfig;
    private long[]          mNewConfig;

    public X15_JointConsensus(long _uid, long nodeId, long termId, long slotIndex, long lastCommittedSlotIndex) {
        super(COMMAND, _uid, nodeId, termId, slotIndex);
        this.lastCommittedSlotIndex = lastCommittedSlotIndex;
    }

    public X15_JointConsensus() {
        super(COMMAND);
    }

    @Override
    public int decodec(byte[] data, int pos) {
        oldNodeCount = IoUtil.readUnsignedShort(data, pos);
        pos += 2;
        if (oldNodeCount > 0) mOldConfig = new long[oldNodeCount];
        pos = IoUtil.readLongArray(data, pos, mOldConfig);
        newNodeCount = IoUtil.readUnsignedShort(data, pos);
        pos += 2;
        if (newNodeCount > 0) mNewConfig = new long[newNodeCount];
        pos = IoUtil.readLongArray(data, pos, mNewConfig);
        return super.decodec(data, pos);
    }

    @Override
    public int encodec(byte[] data, int pos) {
        pos += IoUtil.writeShort(oldNodeCount, data, pos);
        if (oldNodeCount > 0) pos += IoUtil.writeLongArray(mOldConfig, data, pos);
        pos += IoUtil.writeShort(newNodeCount, data, pos);
        if (newNodeCount > 0) pos += IoUtil.writeLongArray(mNewConfig, data, pos);
        return super.encodec(data, pos);
    }

    @Override
    public int dataLength() {
        return super.dataLength() + 12 + (oldNodeCount << 3) + (newNodeCount << 3);
    }

    public long getNewConfigCount() {
        return newNodeCount;
    }

    public long[] getNewConfig() {
        return mNewConfig;
    }

    public X15_JointConsensus setNewConfig(long... config) {
        if (config != null && config.length > 0) {
            mNewConfig = config;
            newNodeCount = config.length;
        }
        return this;
    }

    public long getOldConfigCount() {
        return oldNodeCount;
    }

    public long[] getOldConfig() {
        return mOldConfig;
    }

    public X15_JointConsensus setOldConfig(long... config) {
        if (config != null && config.length > 0) {
            mOldConfig = config;
            oldNodeCount = config.length;
        }
        return this;
    }

    @Override
    public X15_JointConsensus duplicate() {
        X15_JointConsensus x15 = new X15_JointConsensus(getUID(), nodeId, termId, slotIndex, lastCommittedSlotIndex);
        x15.setOldConfig(mOldConfig);
        x15.setNewConfig(mNewConfig);
        return x15;
    }

    @Override
    public void dispose() {
        mOldConfig = null;
        mNewConfig = null;
        super.dispose();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(super.toString());
        sb.append("node list old: ").append(oldNodeCount).append(CRLFTAB);
        for (int i = 0; i < oldNodeCount; i++)
            sb.append(TAB).append(" --> ").append(Long.toHexString(mOldConfig[i]).toUpperCase()).append(CRLFTAB);
        sb.append("node list new: ").append(newNodeCount).append(CRLFTAB);
        for (int i = 0; i < newNodeCount; i++)
            sb.append(TAB).append(" --> ").append(Long.toHexString(mNewConfig[i]).toUpperCase()).append(CRLFTAB);
        sb.append(CRLF);
        return sb.toString();

    }
}
