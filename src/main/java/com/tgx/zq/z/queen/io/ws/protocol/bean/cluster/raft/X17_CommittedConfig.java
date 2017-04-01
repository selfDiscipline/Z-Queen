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

import java.util.Arrays;

import com.tgx.zq.z.queen.base.util.IoUtil;

public class X17_CommittedConfig
        extends
        X1X_ClusterExchange
{
    public final static int COMMAND = 0x17;
    private int             mNewNodeCount;
    private long[]          mNewConfig;

    public X17_CommittedConfig(long _uid, long leaderId, long termId, long slotIndex) {
        super(COMMAND, _uid, leaderId, termId, slotIndex);
    }

    public X17_CommittedConfig() {
        super(COMMAND);
    }

    @Override
    public int dataLength() {
        return super.dataLength() + 1 + mNewNodeCount << 3;
    }

    @Override
    public int encodec(byte[] data, int pos) {
        pos += IoUtil.writeByte(mNewNodeCount, data, pos);
        pos += IoUtil.writeLongArray(mNewConfig, data, pos);
        return super.encodec(data, pos);
    }

    @Override
    public int decodec(byte[] data, int pos) {
        mNewNodeCount = data[pos++] & 0xFF;
        if (mNewNodeCount > 0) mNewConfig = new long[mNewNodeCount];
        pos = IoUtil.readLongArray(data, pos, mNewConfig);
        return super.decodec(data, pos);
    }

    public long[] getNewConfig() {
        return mNewConfig;
    }

    public X17_CommittedConfig setNewConfig(long... newConfig) {
        if (newConfig != null && newConfig.length > 0) {
            mNewConfig = newConfig;
            mNewNodeCount = newConfig.length;
        }
        return this;
    }

    @Override
    public X17_CommittedConfig duplicate() {
        X17_CommittedConfig x17 = new X17_CommittedConfig(getUID(), nodeId, termId, slotIndex);
        x17.setNewConfig(mNewConfig);
        x17.setCluster(isCluster());
        return x17;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(super.toString());
        return sb.append("commit new config: ")
                 .append(mNewNodeCount)
                 .append(CRLFTAB)
                 .append(Arrays.toString(mNewConfig))
                 .append(CRLF)
                 .toString();

    }
}
