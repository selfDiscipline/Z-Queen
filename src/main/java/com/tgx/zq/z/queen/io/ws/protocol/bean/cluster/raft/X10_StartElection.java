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

package com.tgx.zq.z.queen.io.ws.protocol.bean.cluster.raft;

import com.tgx.zq.z.queen.base.util.ArrayUtil;
import com.tgx.zq.z.queen.base.util.IoUtil;

public class X10_StartElection
        extends
        X1X_ClusterExchange
{
    public final static int COMMAND = 0x10;
    public long             lastCommittedSlotIndex, lastCommittedTermId;
    public long[]           config;

    public X10_StartElection(long _uid,
                             long nodeId,
                             long termId,
                             long slotIndex,
                             long lastCommittedTermId,
                             long lastCommittedSlotIndex,
                             long... config) {
        super(COMMAND, _uid, nodeId, termId, slotIndex);
        this.lastCommittedTermId = lastCommittedTermId;
        this.lastCommittedSlotIndex = lastCommittedSlotIndex;
        this.config = config;
    }

    public X10_StartElection() {
        super(COMMAND);
    }

    @Override
    public int decodec(byte[] data, int pos) {
        lastCommittedTermId = IoUtil.readLong(data, pos);
        pos += 8;
        lastCommittedSlotIndex = IoUtil.readLong(data, pos);
        pos += 8;
        int nodeCount = data[pos++] & 0xFF;
        if (nodeCount > 0) {
            config = new long[nodeCount];
            pos = IoUtil.readLongArray(data, pos, config);
        }
        return super.decodec(data, pos);
    }

    @Override
    public int dataLength() {
        return super.dataLength() + 17 + (config == null ? 0 : config.length << 3);
    }

    @Override
    public int encodec(byte[] data, int pos) {
        pos += IoUtil.writeLong(lastCommittedTermId, data, pos);
        pos += IoUtil.writeLong(lastCommittedSlotIndex, data, pos);
        pos += IoUtil.writeByte(config == null ? 0 : config.length, data, pos);
        pos += IoUtil.writeLongArray(config, data, pos);
        return super.encodec(data, pos);
    }

    @Override
    public X10_StartElection duplicate() {
        X10_StartElection x10 = new X10_StartElection(getUID(),
                                                      nodeId,
                                                      termId,
                                                      slotIndex,
                                                      lastCommittedTermId,
                                                      lastCommittedSlotIndex,
                                                      config);
        x10.setCluster(isCluster());
        return x10;
    }

    @Override
    public String toString() {
        return super.toString()
               + "last committed term id: "
               + lastCommittedTermId
               + CRLF_TAB
               + "last committed slot index: "
               + lastCommittedSlotIndex
               + "node cluster: "
               + CRLF_TAB
               + ArrayUtil.toHexString(config)
               + CRLF;

    }

}
