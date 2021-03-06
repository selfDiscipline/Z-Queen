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

import com.tgx.zq.z.queen.base.util.IoUtil;

/**
 * @author William.d.zk
 */
public class X13_EntryAck
        extends
        X1X_ClusterExchange
{
    public final static int COMMAND = 0x13;
    public long             nextIndex, lastCommittedSlotIndex;
    public boolean          accept, qualify;

    public X13_EntryAck() {
        super(COMMAND);
    }

    public X13_EntryAck(long mMsgUID, long nodeId, long termId, long slotIndex, boolean accept, boolean qualify) {
        super(COMMAND, mMsgUID, nodeId, termId, slotIndex);
        this.accept = accept;
        this.qualify = qualify;
        nextIndex = -1;
    }

    @Override
    public int dataLength() {
        return super.dataLength() + 17;
    }

    @Override
    public int decodec(byte[] data, int pos) {
        nextIndex = IoUtil.readLong(data, pos);
        pos += 8;
        lastCommittedSlotIndex = IoUtil.readLong(data, pos);
        pos += 8;
        byte attr = data[pos++];
        accept = (attr & 0x01) != 0;
        qualify = (attr & 0x02) != 0;
        return super.decodec(data, pos);
    }

    @Override
    public int encodec(byte[] data, int pos) {
        pos += IoUtil.writeLong(nextIndex, data, pos);
        pos += IoUtil.writeLong(lastCommittedSlotIndex, data, pos);
        pos += IoUtil.writeByte((accept ? 1 : 0) | (qualify ? 2 : 0), data, pos);
        return super.encodec(data, pos);
    }

    @Override
    public String toString() {
        return super.toString()
               + "result:"
               + (accept ? "accept" : "reject")
               + CRLF_TAB
               + (qualify ? "qualify" : "disqualify")
               + CRLF_TAB
               + "next index: "
               + nextIndex
               + CRLF_TAB
               + "last committed slot: "
               + lastCommittedSlotIndex
               + CRLF;
    }
}
