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
import com.tgx.zq.z.queen.io.ws.protocol.Command;
import com.tgx.zq.z.queen.io.ws.protocol.WsContext;

public abstract class X1X_ClusterExchange
        extends
        Command<WsContext>
{
    public final static int SerialNum = 0x1F;
    public long             termId;
    public long             slotIndex;
    public long             nodeId;

    X1X_ClusterExchange(int command, long mMsgUID) {
        super(command, mMsgUID);
    }

    public X1X_ClusterExchange(int command) {
        super(command, true);
    }

    public X1X_ClusterExchange(int command, long mMsgUID, long nodeId, long termId, long slotIndex) {
        super(command, mMsgUID);
        this.nodeId = nodeId;
        this.termId = termId;
        this.slotIndex = slotIndex;
    }

    @Override
    public int getSuperSerialNum() {
        return SerialNum;
    }

    @Override
    public int dataLength() {
        return super.dataLength() + 24;
    }

    @Override
    public int encodec(byte[] data, int pos) {
        pos += IoUtil.writeLong(nodeId, data, pos);
        pos += IoUtil.writeLong(termId, data, pos);
        pos += IoUtil.writeLong(slotIndex, data, pos);
        return pos;
    }

    @Override
    public int decodec(byte[] data, int pos) {
        nodeId = IoUtil.readLong(data, pos);
        pos += 8;
        termId = IoUtil.readLong(data, pos);
        pos += 8;
        slotIndex = IoUtil.readLong(data, pos);
        pos += 8;
        return pos;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(super.toString());
        return sb.append("node id: ")
                 .append(Long.toHexString(nodeId).toUpperCase())
                 .append(CRLF_TAB)
                 .append("current term id: ")
                 .append(termId)
                 .append(CRLF_TAB)
                 .append("slot index: ")
                 .append(slotIndex)
                 .append(CRLF_TAB)
                 .toString();
    }

    @Override
    public int getPriority() {
        return QOS_01_CLUSTER_CONTROL;
    }

    @Override
    public long getOrder() {
        return slotIndex;
    }

    @Override
    public boolean isOrder() {
        return true;
    }
}
