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
public class X11_Ballot
        extends
        X1X_ClusterExchange
{

    public final static int COMMAND = 0x11;
    public boolean          accept;
    public long             ballotId;

    public X11_Ballot() {
        super(COMMAND);
    }

    public X11_Ballot(long _uid, long nodeId, long termId, long slotIndex, long ballotId, boolean accept) {
        super(COMMAND, _uid, nodeId, termId, slotIndex);
        this.accept = accept;
        this.ballotId = ballotId;
    }

    @Override
    public int dataLength() {
        return super.dataLength() + 9;
    }

    @Override
    public int encodec(byte[] data, int pos) {
        pos += IoUtil.writeByte(accept ? 1 : 0, data, pos);
        pos += IoUtil.writeLong(ballotId, data, pos);
        return super.encodec(data, pos);
    }

    @Override
    public int decodec(byte[] data, int pos) {
        accept = data[pos++] > 0;
        ballotId = IoUtil.readLong(data, pos);
        pos += 8;
        return super.decodec(data, pos);
    }

    @Override
    public String toString() {
        return super.toString()
               + "node :"
               + Long.toHexString(nodeId).toUpperCase()
               + CRLF_TAB
               + "result:"
               + (accept ? "accept" : "reject")
               + CRLF_TAB
               + "vote for : "
               + Long.toHexString(ballotId).toUpperCase()
               + CRLF;

    }
}
