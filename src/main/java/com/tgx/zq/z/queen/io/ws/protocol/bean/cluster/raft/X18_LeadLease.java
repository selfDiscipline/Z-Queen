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

public class X18_LeadLease
        extends
        X1X_ClusterExchange
{
    public final static int COMMAND = 0x18;

    public X18_LeadLease(long mMsgUID, long nodeId, long termId, long slotIndex) {
        super(COMMAND, mMsgUID, nodeId, termId, slotIndex);
    }

    public X18_LeadLease() {
        super(COMMAND);
    }

    @Override
    public X18_LeadLease duplicate() {
        X18_LeadLease x18 = new X18_LeadLease(getUID(), nodeId, termId, slotIndex);
        x18.setCluster(isCluster());
        return x18;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(super.toString());
        return sb.append("leader term: ")
                 .append(termId)
                 .append(ISerialTick.CRLFTAB)
                 .append("leader last slot index")
                 .append(slotIndex)
                 .append(ISerialTick.CRLF)
                 .toString();
    }
}
