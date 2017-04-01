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

package com.tgx.zq.z.queen.cluster.election.raft.inf;

import java.util.List;

import com.tgx.zq.z.queen.base.classic.task.inf.ITaskListener;
import com.tgx.zq.z.queen.cluster.inf.IBroadcast;
import com.tgx.zq.z.queen.cluster.replication.bean.raft.LogEntry;
import com.tgx.zq.z.queen.db.bdb.inf.IDbStorageProtocol;
import com.tgx.zq.z.queen.io.inf.ICommand;
import com.tgx.zq.z.queen.io.ws.protocol.WsContext;

public interface ICandidate<E extends IDbStorageProtocol>
        extends
        ITaskListener
{

    List<ICommand> proposal(IBroadcast<WsContext> broadcast,
                            List<ICommand> wList,
                            long candidateId,
                            long termId,
                            long slotIndex,
                            long lastCommittedTermId,
                            long lastCommittedSlotIndex);

    void randomWaitNext();

    long getIdentity();

    long getCurrentTermId();

    void setTermId(long termId);

    long getSlotIndex();

    long getLastCommittedTermId();

    long getLastCommittedSlotIndex();

    long newTerm();

    void revertFollower(long leaderId);

    ICommand onReceiveElection(long candidateId, long termId, long slotIndex, long lastCommittedTermId, long lastCommittedSlotIndex);

    void onReceiveBallot(long nodeId, long termId, long slotIndex, long ballotId, boolean accept);

    ICommand onReceiveEntity(long leaderId, long lastCommittedSlotIndex, LogEntry<E> entry);

}
