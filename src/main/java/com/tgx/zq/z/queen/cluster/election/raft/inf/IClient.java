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

public interface IClient<E extends IDbStorageProtocol>
        extends
        ITaskListener
{

    List<ICommand> sendEntry(IBroadcast<WsContext> broadcast, List<ICommand> wList, long leaderId, LogEntry<E> entity);

    List<ICommand> consistentStorage(E storage, List<ICommand> wList);

    ICommand onReceiveEntryAck(long leaderId,
                               long leaderTermId,
                               long leaderSlotIndex,
                               long leaderLastCommittedSlotIndex,
                               long clientSlotIndex);

    long getClientSlotIndex();

    long getClientNextSlot();
}
