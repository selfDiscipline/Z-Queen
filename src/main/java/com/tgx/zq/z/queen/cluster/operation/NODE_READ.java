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
package com.tgx.zq.z.queen.cluster.operation;

import java.util.logging.Logger;

import com.tgx.zq.z.queen.base.disruptor.inf.IEventOp;
import com.tgx.zq.z.queen.base.util.Triple;
import com.tgx.zq.z.queen.io.disruptor.operations.ws.DEFAULT_TRANSFER_DISPATCH;
import com.tgx.zq.z.queen.io.inf.ICommand;
import com.tgx.zq.z.queen.io.inf.IPipeRead;
import com.tgx.zq.z.queen.io.inf.IPoS;
import com.tgx.zq.z.queen.io.inf.ISession;
import com.tgx.zq.z.queen.io.ws.filter.WsControlFilter;
import com.tgx.zq.z.queen.io.ws.filter.WsFrameFilter;
import com.tgx.zq.z.queen.io.ws.filter.ZCommandFilter;
import com.tgx.zq.z.queen.io.ws.protocol.WsContext;
import com.tgx.zq.z.queen.io.ws.protocol.bean.cluster.raft.X10_StartElection;
import com.tgx.zq.z.queen.io.ws.protocol.bean.cluster.raft.X11_Ballot;
import com.tgx.zq.z.queen.io.ws.protocol.bean.cluster.raft.X12_AppendEntity;
import com.tgx.zq.z.queen.io.ws.protocol.bean.cluster.raft.X13_EntryAck;
import com.tgx.zq.z.queen.io.ws.protocol.bean.cluster.raft.X14_RSyncEntry;
import com.tgx.zq.z.queen.io.ws.protocol.bean.cluster.raft.X15_JointConsensus;
import com.tgx.zq.z.queen.io.ws.protocol.bean.cluster.raft.X16_ConfigAck;
import com.tgx.zq.z.queen.io.ws.protocol.bean.cluster.raft.X17_CommittedConfig;
import com.tgx.zq.z.queen.io.ws.protocol.bean.cluster.raft.X18_LeadLease;
import com.tgx.zq.z.queen.io.ws.protocol.bean.cluster.raft.X19_LeaseAck;
import com.tgx.zq.z.queen.io.ws.protocol.bean.cluster.raft.X1A_CommitEntry;
import com.tgx.zq.z.queen.io.ws.protocol.bean.cluster.raft.X1B_CommittedAck;
import com.tgx.zq.z.queen.io.ws.protocol.bean.cluster.raft.X1C_ClientEntry;
import com.tgx.zq.z.queen.io.ws.protocol.bean.cluster.raft.X1D_CreateSnapshot;
import com.tgx.zq.z.queen.io.ws.protocol.bean.cluster.raft.X1E_SnapshotAck;
import com.tgx.zq.z.queen.io.ws.protocol.bean.cluster.raft.X1F_SnapshotInstall;

public enum NODE_READ
        implements
        IEventOp<IPoS, ISession>,
        IPipeRead
{
    INSTANCE;
    Logger                log = Logger.getLogger(getClass().getSimpleName());
    private WsFrameFilter wsFilter;

    NODE_READ() {
        wsFilter = new WsFrameFilter();
        wsFilter.linkFront(new ZCommandFilter(command -> {
            switch (command) {
                case X10_StartElection.COMMAND:
                    return new X10_StartElection();
                case X11_Ballot.COMMAND:
                    return new X11_Ballot();
                case X12_AppendEntity.COMMAND:
                    return new X12_AppendEntity();
                case X13_EntryAck.COMMAND:
                    return new X13_EntryAck();
                case X14_RSyncEntry.COMMAND:
                    return new X14_RSyncEntry();
                case X15_JointConsensus.COMMAND:
                    return new X15_JointConsensus();
                case X16_ConfigAck.COMMAND:
                    return new X16_ConfigAck();
                case X17_CommittedConfig.COMMAND:
                    return new X17_CommittedConfig();
                case X18_LeadLease.COMMAND:
                    return new X18_LeadLease();
                case X19_LeaseAck.COMMAND:
                    return new X19_LeaseAck();
                case X1A_CommitEntry.COMMAND:
                    return new X1A_CommitEntry();
                case X1B_CommittedAck.COMMAND:
                    return new X1B_CommittedAck();
                case X1C_ClientEntry.COMMAND:
                    return new X1C_ClientEntry();
                case X1D_CreateSnapshot.COMMAND:
                    return new X1D_CreateSnapshot();
                case X1E_SnapshotAck.COMMAND:
                    return new X1E_SnapshotAck();
                case X1F_SnapshotInstall.COMMAND:
                    return new X1F_SnapshotInstall();
                default:
                    log.warning("command is not cluster consistentHandle: X" + Integer.toHexString(command).toUpperCase());
                    break;
            }
            return null;
        })).linkFront(new WsControlFilter());
    }

    @SuppressWarnings("unchecked")
    @Override
    public Triple<ICommand[], ISession, IEventOp<ICommand[], ISession>> handle(IPoS _InPoS, ISession session) {
        return new Triple<>(filterRead(_InPoS, wsFilter, (WsContext) session.getContext()),
                            session,
                            DEFAULT_TRANSFER_DISPATCH.PLAIN_SYMMETRY);
    }

}
