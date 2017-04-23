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

package com.tgx.zq.z.queen.cluster.operation;

import java.util.LinkedList;
import java.util.List;

import com.tgx.zq.z.queen.base.disruptor.inf.IEventOp;
import com.tgx.zq.z.queen.base.util.Triple;
import com.tgx.zq.z.queen.cluster.node.ClusterNode;
import com.tgx.zq.z.queen.io.bean.cluster.XF002_ClusterLocal;
import com.tgx.zq.z.queen.io.disruptor.operations.ws.WRITE_OPERATOR;
import com.tgx.zq.z.queen.io.inf.ICommand;
import com.tgx.zq.z.queen.io.inf.ISession;
import com.tgx.zq.z.queen.io.ws.protocol.bean.cluster.raft.X10_StartElection;
import com.tgx.zq.z.queen.io.ws.protocol.bean.cluster.raft.X12_AppendEntity;
import com.tgx.zq.z.queen.io.ws.protocol.bean.cluster.raft.X15_CommitEntry;
import com.tgx.zq.z.queen.io.ws.protocol.bean.cluster.raft.X19_LeadLease;
import com.tgx.zq.z.queen.io.ws.protocol.bean.control.X102_Ping;

/**
 * @author William.d.zk
 */
public enum NODE_LOCAL
        implements
        IEventOp<XF002_ClusterLocal, ClusterNode<?, ?, ?>>
{

    INSTANCE;

    @SuppressWarnings({ "unchecked",
                        "rawtypes" })
    @Override
    public List<Triple<ICommand, ISession, IEventOp<ICommand, ISession>>> handleResultAsList(XF002_ClusterLocal localCmd,
                                                                                             ClusterNode clusterNode) {
        List<Triple<ICommand, ISession, IEventOp<ICommand, ISession>>> result = null;
        List<ICommand> wList = null;
        switch (localCmd.getRaftCommand()) {
            case X10_StartElection.COMMAND:
                wList = clusterNode.proposal(clusterNode,
                                             new LinkedList<>(),
                                             clusterNode.getIdentity(),
                                             clusterNode.newTerm(),
                                             clusterNode.getNextSlot(),
                                             clusterNode.getLastCommittedTermId(),
                                             clusterNode.getLastCommittedSlotIndex());

                break;
            case X12_AppendEntity.COMMAND:
                wList = clusterNode.jointConsensus(clusterNode,
                                                   new LinkedList<>(),
                                                   clusterNode.getIdentity(),
                                                   clusterNode.getCurrentTermId(),
                                                   clusterNode.getNextSlot(),
                                                   clusterNode.getLastCommittedSlotIndex(),
                                                   clusterNode.getNewConfig(),
                                                   clusterNode.getOldConfig());

                break;
            case X15_CommitEntry.COMMAND:
                wList = clusterNode.configConsensus(clusterNode,
                                                    new LinkedList<>(),
                                                    clusterNode.getIdentity(),
                                                    clusterNode.getCurrentTermId(),
                                                    clusterNode.getNextSlot(),
                                                    clusterNode.getLastCommittedSlotIndex(),
                                                    clusterNode.getNewConfig());

                break;
            case X19_LeadLease.COMMAND:
                wList = clusterNode.lease(clusterNode,
                                          new LinkedList<>(),
                                          clusterNode.getIdentity(),
                                          clusterNode.getCurrentTermId(),
                                          clusterNode.getLastCommittedSlotIndex());
                break;
            case X102_Ping.COMMAND:
                wList = clusterNode.heartbeatAll(new LinkedList<>(), new X102_Ping());
                break;
        }
        if (wList != null && !wList.isEmpty()) {
            result = new LinkedList<>();
            for (ICommand out : wList)
                result.add(new Triple<>(out, out.getSession(), WRITE_OPERATOR.PLAIN_SYMMETRY));
        }
        return result;
    }

}
