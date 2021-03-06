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
package com.tgx.zq.z.queen.io.disruptor.handler;

import java.nio.channels.AsynchronousSocketChannel;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;

import com.lmax.disruptor.RingBuffer;
import com.tgx.zq.z.queen.base.constant.QueenCode;
import com.tgx.zq.z.queen.base.disruptor.QEvent;
import com.tgx.zq.z.queen.base.disruptor.inf.IError;
import com.tgx.zq.z.queen.base.disruptor.inf.IEventOp;
import com.tgx.zq.z.queen.base.disruptor.inf.IEventOp.Type;
import com.tgx.zq.z.queen.base.disruptor.inf.IPipeEventHandler;
import com.tgx.zq.z.queen.base.util.Pair;
import com.tgx.zq.z.queen.base.util.Triple;
import com.tgx.zq.z.queen.biz.template.BizNode;
import com.tgx.zq.z.queen.cluster.node.ClusterNode;
import com.tgx.zq.z.queen.cluster.replication.bean.raft.LogEntry;
import com.tgx.zq.z.queen.cluster.replication.inf.IConsistentWrite;
import com.tgx.zq.z.queen.db.bdb.inf.IBizDao;
import com.tgx.zq.z.queen.db.bdb.inf.IDbStorageProtocol;
import com.tgx.zq.z.queen.io.bean.XF000_NULL;
import com.tgx.zq.z.queen.io.bean.cluster.XF001_TransactionCompleted;
import com.tgx.zq.z.queen.io.disruptor.operations.CLOSE_OPERATOR;
import com.tgx.zq.z.queen.io.disruptor.operations.ws.WRITE_OPERATOR;
import com.tgx.zq.z.queen.io.impl.AioSessionManager;
import com.tgx.zq.z.queen.io.inf.ICommand;
import com.tgx.zq.z.queen.io.inf.IConnectActive;
import com.tgx.zq.z.queen.io.inf.IConnectMode;
import com.tgx.zq.z.queen.io.inf.ISession;
import com.tgx.zq.z.queen.io.inf.ISessionDismiss;
import com.tgx.zq.z.queen.io.ws.protocol.bean.cluster.raft.X10_StartElection;
import com.tgx.zq.z.queen.io.ws.protocol.bean.cluster.raft.X11_Ballot;
import com.tgx.zq.z.queen.io.ws.protocol.bean.cluster.raft.X12_AppendEntity;
import com.tgx.zq.z.queen.io.ws.protocol.bean.cluster.raft.X13_EntryAck;
import com.tgx.zq.z.queen.io.ws.protocol.bean.cluster.raft.X14_RSyncEntry;
import com.tgx.zq.z.queen.io.ws.protocol.bean.cluster.raft.X15_CommitEntry;
import com.tgx.zq.z.queen.io.ws.protocol.bean.cluster.raft.X16_CommittedAck;
import com.tgx.zq.z.queen.io.ws.protocol.bean.cluster.raft.X17_ClientEntry;
import com.tgx.zq.z.queen.io.ws.protocol.bean.cluster.raft.X18_ClientEntryAck;
import com.tgx.zq.z.queen.io.ws.protocol.bean.cluster.raft.X19_LeadLease;
import com.tgx.zq.z.queen.io.ws.protocol.bean.cluster.raft.X1A_LeaseAck;
import com.tgx.zq.z.queen.io.ws.protocol.bean.control.X101_Close;
import com.tgx.zq.z.queen.io.ws.protocol.bean.control.X104_ExchangeIdentity;

/**
 * @author William.d.zk
 */
public abstract class ClusterHandler<E extends IDbStorageProtocol, D extends IBizDao<E>, N extends BizNode<E, D>>
        implements
        IPipeEventHandler<QEvent, QEvent>,
        IConsistentWrite<E, D, N>
{

    private final RingBuffer<QEvent>   _WriteRB, _ConsistentResultRB;
    private final ClusterNode<E, D, N> _ClusterNode;

    public ClusterHandler(final RingBuffer<QEvent> writeRB,
                          final RingBuffer<QEvent> consistentResultRB,
                          final ClusterNode<E, D, N> clusterNode) {
        _ClusterNode = clusterNode;
        _WriteRB = writeRB;
        _ConsistentResultRB = consistentResultRB;
    }

    @SuppressWarnings("unchecked")
    @Override
    public final void onEvent(QEvent event, long sequence, boolean endOfBatch) throws Exception {
        if (event.noError()) {
            switch (event.getEventType()) {
                case CONNECTED:
                    IEventOp<Pair<ClusterNode<E, D, N>, IConnectMode.OPERATION_MODE>, AsynchronousSocketChannel> cOperator = event.getEventOp();// NODE_CONNECTED
                    Pair<Pair<ClusterNode<E, D, N>, IConnectMode.OPERATION_MODE>, AsynchronousSocketChannel> cContent = event.getContent();
                    Pair<ClusterNode<E, D, N>, IConnectMode.OPERATION_MODE> nmPair = cContent.first();
                    // 集群至少2台机器的时候才需要进行诸多网络操作。
                    AsynchronousSocketChannel channel = cContent.second();
                    Triple<ICommand, ISession, IEventOp<ICommand, ISession>> cResult = cOperator.handle(nmPair, channel);
                    ICommand inCmd = cResult.first();
                    if (inCmd != null
                        && inCmd.getSerialNum() != XF000_NULL.COMMAND) publish(_WriteRB,
                                                                               Type.DISPATCH,
                                                                               inCmd,
                                                                               cResult.second(),
                                                                               cResult.third());
                    break;
                case CLOSE:
                    IEventOp<Void, ISession> ccOperator = event.getEventOp();
                    Pair<Void, ISession> ccContent = event.getContent();
                    ISession session = ccContent.second();
                    ISessionDismiss dismiss = session.getDismissCallback();
                    if (dismiss != null && !session.isClosed()) dismiss.onDismiss(session);
                    ccOperator.handle(null, session);
                    _ClusterNode.clearSession(session);
                    _ClusterNode.rmSession(session);
                    break;
                case LOCAL:
                    Pair<ICommand, AioSessionManager> lContent = event.getContent();
                    IEventOp<ICommand, AioSessionManager> lcOperator = event.getEventOp();// NODE_LOCAL
                    List<Triple<ICommand, ISession, IEventOp<ICommand, ISession>>> lcResult = lcOperator.handleResultAsList(lContent.first(),
                                                                                                                            lContent.second());
                    if (lcResult != null) for (Triple<ICommand, ISession, IEventOp<ICommand, ISession>> llt : lcResult) {
                        publish(_WriteRB, Type.DISPATCH, llt.first(), llt.second(), llt.third());
                    }
                    break;
                case LOGIC:
                    // Cluster bind Session
                    IEventOp<ICommand, ISession> lOperator = event.getEventOp();// DEFAULT_TRANSFER_LOGIC
                    Pair<ICommand, ISession> rContent = event.getContent();
                    inCmd = rContent.first();
                    session = rContent.second();
                    List<ICommand> wList = new LinkedList<>();
                    switch (inCmd.getSerialNum()) {
                        case X10_StartElection.COMMAND:
                            X10_StartElection x10 = (X10_StartElection) inCmd;
                            wList.add(_ClusterNode.onReceiveElection(x10.nodeId,
                                                                     x10.termId,
                                                                     x10.slotIndex,
                                                                     x10.lastCommittedTermId,
                                                                     x10.lastCommittedSlotIndex));
                            break;
                        case X11_Ballot.COMMAND:
                            X11_Ballot x11 = (X11_Ballot) inCmd;
                            _ClusterNode.onReceiveBallot(x11.nodeId, x11.termId, x11.slotIndex, x11.ballotId, x11.accept);
                            break;
                        case X12_AppendEntity.COMMAND:
                            X12_AppendEntity x12 = (X12_AppendEntity) inCmd;
                            wList.add(_ClusterNode.onReceiveEntity(x12.getLeaderId(),
                                                                   x12.getLeaderCommittedSlotIndex(),
                                                                   (LogEntry<E>) x12.getEntry()));
                            break;
                        case X13_EntryAck.COMMAND:
                            X13_EntryAck x13 = (X13_EntryAck) inCmd;
                            _ClusterNode.onReceiveEntryAck(wList,
                                                           x13.nodeId,
                                                           x13.termId,
                                                           x13.slotIndex,
                                                           x13.nextIndex,
                                                           x13.accept,
                                                           x13.qualify);
                            break;
                        case X15_CommitEntry.COMMAND:
                            X15_CommitEntry x1A = (X15_CommitEntry) inCmd;
                            wList.add(_ClusterNode.onReceiveCommit(x1A.nodeId, x1A.termId, x1A.slotIndex, x1A.idempotent));
                            break;
                        case X17_ClientEntry.COMMAND:
                            X17_ClientEntry x1C = (X17_ClientEntry) inCmd;
                            LogEntry<E> clientLogEntry = new LogEntry<>();
                            clientLogEntry.decode(x1C.getPayload());
                            _ClusterNode.onReceiveClientEntity(wList, x1C.nodeId, clientLogEntry);
                            break;
                        case X18_ClientEntryAck.COMMAND:
                            X18_ClientEntryAck x18 = (X18_ClientEntryAck) inCmd;
                            wList.add(_ClusterNode.onReceiveEntryAck(x18.nodeId,
                                                                     x18.termId,
                                                                     x18.slotIndex,
                                                                     x18.lastCommittedSlotIndex,
                                                                     x18.clientSlotIndex));
                            break;
                        case X19_LeadLease.COMMAND:
                            X19_LeadLease x19 = (X19_LeadLease) inCmd;
                            wList.add(_ClusterNode.onReceiveLease(x19.nodeId, x19.termId, x19.slotIndex));
                            break;
                        case X101_Close.COMMAND:
                            dismiss = session.getDismissCallback();
                            if (dismiss != null && !session.isClosed()) dismiss.onDismiss(session);
                            CLOSE_OPERATOR.INSTANCE.handle(null, session);
                            _ClusterNode.clearSession(session);
                            _ClusterNode.rmSession(session);
                            break;
                        case X104_ExchangeIdentity.COMMAND:
                            X104_ExchangeIdentity x104 = (X104_ExchangeIdentity) inCmd;
                            long identity = x104.getNodeIdentity();
                            long _clusterId = identity & QueenCode._IndexHighMask;
                            long _XID_TYPE = _clusterId & QueenCode.XID_MK;
                            _ClusterNode.mapSession(identity, session, _clusterId, _XID_TYPE);
                            _ClusterNode.onClusterConnected(_clusterId);
                            break;
                        default:
                            Triple<ICommand, ISession, IEventOp<ICommand, ISession>> rResult = lOperator.handle(inCmd, session);
                            if (rResult.first().getSerialNum() != XF000_NULL.COMMAND) publish(_WriteRB,
                                                                                              Type.DISPATCH,
                                                                                              rResult.first(),
                                                                                              rResult.second(),
                                                                                              rResult.third());
                            break;
                    }
                    for (ICommand outCommand : wList)
                        switch (outCommand.getSerialNum()) {
                            case XF000_NULL.COMMAND:
                                break;// drop
                            case XF001_TransactionCompleted.COMMAND:
                                publish(_ConsistentResultRB, Type.BRANCH, outCommand, null, null);
                                break;
                            default:
                                ISession oSession = outCommand.getSession();
                                publish(_WriteRB,
                                        Type.DISPATCH,
                                        outCommand,
                                        oSession == null ? session : oSession,
                                        WRITE_OPERATOR.PLAIN_SYMMETRY);
                                break;
                        }
                    break;
                case BRANCH:
                    Pair<ICommand, ISession> bContent = event.getContent();
                    IEventOp<ICommand, ISession> bOperator = event.getEventOp();
                    inCmd = bContent.first();
                    session = bContent.second();
                    Collection<ICommand> rCollection = consistentWrite(inCmd, _ClusterNode, session, inCmd.getTransactionKey());
                    if (rCollection != null) for (ICommand out : rCollection)
                        switch (out.getSerialNum()) {
                            case XF001_TransactionCompleted.COMMAND:
                                tryPublish(_ConsistentResultRB, Type.BRANCH, out, null, bOperator);
                                break;
                            default:
                                ISession oSession = out.getSession();
                                publish(_WriteRB, Type.DISPATCH, out, oSession == null ? session : oSession, WRITE_OPERATOR.PLAIN_SYMMETRY);
                                break;
                        }
                    break;
                default:
                    break;
            }
        }
        else {
            IError.Type errorType = event.getErrorType();
            switch (errorType) {
                case CONNECT_FAILED:
                    IEventOp<Pair<ClusterNode<E, D, N>, Throwable>, IConnectActive> cOperator = event.getEventOp();
                    Pair<Pair<ClusterNode<E, D, N>, Throwable>, IConnectActive> cContent = event.getContent();
                    cOperator.handle(cContent.first(), cContent.second());
                    ClusterNode<E, D, N> clusterNode = cContent.first().first();
                    clusterNode.onError(cContent.second());
                    break;
                case CLOSED:// CLOSE_ERROR_OPERATOR
                    IEventOp<Throwable, ISession> ccOperator = event.getEventOp();
                    Pair<Throwable, ISession> ccContent = event.getContent();
                    Throwable throwable = ccContent.first();
                    ISession session = ccContent.second();
                    ISessionDismiss dismiss = session.getDismissCallback();
                    if (dismiss != null && !session.isClosed()) dismiss.onDismiss(session);
                    ccOperator.handle(throwable, session);
                    _ClusterNode.clearSession(session);
                    _ClusterNode.rmSession(session);
                    break;
                default:
                    IEventOp<Throwable, ISession> eOperator = event.getEventOp();
                    Pair<Throwable, ISession> eContent = event.getContent();
                    eOperator.handle(eContent.first(), eContent.second());// LOG_OPERATOR
                    break;
            }
        }
        event.reset();
    }

    public final ClusterNode<E, D, N> getNode() {
        return _ClusterNode;
    }

    @Override
    public final RESULT trial(ICommand cmd, IConnectMode.OPERATION_MODE mode) {
        boolean modeCheck = mode.equals(IConnectMode.OPERATION_MODE.CONNECT_CLUSTER)
                            || mode.equals(IConnectMode.OPERATION_MODE.ACCEPT_CLUSTER);
        if (modeCheck) switch (cmd.getSerialNum()) {
            case X10_StartElection.COMMAND:
            case X11_Ballot.COMMAND:
            case X12_AppendEntity.COMMAND:
            case X13_EntryAck.COMMAND:
            case X14_RSyncEntry.COMMAND:
            case X15_CommitEntry.COMMAND:
            case X16_CommittedAck.COMMAND:
            case X17_ClientEntry.COMMAND:
            case X18_ClientEntryAck.COMMAND:
            case X19_LeadLease.COMMAND:
            case X1A_LeaseAck.COMMAND:
            case X101_Close.COMMAND:
            case X104_ExchangeIdentity.COMMAND:
                return RESULT.HANDLE;
        }
        return RESULT.IGNORE;
    }

}
