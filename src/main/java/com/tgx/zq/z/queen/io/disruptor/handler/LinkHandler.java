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

package com.tgx.zq.z.queen.io.disruptor.handler;

import java.nio.channels.AsynchronousSocketChannel;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.logging.Level;

import com.lmax.disruptor.RingBuffer;
import com.tgx.zq.z.queen.base.constant.QueenCode;
import com.tgx.zq.z.queen.base.disruptor.QEvent;
import com.tgx.zq.z.queen.base.disruptor.inf.IError;
import com.tgx.zq.z.queen.base.disruptor.inf.IEventOp;
import com.tgx.zq.z.queen.base.disruptor.inf.IEventOp.Type;
import com.tgx.zq.z.queen.base.disruptor.inf.IPipeEventHandler;
import com.tgx.zq.z.queen.base.util.Pair;
import com.tgx.zq.z.queen.base.util.TimeUtil;
import com.tgx.zq.z.queen.base.util.Triple;
import com.tgx.zq.z.queen.biz.inf.IConsistentRead;
import com.tgx.zq.z.queen.biz.template.BizNode;
import com.tgx.zq.z.queen.cluster.consistent.bean.ConsistentTransaction;
import com.tgx.zq.z.queen.cluster.node.ClusterNode;
import com.tgx.zq.z.queen.db.bdb.inf.IBizDao;
import com.tgx.zq.z.queen.db.bdb.inf.IDbStorageProtocol;
import com.tgx.zq.z.queen.io.bean.XF000_NULL;
import com.tgx.zq.z.queen.io.disruptor.operations.CLOSE_OPERATOR;
import com.tgx.zq.z.queen.io.impl.AioSession;
import com.tgx.zq.z.queen.io.impl.AioSessionManager;
import com.tgx.zq.z.queen.io.inf.ICommand;
import com.tgx.zq.z.queen.io.inf.IConnectActive;
import com.tgx.zq.z.queen.io.inf.IConnectError;
import com.tgx.zq.z.queen.io.inf.IConnectMode;
import com.tgx.zq.z.queen.io.inf.IConnectMode.OPERATION_MODE;
import com.tgx.zq.z.queen.io.inf.ISession;
import com.tgx.zq.z.queen.io.inf.ISessionDismiss;
import com.tgx.zq.z.queen.io.ws.protocol.bean.control.X101_Close;
import com.tgx.zq.z.queen.io.ws.protocol.bean.control.X104_ExchangeIdentity;

public abstract class LinkHandler<E extends IDbStorageProtocol, D extends IBizDao<E>, N extends BizNode<E, D>>
        implements
        IPipeEventHandler<QEvent, QEvent>,
        IConsistentRead<E, D, N>
{

    private final RingBuffer<QEvent>                                           _WriteRB, _ConsistentWriteRB;
    private final N                                                            _BizNode;
    private final ConcurrentSkipListMap<Long, ConsistentTransaction<ISession>> _TransactionSkipListMap = new ConcurrentSkipListMap<>();

    public LinkHandler(final RingBuffer<QEvent> writeRB, final RingBuffer<QEvent> consistentWriteRB, final N bizNode) {
        _WriteRB = writeRB;
        _ConsistentWriteRB = consistentWriteRB;
        _BizNode = bizNode;
    }

    @Override
    public void onEvent(QEvent event, long sequence, boolean endOfBatch) throws Exception {
        if (event.noError()) {
            switch (event.getEventType()) {
                case CONNECTED:
                    IEventOp<Pair<N, OPERATION_MODE>, AsynchronousSocketChannel> cOperator = event.getEventOp();
                    Pair<Pair<N, IConnectMode.OPERATION_MODE>, AsynchronousSocketChannel> cContent = event.getContent();
                    Pair<N, IConnectMode.OPERATION_MODE> nmPair = cContent.first();
                    AsynchronousSocketChannel channel = cContent.second();
                    Triple<ICommand, AioSession, IEventOp<ICommand, AioSession>> cResult = cOperator.handle(nmPair, channel);
                    ICommand cmd = cResult.first();
                    if (cmd != null && cmd.getSerialNum() != XF000_NULL.COMMAND) {
                        publish(_WriteRB, Type.DISPATCH, cmd, cResult.second(), cResult.third());
                    }
                    break;
                case CLOSE:
                    IEventOp<Void, ISession> ccOperator = event.getEventOp();
                    Pair<Void, ISession> ccContent = event.getContent();
                    ISession session = ccContent.second();
                    ISessionDismiss dismiss = session.getDismissCallback();
                    if (dismiss != null && !session.isClosed()) dismiss.onDismiss(session);
                    ccOperator.handle(null, session);
                    _BizNode.clearSession(session);
                    _BizNode.rmSession(session);
                    break;
                case LOCAL:
                    Pair<ICommand, AioSessionManager> lcContent = event.getContent();
                    IEventOp<ICommand, AioSessionManager> lcOperator = event.getEventOp();
                    List<Triple<ICommand, ISession, IEventOp<ICommand, ISession>>> lcResult = lcOperator.handleResultAsList(lcContent.first(),
                                                                                                                            lcContent.second());
                    if (lcResult != null) for (Triple<ICommand, ISession, IEventOp<ICommand, ISession>> llt : lcResult) {
                        publish(_WriteRB, Type.DISPATCH, llt.first(), llt.second(), llt.third());
                    }
                    break;
                case LOGIC:
                    Pair<ICommand, ISession> rContent = event.getContent();
                    cmd = rContent.first();
                    session = rContent.second();
                    IEventOp<ICommand, ISession> rOperator = event.getEventOp();
                    Triple<RESULT, ICommand, Throwable> lResult = consistentRead(cmd, _BizNode.getBizDao());
                    convergentClearTransaction();
                    switch (lResult.first()) {
                        case HANDLE:
                            Triple<ICommand, ISession, IEventOp<ICommand, ISession>> rResult = rOperator.handle(cmd, session);
                            publish(_WriteRB, Type.DISPATCH, rResult.first(), rResult.second(), rResult.third());
                            break;
                        case PASS:
                            ICommand cCmd = lResult.second();
                            long transactionKey = ClusterNode.getUniqueIdentity(sequence);
                            cCmd.setTransactionKey(transactionKey);
                            _BizNode.mapSession(transactionKey, session);
                            _TransactionSkipListMap.put(transactionKey,
                                                        new ConsistentTransaction<>(transactionKey,
                                                                                    TimeUtil.CURRENT_TIME_SECOND_CACHE + 3,
                                                                                    session));
                            tryPublish(_ConsistentWriteRB, Type.BRANCH, cCmd, session, rOperator);
                            break;
                        case SECTION:
                            rResult = rOperator.handle(cmd, session);
                            publish(_WriteRB, Type.DISPATCH, rResult.first(), rResult.second(), rResult.third());
                            cCmd = lResult.second();
                            transactionKey = ClusterNode.getUniqueIdentity(sequence);
                            cCmd.setTransactionKey(transactionKey);
                            _BizNode.mapSession(transactionKey, session);
                            _TransactionSkipListMap.put(transactionKey,
                                                        new ConsistentTransaction<>(transactionKey,
                                                                                    TimeUtil.CURRENT_TIME_SECOND_CACHE + 3,
                                                                                    session));
                            tryPublish(_ConsistentWriteRB, Type.BRANCH, cCmd, session, rOperator);
                            break;
                        case IGNORE:
                            switch (cmd.getSerialNum()) {
                                case X101_Close.COMMAND:
                                    dismiss = session.getDismissCallback();
                                    if (dismiss != null && !session.isClosed()) dismiss.onDismiss(session);
                                    CLOSE_OPERATOR.INSTANCE.handle(null, session);
                                    _BizNode.clearSession(session);
                                    _BizNode.rmSession(session);
                                    break;
                                case X104_ExchangeIdentity.COMMAND:
                                    X104_ExchangeIdentity x104 = (X104_ExchangeIdentity) cmd;
                                    long identity = x104.getNodeIdentity();
                                    long clusterId = identity & QueenCode._IndexHighMask;
                                    long modeType = clusterId & QueenCode.XID_MK;
                                    _BizNode.mapSession(identity, session, clusterId, modeType);
                                    break;
                                default:
                                    Triple<ICommand, ISession, IEventOp<ICommand, ISession>> mqResult = _BizNode.getMqServer()
                                                                                                                .consistentHandle(cmd,
                                                                                                                                  session);
                                    if (mqResult != null && mqResult.first().getSerialNum() != XF000_NULL.COMMAND) {
                                        publish(_WriteRB, Type.DISPATCH, mqResult.first(), mqResult.second(), mqResult.third());
                                    }
                                    break;
                            }
                            break;
                        case ERROR:
                            log.log(Level.WARNING, " consistent read error: @->" + cmd.toString(), lResult.third());
                            break;
                        default:
                            break;
                    }
                    break;
                case BRANCH:
                    Pair<ICommand, ISession> bContent = event.getContent();
                    cmd = bContent.first();
                    Triple<ICommand, ISession, IEventOp<ICommand, ISession>> bResult = consistentRead(cmd.getTransactionKey(), _BizNode);
                    publish(_WriteRB, Type.DISPATCH, bResult.first(), bResult.second(), bResult.third());
                    clearTransaction(cmd.getTransactionKey());
                    break;
                default:
                    break;
            }
        }
        else {
            IError.Type errorType = event.getErrorType();
            switch (errorType) {
                case CONNECT_FAILED:
                    IEventOp<Pair<IConnectError, Throwable>, IConnectActive> cOperator = event.getEventOp();
                    Pair<Pair<IConnectError, Throwable>, IConnectActive> cContent = event.getContent();
                    IConnectError connectError = cContent.first().first();
                    IConnectActive cActive = cContent.second();
                    connectError.onError(cActive);
                    cOperator.handle(cContent.first(), cActive);
                    break;
                case CLOSED:
                    IEventOp<Throwable, ISession> ccOperator = event.getEventOp();
                    Pair<Throwable, ISession> ccContent = event.getContent();
                    ISession session = ccContent.second();
                    ISessionDismiss dismiss = session.getDismissCallback();
                    if (dismiss != null && !session.isClosed()) dismiss.onDismiss(session);
                    ccOperator.handle(ccContent.first(), session);
                    _BizNode.clearSession(session);
                    _BizNode.rmSession(session);
                    break;
                default:
                    IEventOp<Throwable, ISession> eOperator = event.getEventOp();
                    Pair<Throwable, ISession> eContent = event.getContent();
                    Throwable t = eContent.first();
                    session = eContent.second();
                    eOperator.handle(t, session);// LOG_OPERATOR
                    break;
            }

        }
        event.reset();
    }

    private void convergentClearTransaction() {
        if (_TransactionSkipListMap.isEmpty()) { return; }
        int convergentDepth = 2;
        for (int i = 0; i < convergentDepth; i++) {
            Map.Entry<Long, ConsistentTransaction<ISession>> firstEntry = _TransactionSkipListMap.firstEntry();
            if (firstEntry == null) {
                break;
            }
            ConsistentTransaction<ISession> transaction = firstEntry.getValue();
            if (transaction.isTimeout(TimeUtil.CURRENT_TIME_SECOND_CACHE) || !transaction.getEntity().isValid()) {
                _TransactionSkipListMap.pollFirstEntry();
                _BizNode.clearSession(transaction.getTransactionKey());
            }
            else {
                break;
            }
        }
    }

    private void clearTransaction(long transactionKey) {
        _BizNode.clearSession(transactionKey);
        _TransactionSkipListMap.remove(transactionKey);
        convergentClearTransaction();
    }

    @Override
    public RESULT trial(ICommand cmd, IConnectMode.OPERATION_MODE mode) {
        if (mode.equals(IConnectMode.OPERATION_MODE.SYMMETRY)) {
            switch (cmd.getSerialNum()) {
                case X101_Close.COMMAND:
                case X104_ExchangeIdentity.COMMAND:
                    return RESULT.HANDLE;
            }
        }
        else if (mode.equals(OPERATION_MODE.ACCEPT_SERVER) || mode.equals(OPERATION_MODE.ACCEPT_SERVER_SSL)) {
            if (cmd.getSerialNum() == X101_Close.COMMAND)
                return RESULT.HANDLE;
        }
        else if (_BizNode.getMqServer().trial(cmd, mode).equals(RESULT.HANDLE)) return RESULT.HANDLE;
        return RESULT.IGNORE;
    }

}
