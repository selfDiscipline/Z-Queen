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
package com.tgx.zq.z.queen.cluster.node;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.net.StandardSocketOptions;
import java.nio.channels.AsynchronousChannelGroup;
import java.nio.channels.AsynchronousServerSocketChannel;
import java.nio.channels.AsynchronousSocketChannel;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.MissingResourceException;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.logging.Level;

import com.lmax.disruptor.RingBuffer;
import com.tgx.zq.z.queen.base.classic.task.TaskService;
import com.tgx.zq.z.queen.base.classic.task.inf.ITaskResult;
import com.tgx.zq.z.queen.base.classic.task.timer.TimerTask;
import com.tgx.zq.z.queen.base.constant.QueenCode;
import com.tgx.zq.z.queen.base.disruptor.QEvent;
import com.tgx.zq.z.queen.base.disruptor.inf.IEventOp;
import com.tgx.zq.z.queen.base.util.Configuration;
import com.tgx.zq.z.queen.base.util.Pair;
import com.tgx.zq.z.queen.base.util.TimeUtil;
import com.tgx.zq.z.queen.biz.template.BizNode;
import com.tgx.zq.z.queen.cluster.db.bdb.impl.ConsistentDao;
import com.tgx.zq.z.queen.cluster.election.raft.ElectTimer;
import com.tgx.zq.z.queen.cluster.election.raft.RaftStage;
import com.tgx.zq.z.queen.cluster.election.raft.RaftStatus;
import com.tgx.zq.z.queen.cluster.election.raft.inf.IElector;
import com.tgx.zq.z.queen.cluster.inf.IBroadcast;
import com.tgx.zq.z.queen.cluster.operation.NODE_CONNECTED;
import com.tgx.zq.z.queen.cluster.operation.NODE_CONNECT_ERROR;
import com.tgx.zq.z.queen.cluster.operation.NODE_LOCAL;
import com.tgx.zq.z.queen.cluster.operation.NODE_READ;
import com.tgx.zq.z.queen.cluster.replication.bean.raft.LogEntry;
import com.tgx.zq.z.queen.cluster.replication.bean.raft.MetaEntry;
import com.tgx.zq.z.queen.cluster.replication.bean.raft.NodeEntity;
import com.tgx.zq.z.queen.db.bdb.inf.IBizDao;
import com.tgx.zq.z.queen.db.bdb.inf.IDbStorageProtocol;
import com.tgx.zq.z.queen.io.bean.XF000_NULL;
import com.tgx.zq.z.queen.io.bean.cluster.XF001_TransactionCompleted;
import com.tgx.zq.z.queen.io.bean.cluster.XF002_ClusterLocal;
import com.tgx.zq.z.queen.io.disruptor.AioAcceptor;
import com.tgx.zq.z.queen.io.disruptor.AioConnector;
import com.tgx.zq.z.queen.io.disruptor.ExecutorCore;
import com.tgx.zq.z.queen.io.disruptor.operations.ws.WRITE_OPERATOR;
import com.tgx.zq.z.queen.io.impl.AioCreator;
import com.tgx.zq.z.queen.io.impl.AioSession;
import com.tgx.zq.z.queen.io.impl.AioSessionManager;
import com.tgx.zq.z.queen.io.inf.IAioClient;
import com.tgx.zq.z.queen.io.inf.IAioServer;
import com.tgx.zq.z.queen.io.inf.ICommand;
import com.tgx.zq.z.queen.io.inf.IConnectActive;
import com.tgx.zq.z.queen.io.inf.IConnected;
import com.tgx.zq.z.queen.io.inf.IContext;
import com.tgx.zq.z.queen.io.inf.ISession;
import com.tgx.zq.z.queen.io.inf.ISessionCreated;
import com.tgx.zq.z.queen.io.inf.ISessionCreator;
import com.tgx.zq.z.queen.io.inf.ISessionDismiss;
import com.tgx.zq.z.queen.io.inf.ISessionOption;
import com.tgx.zq.z.queen.io.manager.QueenManager;
import com.tgx.zq.z.queen.io.ws.protocol.Command;
import com.tgx.zq.z.queen.io.ws.protocol.WsContext;
import com.tgx.zq.z.queen.io.ws.protocol.bean.cluster.raft.X10_StartElection;
import com.tgx.zq.z.queen.io.ws.protocol.bean.cluster.raft.X11_Ballot;
import com.tgx.zq.z.queen.io.ws.protocol.bean.cluster.raft.X12_AppendEntity;
import com.tgx.zq.z.queen.io.ws.protocol.bean.cluster.raft.X13_EntryAck;
import com.tgx.zq.z.queen.io.ws.protocol.bean.cluster.raft.X15_JointConsensus;
import com.tgx.zq.z.queen.io.ws.protocol.bean.cluster.raft.X16_ConfigAck;
import com.tgx.zq.z.queen.io.ws.protocol.bean.cluster.raft.X17_CommittedConfig;
import com.tgx.zq.z.queen.io.ws.protocol.bean.cluster.raft.X18_LeadLease;
import com.tgx.zq.z.queen.io.ws.protocol.bean.cluster.raft.X19_LeaseAck;
import com.tgx.zq.z.queen.io.ws.protocol.bean.cluster.raft.X1A_CommitEntry;
import com.tgx.zq.z.queen.io.ws.protocol.bean.cluster.raft.X1C_ClientEntry;
import com.tgx.zq.z.queen.io.ws.protocol.bean.control.X102_Ping;

public class ClusterNode<E extends IDbStorageProtocol, D extends IBizDao<E>, BN extends BizNode<E, D>>
        extends
        QueenManager
        implements
        IAioClient,
        IAioServer,
        ISessionCreated,
        ISessionDismiss,
        IElector<E>
{

    private final static String              ConfigFileName                 = "ClusterConfig";
    private final static String              sSocketOptionAttrFileName      = "SocketOption";
    private static final int                 _HEARTBEAT_TIMER_SERIAL_NUM    = TimerTask.SuperSerialNum + 399;
    private static long                      _XID;
    private static String                    _NODE_LOCAL_HOST;
    private static int                       _CLUSTER_PORT;
    private static String[]                  _NODE_PREVIOUS_LIST;
    private static int                       _AIO_PRODUCER_SIZE;
    private static int                       _PORTCHANNEL_CAPACITY;
    private final long                       _NodeId;
    private final int                        _BindSerial;
    private final ConsistentDao<E>           _Dao;
    private final D                          _BizDao;
    private final long[][]                   _PortChannelTimerTick;
    private final AtomicInteger              _Majority                      = new AtomicInteger(1);
    private final AtomicInteger              _NewMajority                   = new AtomicInteger(1);
    private final AtomicReference<RaftStage> _Stage                         = new AtomicReference<>(RaftStage.DISCOVER);
    private final Random                     _Random                        = new Random();
    private final Map<Long, NodeEntity>      _OldNodesStateMap              = new HashMap<>(1 << 8);
    private final Map<Long, NodeEntity>      _NewNodesStateMap              = new HashMap<>(1 << 8);
    private final Set<Long>                  _ElectionBallotSet             = new TreeSet<>();
    private final Set<Long>                  _NewElectionBallotSet          = new TreeSet<>();
    private final Map<Long, Integer>         _SlotNewMajorityAppendCountMap = new HashMap<>(1 << 8);
    private final Map<Long, Integer>         _SlotOldMajorityAppendCountMap = new HashMap<>(1 << 8);
    private InetSocketAddress                mLocalAddress;
    private AsynchronousServerSocketChannel  mServerChannel;
    private BN                               mBizNode;
    private final ISessionCreator            _Creator                       = new AioCreator(sSocketOptionAttrFileName)
                                                                            {
                                                                                @Override
                                                                                public IContext createContext(ISessionOption option,
                                                                                                              OPERATION_MODE mode) {
                                                                                    return new WsContext(option, mode);
                                                                                }

                                                                                @Override
                                                                                public AioSession createSession(AsynchronousSocketChannel socketChannel,
                                                                                                                IConnectActive active) throws IOException {
                                                                                    return new AioSession(socketChannel,
                                                                                                          active,
                                                                                                          this,
                                                                                                          this,
                                                                                                          ClusterNode.this,
                                                                                                          NODE_READ.INSTANCE);
                                                                                }

                                                                            };

    private long                             mTermId                        = -1L;
    private long                             mBallotId;
    private long                             mSlotIndex                     = -1L;
    private long                             mNextSlot;
    private long                             mLastCommittedTermId           = -1L;
    private long                             mLastCommittedSlotIndex        = -1L;
    private long                             mClientSlotIndex               = -1L;
    private long                             mClientNextSlot;
    private boolean                          mIsJointConsensus;
    private RaftStatus                       mStatus                        = RaftStatus.DISCOVER_WAIT;
    private volatile long                    vLeaseTimeout;
    private volatile long                    vLeaderLeaseTimeout;
    private volatile long                    vRandomWaitTimeout;
    private volatile long                    vNodeDiscoverTimeout;
    private volatile long                    vNextElectionRandomWaitTimeout;
    private volatile long                    vNodeDismissTimeout;
    private volatile int                     vBallotCount, vNewBallotCount;

    public ClusterNode(D bizDao) throws IOException {
        super(new HashMap<>(17), new HashMap<>(1 << 17));
        _NodeId = getConfig_XID();
        _BizDao = bizDao;
        _Dao = new ConsistentDao<>(bizDao.getEnv(), getIdentity());
        _PortChannelTimerTick = new long[2][getConfig_PortChannelCapacity()];
        setLocalAddress(new InetSocketAddress(getConfig_LocalHostAddress(), getConfig_ClusterPort()));
        _BindSerial = hashCode();
        ElectTimer.NODE_EXCHANGE_TIMEOUT = getConfig_ExchangeTimeout();
        ElectTimer.NODE_LEASE_TIMEOUT = getConfig_LeaseTimeout();
        TaskService.getInstance().addListener(this);
    }

    // -------------------------------------------------------------------------------------------------------------------/

    public static long getConfig_XID() {
        try {
            if (_XID == 0) {
                _XID = Configuration.readConfigHexInteger("XID", ConfigFileName);
                return _XID <<= 48;
            }
            return _XID;
        }
        catch (MissingResourceException |
               ClassCastException e) {
            log.log(Level.ALL, "XID hasn't been set", e);
            return 0;
        }
    }

    private static String getConfig_LocalHostAddress() {
        try {
            return _NODE_LOCAL_HOST == null ? _NODE_LOCAL_HOST = Configuration.readConfigString("NODE_LOCAL_HOST", ConfigFileName)
                                            : _NODE_LOCAL_HOST;
        }
        catch (MissingResourceException |
               ClassCastException e) {
            // ipv6 address ?
            return "0.0.0.0";
        }
    }

    private static int getConfig_ClusterPort() {
        try {
            return _CLUSTER_PORT == 0 ? _CLUSTER_PORT = Configuration.readConfigInteger("CLUSTER_PORT", ConfigFileName) : _CLUSTER_PORT;

        }
        catch (MissingResourceException |
               ClassCastException e) {
            log.log(Level.ALL, "cluster listen port error", e);
            return 0;
        }
    }

    private static String[] getConfig_PreviousNodesAddress() {
        try {
            if (_NODE_PREVIOUS_LIST == null) {
                String noSplit = Configuration.readConfigString("NODE_PREVIOUS_LIST", ConfigFileName);
                return _NODE_PREVIOUS_LIST = !noSplit.equals("") ? noSplit.split(",") : null;
            }
            return _NODE_PREVIOUS_LIST;
        }
        catch (MissingResourceException |
               ClassCastException e) {
            return null;
        }
    }

    private static long getConfig_ExchangeTimeout() {
        try {
            return Configuration.readConfigInteger("NODE_EXCHANGE_TIMEOUT", ConfigFileName);
        }
        catch (MissingResourceException |
               ClassCastException e) {
            log.log(Level.ALL, "exchange timeout set error", e);
            return 75;
        }
    }

    private static long getConfig_LeaseTimeout() {
        try {
            return Configuration.readConfigInteger("NODE_LEASE_TIMEOUT", ConfigFileName);
        }
        catch (MissingResourceException |
               ClassCastException e) {
            log.log(Level.ALL, "lease timeout set error", e);
            return 5000;
        }
    }

    public static int getConfig_AIO_ProducerSize() {
        try {
            return _AIO_PRODUCER_SIZE == 0 ? _AIO_PRODUCER_SIZE = Configuration.readConfigInteger("AIO_PRODUCER_SIZE", ConfigFileName)
                                           : _AIO_PRODUCER_SIZE;

        }
        catch (MissingResourceException |
               ClassCastException e) {
            log.log(Level.ALL, "cluster io producer size set error", e);
            return 2;
        }
    }

    public static int getConfig_PortChannelCapacity() {
        try {
            return _PORTCHANNEL_CAPACITY == 0 ? _PORTCHANNEL_CAPACITY = Configuration.readConfigInteger("PORTCHANNEL_CAPACITY",
                                                                                                        ConfigFileName)
                                              : _PORTCHANNEL_CAPACITY;

        }
        catch (MissingResourceException |
               ClassCastException e) {
            log.log(Level.ALL, "cluster io producer size set error", e);
            return 4;
        }
    }

    /*--------------------------------------------------------IElector--------------------------------------------------*/

    public static long getUniqueIdentity(long sequence) {
        return getConfig_XID() | TimeUtil.getUID16YearCollision2M() | (sequence & QueenCode.UID_SEQ_21_MK);
    }

    public static void parseParams(String[] args) {
        if (args == null || args.length == 0 || args[0] == null || "".equals(args[0])) return;
        for (String s : args) {
            if (s == null || "".equals(s)) return;
            String[] argv = s.split("--");
            int argc = 0;
            int nArgs = argv.length;

            while (argc < nArgs) {
                String thisArg = argv[argc++];
                switch (thisArg) {
                    case "":
                        break;
                    case "-cluster_port":
                        if (argc < nArgs) _CLUSTER_PORT = Integer.parseInt(argv[argc++]);
                        break;
                    case "-node_local":
                        if (argc < nArgs) _NODE_LOCAL_HOST = argv[argc++];
                        break;
                    case "-node_exchange_timeout":
                        ElectTimer.NODE_EXCHANGE_TIMEOUT = Integer.parseInt(argv[argc++]);
                        break;
                    case "-node_lease_timeout":
                        ElectTimer.NODE_LEASE_TIMEOUT = Integer.parseInt(argv[argc++]);
                        break;
                    case "-node_previous_list":
                        if (argc < nArgs) {
                            String noSplit = argv[argc++];
                            _NODE_PREVIOUS_LIST = !noSplit.equals("") ? noSplit.split(",") : null;
                        }
                        break;
                    case "-aio_producer_size":
                        _AIO_PRODUCER_SIZE = Integer.parseInt(argv[argc++]);
                        break;
                    case "-xid":
                        if (argc < nArgs) _XID = Long.parseLong(argv[argc++], 16) << 48;
                        break;
                    case "-portchannel_capacity":
                        if (argc < nArgs) _PORTCHANNEL_CAPACITY = Integer.parseInt(argv[argc++]);
                        break;
                    case "help":
                        // formatter:off
                        System.out.println("-xid node unique id [hex long]"
                                           + "\r\n\t -aio_producer_size cluster AIO channel group t-pool size [integer]"
                                           + "\r\n\t -cluster_port cluster listen port [integer]"
                                           + "\r\n\t -portchannel_capacity node connections size in portchannel [integer]"
                                           + "\r\n\t -node_previous_list cluster previous address [string by split ';']"
                                           + "\r\n\t -node_local cluster node local bind host [string]"
                                           + "\r\n\t -node_exchange_timeout cluster exchange time out [long/integer]"
                                           + "\r\n\t -node_lease_timeout cluster leader lease time out [long/integer]");
                        System.out.println("ex: -xid--89BF -node_previous_list--10.10.0.3,10.10.0.4 etc.");
                        // formatter:on
                        break;
                    default:
                        log.warning("Unknown argument; " + thisArg + " use '--help' ");
                        break;
                }
            }
        }
    }

    @Override
    public int getBindSerial() {
        return _BindSerial;
    }

    public final IBizDao<E> getBizDao() {
        return _BizDao;
    }

    public final BN getBizNode() {
        return mBizNode;
    }

    @Override
    public void bindAddress(InetSocketAddress address, AsynchronousChannelGroup channelGroup) throws IOException {
        mServerChannel = AsynchronousServerSocketChannel.open(channelGroup);
        mServerChannel.setOption(StandardSocketOptions.SO_REUSEADDR, true);
        mServerChannel.bind(address, 1 << 6);
    }

    @Override
    public OPERATION_MODE getMode() {
        return OPERATION_MODE.ACCEPT_CLUSTER;
    }

    @Override
    public boolean checkMode(OPERATION_MODE... modes) {
        for (OPERATION_MODE mode : modes)
            if (mode.equals(getMode())) return true;
        return false;
    }

    @Override
    public void connect(AioConnector connector) throws IOException {
        AsynchronousSocketChannel channel = AsynchronousSocketChannel.open(getChannelGroup());
        channel.connect(connector.getRemoteAddress(), channel, connector);
    }

    @Override
    public ISessionCreator getCreator(SocketAddress address, OPERATION_MODE mode) {
        return _Creator;
    }

    @Override
    public IConnectActive getConnectActive() {
        return this;
    }

    public <T extends ExecutorCore<E, D, BN>> void start(T core, BN bizNode) throws IOException {
        mBizNode = bizNode;
        initialize(core.getClusterAioProducerSize(), core.getClusterThreadFactory());
        TaskService.getInstance().startService();
        bindAddress(getLocalAddress(), getChannelGroup());
        pendingAccept();
        String[] previousNodes = getConfig_PreviousNodesAddress();
        if (previousNodes != null) {
            for (String address : previousNodes)
                sapConnect(address);
        }
        log.info("cluster node started!");
        initializeMetadata();
        waitDiscoverNode();
        clusterHeartbeat();
    }

    private void initializeMetadata() {
        MetaEntry metaEntry = _Dao.loadMetaData();
        if (metaEntry == null) _Dao.updateMeta();
        else {
            mBallotId = metaEntry.ballotId;
            mSlotIndex = metaEntry.slotIndex;
            mNextSlot = mSlotIndex + 1;
            mTermId = metaEntry.termId;
            mLastCommittedTermId = metaEntry.lastCommittedTermId;
            mLastCommittedSlotIndex = metaEntry.lastCommittedSlotIndex;
            mClientSlotIndex = metaEntry.lastClientSlotIndex;
            mClientNextSlot = mClientSlotIndex + 1;
        }
        log.info(toString());
    }

    @Override
    public void pendingAccept() {
        if (mServerChannel.isOpen()) mServerChannel.accept(this, AioAcceptor.CLUSTER);
    }

    @Override
    public InetSocketAddress getLocalAddress() {
        return mLocalAddress;
    }

    @Override
    public void setLocalAddress(InetSocketAddress address) {
        mLocalAddress = address;
    }

    @SuppressWarnings("unchecked")
    @Override
    public IEventOp<Pair<IConnected, OPERATION_MODE>, AsynchronousSocketChannel> createNormal() {
        return NODE_CONNECTED.INSTANCE;
    }

    @SuppressWarnings("unchecked")
    @Override
    public IEventOp<Pair<ClusterNode<?, ?, ?>, Throwable>, IConnectActive> createError() {
        return NODE_CONNECT_ERROR.INSTANCE;
    }

    @Override
    public void onError(IConnectActive active) {
        switch (active.getMode()) {
            case CONNECT_CLUSTER:
                AioConnector connector = (AioConnector) active;
                int oldPortIndex = connector.getPortIndex();
                connector.setRetry();
                _PortChannelTimerTick[0][oldPortIndex] = TaskService.getInstance()
                                                                    .requestTimerService(connector.new NoHaTimer(ElectTimer.NODE_CONNECT_RETRY_WAIT_MIN
                                                                                                                 + (_Random.nextInt()
                                                                                                                    & Integer.MAX_VALUE)
                                                                                                                   % (ElectTimer.NODE_CONNECT_RETRY_WAIT_MAX
                                                                                                                      - ElectTimer.NODE_CONNECT_RETRY_WAIT_MIN),
                                                                                                                 _PortChannelTimerTick[0]),
                                                                                         getBindSerial());
                break;
            default:
                break;

        }
    }

    private long getNewMsgUid() {
        return getIdentity() | TimeUtil.getUID32YearCollision1M();
    }

    @Override
    public void sendAll(List<ICommand> wList, Command<WsContext> cmd) {
        long[][] c = portIndex(QueenCode.XID_MK & getIdentity());
        if (c != null && c.length > 1 && c[1] != null && c[1].length > 0) {
            for (long _port : c[1]) {
                if (_port != 0) {
                    log.info("++++@" + Long.toHexString(_port).toUpperCase());
                    Command<WsContext> duplicateCmd = cmd.duplicate();
                    duplicateCmd.setPortIdx(_port);
                    ISession cSession = findSessionByPort(_port);
                    if (cSession != null && wList != null) {
                        log.finer("send " + cmd + " -> " + cSession);
                        duplicateCmd.setSession(cSession).setCluster(true);
                        wList.add(duplicateCmd);
                    }
                }
            }
        }
    }

    @Override
    public void sendDirect(List<ICommand> wList, Command<WsContext> cmd, long nodeId) {
        long _port = nodeId;
        log.info("++++@" + Long.toHexString(_port).toUpperCase());
        Command<WsContext> duplicateCmd = cmd.duplicate();
        duplicateCmd.setPortIdx(_port);
        ISession cSession = findSessionByPort(_port);
        if (cSession != null && wList != null) {
            log.finer("send " + cmd + " -> " + cSession);
            duplicateCmd.setSession(cSession).setCluster(true);
            wList.add(duplicateCmd);
        }
    }

    @Override
    public void sendExcept(List<ICommand> wList, Command<WsContext> cmd, long nodeId) {
        long[][] c = portIndex(QueenCode.XID_MK & getIdentity());
        if (c != null && c.length > 1 && c[1] != null && c[1].length > 0) {
            for (long _port : c[1]) {

                if (_port != 0 && _port != nodeId) {
                    log.info("++++@" + Long.toHexString(_port).toUpperCase());
                    Command<WsContext> duplicateCmd = cmd.duplicate();
                    duplicateCmd.setPortIdx(_port);
                    ISession cSession = findSessionByPort(_port);
                    if (cSession != null && wList != null) {
                        log.finer("send " + cmd + " -> " + cSession);
                        duplicateCmd.setSession(cSession).setCluster(true);
                        wList.add(duplicateCmd);
                    }
                }
            }
        }
    }

    @Override
    public void flush(List<ICommand> wList) {
        if (wList.isEmpty()) return;
        RingBuffer<QEvent> localSendBuf = getLocalSendBuffer();
        for (ICommand cmd : wList)
            publish(localSendBuf, IEventOp.Type.WRITE, cmd, cmd.getSession(), WRITE_OPERATOR.PLAIN_SYMMETRY);
    }

    private void localEvent(XF002_ClusterLocal raftLocal) {
        publish(getLocalBackBuffer(), IEventOp.Type.LOCAL, raftLocal, ClusterNode.this, NODE_LOCAL.INSTANCE);
    }

    private <V, A> void publish(RingBuffer<QEvent> publisher, IEventOp.Type t, V v, A a, IEventOp<V, A> operator) {
        long sequence = publisher.next();
        try {
            QEvent event = publisher.get(sequence);
            event.produce(t, v, a, operator);
        }
        finally {
            publisher.publish(sequence);
        }
    }

    private void clusterHeartbeat() {
        TaskService.getInstance().requestService(new TimerTask(ElectTimer.NODE_LEADER_LEASE_SAP >> 1, TimeUnit.MILLISECONDS)
        {
            @Override
            protected boolean doTimeMethod() {
                localEvent(new XF002_ClusterLocal(X102_Ping.COMMAND));
                return false;
            }

            @Override
            public int getSerialNum() {
                return _HEARTBEAT_TIMER_SERIAL_NUM;
            }
        }, getBindSerial());
    }

    @Override
    public String toString() {
        String CRLF = "\r\n", CRLFTAB = "\r\n\t";
        StringBuilder sb = new StringBuilder(CRLF);
        sb.append("ClusterNode:")
          .append(Long.toHexString(getIdentity()))
          .append(" Majority: ")
          .append(_Majority.get())
          .append(" New-Majority: ")
          .append(_NewMajority.get())
          .append(" Stage: ")
          .append(_Stage.get().name())
          .append(" Status: ")
          .append(mStatus.name())
          .append(CRLFTAB)
          .append(" BallotId: ")
          .append(mBallotId)
          .append(CRLFTAB)
          .append(" SlotIndex: ")
          .append(mSlotIndex)
          .append(CRLFTAB)
          .append(" NextSlot: ")
          .append(mNextSlot)
          .append(CRLFTAB)
          .append(" TermId: ")
          .append(mTermId)
          .append(CRLFTAB)
          .append(" LastCommittedTermId: ")
          .append(mLastCommittedTermId)
          .append(CRLFTAB)
          .append(" LastCommittedSlotIndex: ")
          .append(mLastCommittedSlotIndex)
          .append(CRLFTAB)
          .append(" ClientSlotIndex: ")
          .append(mClientSlotIndex)
          .append(CRLF);

        return sb.toString();
    }

    @Override
    public final long getIdentity() {
        return _NodeId;
    }

    @Override
    public long getCurrentTermId() {
        return mTermId;
    }

    @Override
    public long getSlotIndex() {
        return mSlotIndex;
    }

    @Override
    public void setTermId(long termId) {
        mTermId = _Dao.updateMetaTermId(termId);
    }

    @Override
    public long getBallotId() {
        return mBallotId;
    }

    @Override
    public long[] getOldConfig() {
        if (_OldNodesStateMap.isEmpty()) return new long[] { getIdentity() };
        long[] oldConfig = new long[_OldNodesStateMap.size() + 1];
        oldConfig[0] = getIdentity();
        int i = 1;
        for (long nodeId : _OldNodesStateMap.keySet())
            oldConfig[i++] = nodeId;
        return oldConfig;
    }

    @Override
    public long[] getNewConfig() {
        if (_NewNodesStateMap.isEmpty()) return new long[] { getIdentity() };
        long[] newConfig = new long[_NewNodesStateMap.size() + 1];
        newConfig[0] = getIdentity();
        int i = 1;
        for (long nodeId : _NewNodesStateMap.keySet())
            newConfig[i++] = nodeId;
        return newConfig;
    }

    public boolean checkConfigConsistent() {
        boolean consistent = true;
        Map<Long, NodeEntity> superSet = _OldNodesStateMap.size() > _NewNodesStateMap.size() ? _OldNodesStateMap : _NewNodesStateMap;
        Map<Long, NodeEntity> subSet = _OldNodesStateMap.size() > _NewNodesStateMap.size() ? _NewNodesStateMap : _OldNodesStateMap;
        for (long nodeId : superSet.keySet())
            if (!subSet.containsKey(nodeId)) return false;
        return consistent;
    }

    private void onDiscoverTimeout() {
        log.info("discover time out on leader-> to joint consensus");
        localEvent(new XF002_ClusterLocal(X15_JointConsensus.COMMAND));
    }

    @Override
    public void waitDiscoverNode() {
        ElectTimer timer = new ElectTimer(getCurrentStage(), this, ElectTimer.NODE_DISCOVER_TIMEOUT)
        {

            @Override
            public int getSerialNum() {
                return NODE_DISCOVER_TIMER;
            }
        };
        vNodeDiscoverTimeout = TaskService.getInstance().requestTimerService(timer, getBindSerial());
        log.info("Discover node timeout->@ " + TimeUtil.printTime(vNodeDiscoverTimeout));
    }

    @Override
    public void onClusterConnected(long identity) {
        NodeEntity nodeEntity = _OldNodesStateMap.get(identity);
        if (nodeEntity == null) {
            nodeEntity = _NewNodesStateMap.get(identity);
            if (nodeEntity == null) {
                _NewNodesStateMap.put(identity, nodeEntity = new NodeEntity(identity));
                waitDiscoverNode();
                changeStage(RaftStage.DISCOVER, RaftStage.FOLLOWER);
            }
        }
        nodeEntity.sessionIncrement();
    }

    private void sapConnect(String address) {
        for (int i = 0, size = getConfig_PortChannelCapacity(); i < size; i++) {
            AioConnector connector = new AioConnector(this, OPERATION_MODE.CONNECT_CLUSTER, getConfig_LocalHostAddress(), 0, i, address);
            _PortChannelTimerTick[0][i] = TaskService.getInstance()
                                                     .requestTimerService(connector.new NoHaTimer((_Random.nextInt() & Integer.MAX_VALUE)
                                                                                                  % ElectTimer.NODE_CONNECT_SAP,
                                                                                                  _PortChannelTimerTick[0]),
                                                                          getBindSerial());
        }
    }

    @Override
    public void onCreate(ISession session) {

    }

    @Override
    public void waitDismissNode() {
        ElectTimer timer = new ElectTimer(getCurrentStage(), this, ElectTimer.NODE_DISMISS_TIMEOUT)
        {
            @Override
            public int getSerialNum() {
                return NODE_DISMISS_TIMER;
            }
        };
        vNodeDismissTimeout = TaskService.getInstance().requestTimerService(timer, getBindSerial());
        log.info("Dismiss node timeout->@ " + TimeUtil.printTime(vNodeDismissTimeout));
    }

    @Override
    public void onDismiss(ISession session) {
        long _index = session.getIndex();
        if (_index != AioSessionManager.INVALID_INDEX) {
            if (session.getMode().equals(OPERATION_MODE.CONNECT_CLUSTER)) {
                int portIndex = session.getPortIndex();
                long averageAvailableTime = _PortChannelTimerTick[1][portIndex];
                long lastConnectTime = _PortChannelTimerTick[0][portIndex];
                _PortChannelTimerTick[1][portIndex] = averageAvailableTime > 0 ? (System.currentTimeMillis()
                                                                                  - lastConnectTime
                                                                                  + averageAvailableTime) >> 1
                                                                               : System.currentTimeMillis() - lastConnectTime;
                if (averageAvailableTime < session.getReadTimeOut() << 1) log.severe("session keep alive error,check network!");
                InetSocketAddress remoteAddress = session.getRemoteAddress();
                AioConnector connector = new AioConnector(this,
                                                          OPERATION_MODE.CONNECT_CLUSTER,
                                                          0,
                                                          portIndex,
                                                          remoteAddress.getHostString() + ":" + remoteAddress.getPort());
                _PortChannelTimerTick[0][portIndex] = TaskService.getInstance()
                                                                 .requestTimerService(connector.new NoHaTimer(ElectTimer.NODE_CONNECT_RETRY_WAIT_MIN
                                                                                                              + (_Random.nextInt()
                                                                                                                 & Integer.MAX_VALUE)
                                                                                                                % (ElectTimer.NODE_CONNECT_RETRY_WAIT_MAX
                                                                                                                   - ElectTimer.NODE_CONNECT_RETRY_WAIT_MIN),
                                                                                                              _PortChannelTimerTick[0]),
                                                                                      getBindSerial());
            }
            long nodeId = _index & QueenCode._IndexHighMask;
            NodeEntity nodeEntity = _OldNodesStateMap.get(nodeId);
            /* new config session */
            if (nodeEntity == null) nodeEntity = _NewNodesStateMap.get(nodeId);
            /*
             * nodeEntity in old config ,its session count isn't negative. nodeEntity in new config ,after its session count equals zero
             * that remove it
             */
            if (nodeEntity == null) log.severe("session is not in config old either new !");
            if (nodeEntity != null && nodeEntity.sessionDecrement()) _NewNodesStateMap.remove(nodeId);
            if (_OldNodesStateMap.containsKey(nodeId) && !_NewNodesStateMap.containsKey(nodeId)) waitDismissNode();
        }
    }

    private void commitNewConfig() {
        localEvent(new XF002_ClusterLocal(X17_CommittedConfig.COMMAND));
    }

    @Override
    public void setStatus(RaftStatus status) {
        this.mStatus = status;
    }

    @Override
    public void changeStage(RaftStage previous, RaftStage next) {
        if (stageLock(previous)) return;
        for (;;) {
            if (_Stage.get().equals(next) || _Stage.compareAndSet(previous, next)) break;
            if (!_Stage.get().equals(previous)) return;
        }
        switch (next) {
            case FOLLOWER:
                waitLeaderLease();
                break;
            case CANDIDATE:
                proposal();
                break;
            case LEADER:
                mBallotId = getIdentity();
                leaderLease();
            default:
                break;
        }
        log.info(toString());
    }

    @Override
    public void changeStatus(RaftStatus previous, RaftStatus next) {

    }

    @Override
    public boolean checkCurrentStage(RaftStage stage) {
        return _Stage.get().equals(stage);
    }

    @Override
    public long getLastCommittedSlotIndex() {
        return mLastCommittedSlotIndex;
    }

    @Override
    public long getLastCommittedTermId() {
        return mLastCommittedTermId;
    }

    @Override
    public RaftStage getCurrentStage() {
        return _Stage.get();
    }

    /*-----------------------------------------------------IClient------------------------------------------------------*/
    @Override
    public long getClientSlotIndex() {
        return mClientSlotIndex;
    }

    @Override
    public long getClientNextSlot() {
        return mClientNextSlot;
    }

    @Override
    public List<ICommand> sendEntry(IBroadcast<WsContext> broadcast, List<ICommand> wList, long leaderId, LogEntry<E> logEntity) {
        if (logEntity != null) {
            X1C_ClientEntry x1C = new X1C_ClientEntry(logEntity.idempotent, getIdentity(), getCurrentTermId(), getClientSlotIndex());
            x1C.setPayload(logEntity);
            broadcast.sendDirect(wList, x1C, leaderId);
            log.info("client: " + getIdentity() + "\tsend log entry to leader: " + leaderId + "\t" + logEntity.toString());
        }
        else log.info("client has nothing to send");
        return wList;
    }

    @Override
    public ICommand onReceiveEntryAck(long leaderId,
                                      long leaderTermId,
                                      long leaderSlotIndex,
                                      long leaderLastCommittedSlotIndex,
                                      long clientSlotIndex) {
        if (!checkConsistent(leaderTermId, leaderSlotIndex)) {
            X13_EntryAck x13 = new X13_EntryAck(getNewMsgUid(),
                                                getIdentity(),
                                                getCurrentTermId(),
                                                getSlotIndex(),
                                                false,
                                                isQualify(leaderSlotIndex));
            x13.nextIndex = getLastCommittedSlotIndex() + 1;
            return x13;

        }
        if (getLastCommittedSlotIndex() < leaderLastCommittedSlotIndex) {
            for (long slotIndex = getLastCommittedSlotIndex(); slotIndex <= leaderLastCommittedSlotIndex; slotIndex++)
                if (_Dao.commitEntry(slotIndex)) {
                    mLastCommittedSlotIndex = slotIndex;
                    mLastCommittedTermId = _Dao.getLastCommittedTerm(getIdentity());
                }
                else {
                    X13_EntryAck x13 = new X13_EntryAck(getNewMsgUid(),
                                                        getIdentity(),
                                                        getLastCommittedTermId(),
                                                        getLastCommittedSlotIndex(),
                                                        true,
                                                        false);
                    x13.nextIndex = slotIndex;
                    return x13;
                }
        }
        _Dao.transferLogEntry(clientSlotIndex, leaderSlotIndex, leaderTermId);
        setSlotIndex(leaderTermId, leaderSlotIndex);
        return new X13_EntryAck(getNewMsgUid(), getIdentity(), leaderTermId, leaderSlotIndex, true, true);
    }

    @Override
    public List<ICommand> consistentStorage(E storage, List<ICommand> wList) {
        if (wList == null) wList = new LinkedList<>();
        LogEntry<E> logEntry = new LogEntry<>(getCurrentTermId(), getClientNextSlot(), storage.getSecondaryLongKey());
        logEntry.setPayload(storage);
        log.info("create client entry:" + storage.toString());
        boolean sendEntry = false;
        switch (getCurrentStage()) {
            case FOLLOWER:
                sendEntry = true;
            case CANDIDATE:
                setStatus(RaftStatus.CLIENT_RECEIVED);
                mClientSlotIndex = _Dao.appendClientEntry(logEntry);
                mClientNextSlot = mClientSlotIndex + 1;
                if (sendEntry) sendEntry(this, wList, getBallotId(), logEntry);
                break;
            case LEADER:
                setStatus(RaftStatus.LEADER_APPEND);
                logEntry.slotIndex = getNextSlot();
                _Dao.appendEntry(logEntry);
                setSlotIndex(logEntry.termId, logEntry.slotIndex);
                broadcastEntry(this, wList, getIdentity(), getIdentity(), getLastCommittedSlotIndex(), logEntry);
                log.info("leader broadcast entry: " + logEntry.toString());
                break;
            default:
                break;
        }
        return wList;
    }

    /*-----------------------------------------------------ILeader------------------------------------------------------*/

    @Override
    public long getNextSlot() {
        return mNextSlot;
    }

    @Override
    public List<ICommand> onReceiveClientEntity(List<ICommand> wList, long followerId, LogEntry<E> entity) {
        if (wList == null) wList = new LinkedList<>();
        long clientSlotIndex = entity.slotIndex;
        log.info("leader receive from: " + followerId + "\r\n\tclient slot: " + clientSlotIndex);
        LogEntry<E> logEntry = _Dao.getEntryByIdempotent(entity.idempotent);

        if (logEntry != null && logEntry.isCommitted()) {
            X1A_CommitEntry x1A = new X1A_CommitEntry(getNewMsgUid(),
                                                      getIdentity(),
                                                      logEntry.getTermId(),
                                                      logEntry.getSlotIndex(),
                                                      entity.idempotent);
            log.info("leader committed: "
                     + getLastCommittedTermId()
                     + "\tslot: "
                     + getLastCommittedSlotIndex()
                     + " commit follower: "
                     + followerId);
            wList.add(x1A);

        }
        else {
            long slotIndex, termId;
            if (logEntry == null) {
                entity.termId = getCurrentTermId();
                entity.slotIndex = getNextSlot();
                _Dao.appendEntry(entity);
                setSlotIndex(entity.getTermId(), entity.getSlotIndex());
                log.info("leader append entry -> " + entity.toString());
                broadcastEntry(this, wList, getIdentity(), followerId, getLastCommittedSlotIndex(), entity);
                slotIndex = entity.getSlotIndex();
                termId = entity.getTermId();
            }
            else {
                slotIndex = logEntry.getSlotIndex();
                termId = logEntry.getTermId();
            }
            X13_EntryAck x13 = new X13_EntryAck(getNewMsgUid(), getIdentity(), termId, slotIndex, true, true);
            x13.clientSlotIndex = clientSlotIndex;
            x13.leaderAck = true;
            wList.add(x13);
        }

        return wList;
    }

    @Override
    public List<ICommand> broadcastEntry(IBroadcast<WsContext> broadcast,
                                         List<ICommand> wList,
                                         long leaderId,
                                         long clientId,
                                         long lastCommittedSlotIndex,
                                         LogEntry<E> entity) {
        X12_AppendEntity x12 = new X12_AppendEntity(getNewMsgUid(), leaderId, entity.termId, entity.slotIndex, lastCommittedSlotIndex);
        x12.setEntry(entity);
        broadcast.sendExcept(wList, x12, clientId);
        return wList;
    }

    @Override
    public List<ICommand> onReceiveEntryAck(List<ICommand> wList,
                                            long followerId,
                                            long termId,
                                            long slotIndex,
                                            long nextSlot,
                                            boolean accept,
                                            boolean qualify) {
        if (qualify) {
            if (accept) {
                if (mIsJointConsensus) {
                    boolean commitOld = false, commitNew = false;
                    if (slotIndex > getLastCommittedSlotIndex()) {
                        NodeEntity nodeEntity = _NewNodesStateMap.get(followerId);
                        if (nodeEntity.getAppendSlotIndex() < slotIndex) {
                            nodeEntity.setAppendSlotIndex(slotIndex);
                            int previous = _SlotNewMajorityAppendCountMap.get(slotIndex) == null ? 0
                                                                                                 : _SlotNewMajorityAppendCountMap.get(slotIndex);
                            if (previous + 1 < _NewMajority.get()) _SlotNewMajorityAppendCountMap.put(slotIndex, previous + 1);
                            else {
                                commitNew = true;
                                log.info("committed new config slot index:  " + slotIndex);
                            }
                        }
                        else commitNew = _SlotNewMajorityAppendCountMap.get(slotIndex) >= _NewMajority.get();

                        nodeEntity = _OldNodesStateMap.get(followerId);
                        if (nodeEntity.getAppendSlotIndex() < slotIndex) {
                            nodeEntity.setAppendSlotIndex(slotIndex);
                            int previous = _SlotOldMajorityAppendCountMap.get(slotIndex) == null ? 0
                                                                                                 : _SlotOldMajorityAppendCountMap.get(slotIndex);
                            if (previous + 1 < _Majority.get()) _SlotOldMajorityAppendCountMap.put(slotIndex, previous + 1);
                            else {
                                commitOld = true;
                                log.info("committed old config slot index:  " + slotIndex);
                            }

                        }
                        else commitOld = _SlotOldMajorityAppendCountMap.get(slotIndex) >= _Majority.get();
                        if (commitNew && commitOld) {
                            if (_Dao.commitEntry(slotIndex)) {
                                LogEntry<E> logEntry = _Dao.getEntryBySlotIndex(slotIndex);
                                Pair<Long, Long> update = _Dao.getLastCommitted(getIdentity());
                                mLastCommittedSlotIndex = update.first();
                                mLastCommittedTermId = update.second();
                                log.info("committed slot index: " + slotIndex);
                                broadcastCommit(this,
                                                wList,
                                                getIdentity(),
                                                logEntry.getTermId(),
                                                logEntry.getSlotIndex(),
                                                logEntry.idempotent);
                                if ((logEntry.idempotent & QueenCode._IndexHighMask) == getIdentity()) {
                                    log.info("log entry received in leader: " + getIdentity());
                                    wList.add(new XF001_TransactionCompleted(logEntry.getPayload().getPrimaryKey()));
                                }
                            }
                            else log.severe("commit log entry failed!");
                            _SlotOldMajorityAppendCountMap.remove(slotIndex);
                            _SlotNewMajorityAppendCountMap.remove(slotIndex);
                            return wList;
                        }

                    }
                    LogEntry<E> logEntry = _Dao.getEntryBySlotIndex(slotIndex);
                    wList.add(new X1A_CommitEntry(getNewMsgUid(), getIdentity(), termId, slotIndex, logEntry.idempotent));

                }
                else {
                    if (slotIndex > getLastCommittedSlotIndex()) {
                        NodeEntity nodeEntity = _OldNodesStateMap.get(followerId);
                        if (nodeEntity.getAppendSlotIndex() < slotIndex) {
                            nodeEntity.setAppendSlotIndex(slotIndex);
                            int previous = _SlotOldMajorityAppendCountMap.get(slotIndex) == null ? 0
                                                                                                 : _SlotOldMajorityAppendCountMap.get(slotIndex);
                            if (previous + 1 < _Majority.get()) _SlotOldMajorityAppendCountMap.put(slotIndex, previous + 1);
                            else {
                                if (_Dao.commitEntry(slotIndex)) {
                                    LogEntry<E> logEntry = _Dao.getEntryBySlotIndex(slotIndex);
                                    Pair<Long, Long> update = _Dao.getLastCommitted(getIdentity());
                                    mLastCommittedSlotIndex = update.first();
                                    mLastCommittedTermId = update.second();
                                    log.info("committed slot index: " + slotIndex);
                                    broadcastCommit(this,
                                                    wList,
                                                    getIdentity(),
                                                    logEntry.getTermId(),
                                                    logEntry.getSlotIndex(),
                                                    logEntry.idempotent);
                                    if ((logEntry.idempotent & QueenCode._IndexHighMask) == getIdentity()) {
                                        log.info("log entry received in leader: " + getIdentity());
                                        wList.add(new XF001_TransactionCompleted(logEntry.getPayload().getPrimaryKey()));
                                    }
                                }
                                else log.severe("commit log entry failed!");
                                _SlotOldMajorityAppendCountMap.remove(slotIndex);
                                return wList;
                            }
                        }
                    }
                    LogEntry<E> logEntry = _Dao.getEntryBySlotIndex(slotIndex);
                    wList.add(new X1A_CommitEntry(getNewMsgUid(), getIdentity(), termId, slotIndex, logEntry.idempotent));
                }
            }
            else {
                log.info("reject from :" + followerId + "\r\n\t" + "term: " + termId + "\tslot: " + slotIndex);
                revertFollower();
            }
        }
        else {
            LogEntry<E> rsyncEntry = _Dao.getEntryBySlotIndex(nextSlot);
            if (rsyncEntry == null) log.severe("leader: "
                                               + getIdentity()
                                               + "\tfollower: "
                                               + followerId
                                               + " RSync failed next slot: "
                                               + nextSlot
                                               + "\tno entry");

            else {
                X12_AppendEntity x12 = new X12_AppendEntity(getNewMsgUid(),
                                                            getIdentity(),
                                                            rsyncEntry.getTermId(),
                                                            rsyncEntry.getSlotIndex(),
                                                            getLastCommittedSlotIndex());
                x12.setEntry(rsyncEntry);
                wList.add(x12);
            }

        }
        return wList;
    }

    @Override
    public List<ICommand> broadcastCommit(IBroadcast<WsContext> broadcast,
                                          List<ICommand> wList,
                                          long leaderId,
                                          long currentTermId,
                                          long slotIndex,
                                          long idempotent) {
        broadcast.sendAll(wList, new X1A_CommitEntry(getNewMsgUid(), leaderId, currentTermId, slotIndex, idempotent));
        return wList;
    }

    @Override
    public void revertFollower() {
        changeStage(RaftStage.LEADER, RaftStage.FOLLOWER);
        mBallotId = _Dao.updateMetaBallot(0);
    }

    @Override
    public List<ICommand> lease(IBroadcast<WsContext> broadcast,
                                List<ICommand> wList,
                                long leaderId,
                                long termId,
                                long lastCommittedSlotIndex) {
        X18_LeadLease x18 = new X18_LeadLease(getNewMsgUid(), leaderId, termId, lastCommittedSlotIndex);
        broadcast.sendAll(wList, x18);
        setStatus(RaftStatus.LEADER_LEASE);
        return wList;
    }

    private void leaderLease() {
        localEvent(new XF002_ClusterLocal(X18_LeadLease.COMMAND));
        nextLease();
    }

    @Override
    public void nextLease() {
        ElectTimer timer = new ElectTimer(RaftStage.LEADER, this, ElectTimer.NODE_LEADER_LEASE_SAP)
        {
            @Override
            public int getSerialNum() {
                return LEADER_LEASE_TIMER;
            }
        };
        vLeaderLeaseTimeout = TaskService.getInstance().requestTimerService(timer, getBindSerial());
    }

    @Override
    public List<ICommand> jointConsensus(IBroadcast<WsContext> broadcast,
                                         List<ICommand> wList,
                                         long localNodeId,
                                         long currentTermId,
                                         long slotIndex,
                                         long lastCommittedSlotIndex,
                                         long[] oldConfig,
                                         long[] newConfig) {

        LogEntry<E> logEntry = new LogEntry<>();
        logEntry.termId = currentTermId;
        logEntry.slotIndex = slotIndex;
        _Dao.jointConsensus(logEntry, oldConfig, newConfig);
        mIsJointConsensus = true;
        X15_JointConsensus x15 = new X15_JointConsensus(getNewMsgUid(), localNodeId, currentTermId, slotIndex, lastCommittedSlotIndex);
        x15.setOldConfig(oldConfig);
        x15.setNewConfig(newConfig);
        broadcast.sendAll(wList, x15);
        return wList;
    }

    @Override
    public List<ICommand> configConsensus(IBroadcast<WsContext> broadcast,
                                          List<ICommand> wList,
                                          long nodeId,
                                          long termId,
                                          long slotIndex,
                                          long[] newConfig) {
        _Dao.appendEntry(new LogEntry<>(termId, slotIndex, getNewMsgUid()));
        setSlotIndex(termId, slotIndex);
        mIsJointConsensus = false;
        X17_CommittedConfig x17 = new X17_CommittedConfig(getNewMsgUid(), nodeId, termId, slotIndex);
        x17.setNewConfig(newConfig);
        wList.add(x17);
        return wList;
    }

    /*-----------------------------------------------------ICandidate---------------------------------------------------*/
    private void proposal() {
        localEvent(new XF002_ClusterLocal(X10_StartElection.COMMAND));
    }

    @Override
    public long newTerm() {
        if (!checkCurrentStage(RaftStage.CANDIDATE)) return mTermId;
        log.info("candidate new term");
        return this.mTermId = _Dao.updateMetaTermId(mTermId + 1);
    }

    @Override
    public ICommand onReceiveElection(long candidateId,
                                      long termId,
                                      long slotIndex,
                                      long lastCommittedTermId,
                                      long lastCommittedSlotIndex) {
        boolean accept = candidateId == getBallotId()
                         || termId > getCurrentTermId()
                         || lastCommittedSlotIndex > getLastCommittedSlotIndex()
                         || slotIndex > getSlotIndex();
        if (accept) revertFollower(candidateId);
        return new X11_Ballot(getNewMsgUid(), candidateId, termId, slotIndex, getBallotId(), accept);
    }

    @Override
    public void randomWaitNext() {
        ElectTimer timer = new ElectTimer(RaftStage.CANDIDATE,
                                          this,
                                          ElectTimer.NODE_ELECT_RANDOM_WAIT_MIN
                                                + (_Random.nextInt() & Integer.MAX_VALUE)
                                                  % (ElectTimer.NODE_ELECT_RANDOM_WAIT_MAX - ElectTimer.NODE_ELECT_RANDOM_WAIT_MIN))
        {
            @Override
            public int getSerialNum() {
                return NEXT_ELECT_RANDOM_WAIT_TIMER;
            }
        };

        vNextElectionRandomWaitTimeout = TaskService.getInstance().requestTimerService(timer, getBindSerial());
    }

    @Override
    public void onReceiveBallot(long nodeId, long termId, long slotIndex, long ballotId, boolean accept) {
        if (!checkCurrentStage(RaftStage.CANDIDATE) || ballotId != getIdentity()) return;
        if (termId > getCurrentTermId()) {
            changeStage(RaftStage.CANDIDATE, RaftStage.FOLLOWER);
            return;
        }
        if (mIsJointConsensus) {
            if (_ElectionBallotSet.remove(nodeId) && accept) vBallotCount++;
            if (_NewElectionBallotSet.remove(nodeId) && accept) vNewBallotCount++;
            if (vBallotCount >= _Majority.get()
                && vNewBallotCount >= _NewMajority.get()) changeStage(RaftStage.CANDIDATE, RaftStage.LEADER);
            else if (_OldNodesStateMap.size() - vBallotCount >= _Majority.get()
                     || _NewNodesStateMap.size() - vNewBallotCount >= _NewMajority.get()) randomWaitNext();
        }
        else {
            if (_ElectionBallotSet.remove(nodeId)) {
                if (accept) vBallotCount++;
                if (vBallotCount >= _Majority.get()) changeStage(RaftStage.CANDIDATE, RaftStage.LEADER);
                else if (_OldNodesStateMap.size() - vBallotCount >= _Majority.get()) randomWaitNext();
            }
        }
    }

    @Override
    public boolean stageLock(RaftStage stage) {
        switch (stage) {
            case CANDIDATE:
            case FOLLOWER:
            case LEADER:
            default:
                return false;
        }
    }

    @Override
    public List<ICommand> proposal(IBroadcast<WsContext> broadcast,
                                   List<ICommand> wList,
                                   long candidateId,
                                   long termId,
                                   long slotIndex,
                                   long lastCommittedTermId,
                                   long lastCommittedSlotIndex) {
        if (checkCurrentStage(RaftStage.CANDIDATE)) {
            X10_StartElection x10 = new X10_StartElection(getNewMsgUid(),
                                                          candidateId,
                                                          termId,
                                                          slotIndex,
                                                          lastCommittedTermId,
                                                          lastCommittedSlotIndex,
                                                          mIsJointConsensus ? getNewConfig() : getOldConfig());
            x10.setCluster(true);

            Map<Long, NodeEntity> nodeStateMap = mIsJointConsensus ? _NewNodesStateMap : _OldNodesStateMap;
            _NewMajority.set(_NewNodesStateMap.size() / 2 + 1);
            _Majority.set(_OldNodesStateMap.size() / 2 + 1);
            if (nodeStateMap.isEmpty()) changeStage(RaftStage.CANDIDATE, RaftStage.LEADER);
            else broadcast.sendCollection(wList, x10, nodeStateMap.keySet());
        }
        return wList;
    }

    @Override
    public void revertFollower(long leaderId) {
        changeStage(RaftStage.CANDIDATE, RaftStage.FOLLOWER);
        _Dao.updateMetaBallot(leaderId);
        mBallotId = leaderId;
    }

    /*--------------------------------------------------------IFollower-------------------------------------------------*/
    @Override
    public void randomWait() {
        ElectTimer timer = new ElectTimer(RaftStage.CANDIDATE,
                                          this,
                                          ElectTimer.NODE_ELECT_RANDOM_WAIT_MIN
                                                + (_Random.nextInt() & Integer.MAX_VALUE)
                                                  % (ElectTimer.NODE_ELECT_RANDOM_WAIT_MAX - ElectTimer.NODE_ELECT_RANDOM_WAIT_MIN))
        {
            @Override
            public int getSerialNum() {
                return RANDOM_WAIT_TIMER;
            }
        };
        vRandomWaitTimeout = TaskService.getInstance().requestTimerService(timer, getBindSerial());
    }

    @Override
    public void waitLeaderLease() {
        ElectTimer timer = new ElectTimer(RaftStage.FOLLOWER, this, ElectTimer.NODE_LEASE_TIMEOUT)
        {
            @Override
            public int getSerialNum() {
                return LEASE_TIMER;
            }
        };
        vLeaseTimeout = TaskService.getInstance().requestTimerService(timer, getBindSerial());
    }

    @Override
    public boolean isQualify(long leaderSlotIndex) {
        return leaderSlotIndex <= mNextSlot;
    }

    @Override
    public boolean checkConsistent(long leaderTermId, long leaderSlotIndex) {
        return isQualify(leaderSlotIndex) && leaderTermId >= getCurrentTermId();
    }

    @Override
    public void setSlotIndex(long termId, long slotIndex) {
        mSlotIndex = slotIndex;
        mNextSlot = mSlotIndex + 1;
        mTermId = termId;
    }

    @Override
    public ICommand onReceiveEntity(long leaderId, long lastCommittedSlotIndex, LogEntry<E> entry) {
        revertFollower(leaderId);
        long leaderSlotIndex = entry.getSlotIndex();
        long leaderTermId = entry.getTermId();
        if (!checkConsistent(leaderTermId, leaderSlotIndex)) {
            X13_EntryAck x13 = new X13_EntryAck(getNewMsgUid(),
                                                getIdentity(),
                                                getCurrentTermId(),
                                                getSlotIndex(),
                                                false,
                                                isQualify(leaderSlotIndex));
            x13.nextIndex = getLastCommittedSlotIndex() + 1;
            return x13;
        }

        if (getLastCommittedSlotIndex() < lastCommittedSlotIndex) {
            for (long slotIndex = getLastCommittedSlotIndex(); slotIndex <= lastCommittedSlotIndex; slotIndex++)
                if (_Dao.commitEntry(slotIndex)) mLastCommittedSlotIndex = slotIndex;
                else {
                    X13_EntryAck x13 = new X13_EntryAck(getNewMsgUid(),
                                                        getIdentity(),
                                                        getLastCommittedTermId(),
                                                        getLastCommittedSlotIndex(),
                                                        true,
                                                        false);
                    x13.nextIndex = slotIndex;
                    return x13;
                }
        }
        _Dao.appendEntry(entry);
        setSlotIndex(leaderTermId, leaderSlotIndex);
        return new X13_EntryAck(getNewMsgUid(), getIdentity(), getCurrentTermId(), getSlotIndex(), true, true);
    }

    @Override
    public ICommand onReceivedJointConsensus(long identity,
                                             long leaderTermId,
                                             long leaderSlotIndex,
                                             long lastCommittedSlotIndex,
                                             long[] oldNodeConfig,
                                             long[] newNodeConfig) {

        if (!checkConsistent(leaderTermId, leaderSlotIndex)) {
            X16_ConfigAck x16 = new X16_ConfigAck(getNewMsgUid(),
                                                  getIdentity(),
                                                  getCurrentTermId(),
                                                  getSlotIndex(),
                                                  false,
                                                  isQualify(leaderSlotIndex));
            x16.nextIndex = getLastCommittedSlotIndex() + 1;
            return x16;
        }

        if (getLastCommittedSlotIndex() < lastCommittedSlotIndex) {
            for (long slotIndex = getLastCommittedSlotIndex(); slotIndex <= lastCommittedSlotIndex; slotIndex++)
                if (_Dao.commitEntry(slotIndex)) mLastCommittedSlotIndex = slotIndex;
                else {
                    X16_ConfigAck x16 = new X16_ConfigAck(getNewMsgUid(),
                                                          getIdentity(),
                                                          getLastCommittedTermId(),
                                                          getLastCommittedSlotIndex(),
                                                          true,
                                                          false);
                    x16.nextIndex = slotIndex;
                    return x16;
                }
        }
        LogEntry<E> entry = new LogEntry<>();
        entry.termId = leaderTermId;
        entry.slotIndex = leaderSlotIndex;
        _Dao.jointConsensus(entry, oldNodeConfig, newNodeConfig);
        setSlotIndex(leaderTermId, leaderSlotIndex);
        mIsJointConsensus = true;
        return new X16_ConfigAck(getNewMsgUid(), getIdentity(), getCurrentTermId(), getSlotIndex(), true, true);
    }

    @Override
    public ICommand onReceiveCommit(long leaderId, long termId, long slotIndex, long idempotent) {
        if (_Dao.commitEntry(slotIndex)) {
            if (mLastCommittedSlotIndex < slotIndex) {
                mLastCommittedSlotIndex = slotIndex;
                mLastCommittedTermId = termId;
            }
            LogEntry<E> logEntry = _Dao.clearClientEntryByIdempotent(idempotent);
            if (logEntry != null) {
                for (long clientSlot = logEntry.getSlotIndex();; clientSlot--)
                    if (_Dao.clearClientEntryBySlot(clientSlot) == null) break;
                log.info("client log entry: " + logEntry.toString());
                return new XF001_TransactionCompleted(logEntry.getPayload().getPrimaryKey());
            }
            else log.info("client log entry invalid. " + "committed term: " + termId + "\tslot: " + slotIndex);
        }
        return XF000_NULL.INSTANCE;
    }

    @Override
    public ICommand onReceiveLease(long leaderId, long leaderTermId, long leaderSlotIndex) {
        return new X19_LeaseAck(getNewMsgUid(), getIdentity(), getCurrentTermId(), getSlotIndex());
    }

    /*--------------------------------------------------------Timer-----------------------------------------------------*/
    @Override
    public boolean handleResult(ITaskResult result, TaskService service) {
        switch (result.getSerialNum()) {
            case ElectTimer.NET_TRANSPORT_TIMER:
                ElectTimer timer = (ElectTimer) result;
                if (timer.checkStage(RaftStage.CANDIDATE)) {

                }
                else if (timer.checkStage(RaftStage.LEADER)) {
                    // TODO set this follow
                }
                break;
            case ElectTimer.NEXT_ELECT_RANDOM_WAIT_TIMER:
                timer = (ElectTimer) result;
                if (timer.getDoTime() < vNextElectionRandomWaitTimeout || !timer.checkStage(getCurrentStage())) {
                    log.info("candidate random wait ignore! current stage: " + getCurrentStage());
                }
                else {
                    log.info("candidate random wait time out -> next proposal");
                    proposal();
                }
                break;
            case ElectTimer.LEASE_TIMER:
                timer = (ElectTimer) result;
                if (timer.getDoTime() < vLeaseTimeout) break;
                randomWait();
                break;
            case ElectTimer.LEADER_LEASE_TIMER:
                timer = (ElectTimer) result;
                if (timer.getDoTime() < vLeaderLeaseTimeout || !timer.checkStage(getCurrentStage())) break;
                leaderLease();
                break;
            case ElectTimer.RANDOM_WAIT_TIMER:
                timer = (ElectTimer) result;
                if (timer.getDoTime() < vRandomWaitTimeout || !timer.checkStage(getCurrentStage())) {
                    log.info("follower random wait ignore! current stage: " + getCurrentStage());
                }
                else {
                    log.info("follower random wait time out");
                    changeStage(RaftStage.FOLLOWER, RaftStage.CANDIDATE);
                }
                break;
            case ElectTimer.NODE_DISCOVER_TIMER:
                timer = (ElectTimer) result;
                if (timer.getDoTime() >= vNodeDiscoverTimeout) {
                    log.info("discover node time out!");
                    if (timer.checkStage(getCurrentStage()) && _NewNodesStateMap.isEmpty()) {
                        log.info("discover node -> first node start -> only one node ");
                        changeStage(RaftStage.DISCOVER, RaftStage.CANDIDATE);
                    }
                    else if (checkCurrentStage(RaftStage.LEADER)) {
                        onDiscoverTimeout();
                    }
                    else log.info("discover node -> my stage: " + getCurrentStage() + " :nodes= " + _NewNodesStateMap.size());
                }
                break;
            case ElectTimer.NODE_DISMISS_TIMER:
                timer = (ElectTimer) result;
                if (timer.getDoTime() >= vNodeDismissTimeout) {
                    if (checkCurrentStage(RaftStage.LEADER)) commitNewConfig();
                    else if (checkCurrentStage(RaftStage.FOLLOWER) && _NewNodesStateMap.isEmpty()) {
                        /*
                         * 如果当前 follower 是比较慢速的情况，将会出现 slotIndex 远落后于 leader 的情况 此时成为 leader 后需要 client 的重发已被确认返回的 slotIndex 的内容。
                         */
                        changeStage(RaftStage.FOLLOWER, RaftStage.LEADER);
                    }
                }
                break;
            default:
                return false;
        }
        return true;
    }

    @Override
    public boolean exCaught(ITaskResult task, TaskService service) {
        switch (task.getSerialNum()) {
            case _HEARTBEAT_TIMER_SERIAL_NUM:
                clusterHeartbeat();
                break;
            default:
                break;
        }
        return false;
    }

}
