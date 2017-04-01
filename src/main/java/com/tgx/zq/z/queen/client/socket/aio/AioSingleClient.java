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
package com.tgx.zq.z.queen.client.socket.aio;

import java.io.IOException;
import java.nio.channels.AsynchronousChannelGroup;
import java.nio.channels.AsynchronousSocketChannel;
import java.nio.channels.CompletionHandler;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentSkipListMap;
import java.util.concurrent.Executors;
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.tgx.zq.z.queen.base.classic.task.Task;
import com.tgx.zq.z.queen.base.classic.task.TaskService;
import com.tgx.zq.z.queen.base.classic.task.inf.ITaskListener;
import com.tgx.zq.z.queen.base.classic.task.inf.ITaskResult;
import com.tgx.zq.z.queen.base.classic.task.timer.TimerTask;
import com.tgx.zq.z.queen.base.constant.QueenCode;
import com.tgx.zq.z.queen.base.util.IoUtil;
import com.tgx.zq.z.queen.base.util.TimeUtil;
import com.tgx.zq.z.queen.io.bean.XF000_NULL;
import com.tgx.zq.z.queen.io.disruptor.AioConnector;
import com.tgx.zq.z.queen.io.impl.AioPackage;
import com.tgx.zq.z.queen.io.inf.IAioClient;
import com.tgx.zq.z.queen.io.inf.ICommand;
import com.tgx.zq.z.queen.io.inf.IConnectActive;
import com.tgx.zq.z.queen.io.inf.IConnectMode;
import com.tgx.zq.z.queen.io.inf.ICreatorFactory;
import com.tgx.zq.z.queen.io.inf.IFilterChain;
import com.tgx.zq.z.queen.io.inf.IPipeRead;
import com.tgx.zq.z.queen.io.inf.IPipeWrite;
import com.tgx.zq.z.queen.io.inf.ISession;
import com.tgx.zq.z.queen.io.inf.ISessionCreated;
import com.tgx.zq.z.queen.io.inf.ISessionCreator;
import com.tgx.zq.z.queen.io.inf.ISessionDismiss;
import com.tgx.zq.z.queen.io.ws.filter.WsFrameFilter;
import com.tgx.zq.z.queen.io.ws.protocol.WsContext;
import com.tgx.zq.z.queen.io.ws.protocol.bean.control.X101_Close;
import com.tgx.zq.z.queen.io.ws.protocol.bean.control.X102_Ping;
import com.tgx.zq.z.queen.io.ws.protocol.bean.control.X104_ExchangeIdentity;
import com.tgx.zq.z.queen.io.ws.protocol.bean.control.X200_HandShake;

public abstract class AioSingleClient
        implements
        ICreatorFactory,
        IAioClient,
        ISessionDismiss,
        ISessionCreated,
        IPipeWrite,
        ITaskListener
{
    protected Logger                          log               = Logger.getLogger(getClass().getSimpleName());
    private final int                         _BindSerial;
    private final String                      _ClientLocalAddress;
    private final String[]                    _ServerAddress;
    private final AsynchronousChannelGroup    _ChannelGroup;
    private final AtomicLong                  _Sequence         = new AtomicLong(-1);
    private final long[][][]                  _HaTimerTickArray;
    private final BlockingQueue<ICommand>     _PublishQueue     = new PriorityBlockingQueue<>();
    private final TaskService                 _TaskService      = TaskService.getInstance(true);
    private final IFilterChain<WsContext>     _Filter;
    private final int                         _PushTaskCode     = -4226;
    private final int                         _ConsumerTaskCode = -6224;
    private final long                        _Identity;
    private final Map<Integer, IChannelLogic> _LogicMap         = new ConcurrentSkipListMap<>();
    private final IConnectMode.OPERATION_MODE _Mode;

    public AioSingleClient(final long _XID, IConnectMode.OPERATION_MODE mode, String localBind, String... address) throws IOException {
        if (address == null || address.length == 0) throw new NullPointerException();
        _Filter = new WsFrameFilter();
        _BindSerial = hashCode();
        _Identity = _XID;
        _Mode = mode;
        _TaskService.addListener(this);
        _ClientLocalAddress = localBind;
        _ServerAddress = address;
        _HaTimerTickArray = new long[1][2][address.length];
        _ChannelGroup = AsynchronousChannelGroup.withThreadPool(Executors.newFixedThreadPool(3));
    }

    public IFilterChain<WsContext> getFilterChain() {
        return _Filter;
    }

    public IConnectMode.OPERATION_MODE getMode() {
        return _Mode;
    }

    public void start() {
        _TaskService.startService();
        _AioConnector connector = new _AioConnector(AioSingleClient.this, _Mode, _ClientLocalAddress, 0, getHaRemoteAddressArray());
        _HaTimerTickArray[0][0][0] = _TaskService.requestTimerService(connector.new Timer(2, _HaTimerTickArray[0][0]), getBindSerial());
    }

    @Override
    public void connect(AioConnector connector) throws IOException {
        log.info("connect->");
        AsynchronousSocketChannel aSocketChannel = AsynchronousSocketChannel.open(_ChannelGroup);
        aSocketChannel.connect(connector.getRemoteAddress(), aSocketChannel, connector);
    }

    public void close(ISession session) {
        synchronized (session) {
            try {
                session.close();
                onDismiss(session);
            }
            catch (IOException e) {
                e.printStackTrace();
            }
            finally {
                session.dispose();
            }
        }
    }

    public final void publish(ICommand content) {
        log.info("AioClient-Publish: " + content);
        if (content != null && content.getSerialNum() != XF000_NULL.COMMAND) {
            content.setSequence(_Sequence.incrementAndGet());
            _PublishQueue.offer(content);
        }
    }

    private ICommand take() {
        try {
            return _PublishQueue.take();
        }
        catch (InterruptedException e) {
            // ignore
        }
        return null;
    }

    private void startHeartBeat(final ISession _session) {
        _TaskService.requestService(new TimerTask(_session.getHeartBeatSap())
        {

            ISession session = _session;

            @Override
            public int getSerialNum() {
                return hashCode();
            }

            @Override
            protected boolean doTimeMethod() {
                if (session.isClosed()) return true;
                long delay = session.nextBeat();
                if (delay < 0) {
                    publish(new X102_Ping(null));
                    setWaitTime(TimeUnit.SECONDS.toMillis(session.getHeartBeatSap()));
                }
                else setWaitTime(delay);
                return false;
            }

            @Override
            public void dispose() {
                session = null;
                super.dispose();
            }
        }, false);
    }

    @Override
    public void onError(IConnectActive active) {
        _AioConnector connector = (_AioConnector) active;
        int haIndex;
        if (connector.canRetry(3)) {
            haIndex = connector.haSpan();
            _AioConnector haConnector = new _AioConnector(this, _Mode, _ClientLocalAddress, haIndex, getHaRemoteAddressArray());
            _HaTimerTickArray[0][0][haIndex] = _TaskService.requestTimerService(haConnector.new Timer(2, _HaTimerTickArray[0][0]),
                                                                                getBindSerial());
        }
        else {
            connector.setRetry();
            _HaTimerTickArray[0][0][haIndex = connector.getHaIndex()] = _TaskService.requestTimerService(connector.new Timer(2
                                                                                                                             * AioConnector.RETRY_FIB_RATIO[connector.getRetry()],
                                                                                                                             _HaTimerTickArray[0][0]),
                                                                                                         getBindSerial());
        }
        log.info("next time: " + TimeUtil.printTime(_HaTimerTickArray[0][0][haIndex]));
    }

    @Override
    public void onDismiss(ISession session) {
        if (session != null) {
            int haIndex = session.getHaIndex();
            long averageAvailableTime = _HaTimerTickArray[0][1][haIndex];
            long lastConnectTime = _HaTimerTickArray[0][0][haIndex];
            _HaTimerTickArray[0][1][haIndex] = averageAvailableTime > 0 ? (System.currentTimeMillis()
                                                                           - lastConnectTime
                                                                           + averageAvailableTime) >> 1
                                                                        : System.currentTimeMillis() - lastConnectTime;
            if (averageAvailableTime < session.getReadTimeOut() << 1) {
                log.severe("session keep alive error,check network!");
                haIndex = ((haIndex + 1) & QueenCode._HA_ROUTER_REMOTE_ADDRESS_INDEX_MASK) % _HaTimerTickArray[0][0].length;
            }
            _AioConnector haConnector = new _AioConnector(this, _Mode, "0.0.0.0:0", haIndex, getHaRemoteAddressArray());
            _HaTimerTickArray[0][0][haIndex] = TaskService.getInstance()
                                                          .requestTimerService(haConnector.new Timer(2, _HaTimerTickArray[0][0]),
                                                                               getBindSerial());
            log.info("next connect time: " + TimeUtil.printTime(_HaTimerTickArray[0][0][haIndex]));
        }
    }

    @Override
    public void onCreate(ISession session) {
        switch (session.getMode()) {
            case CONNECT_CONSUMER:
            case CONNECT_CONSUMER_SSL:
                log.info("connected , handshake ");
                publish(new X200_HandShake(session.getRemoteAddress().getHostName(), ((WsContext) session.getContext()).getSeKey(), 13));
                break;
            default:
                log.info("connected , register node identity");
                byte[] x104_payload = new byte[8];
                IoUtil.writeLong(IoUtil.writeInet4Addr(session.getLocalAddress().getAddress().getAddress(),
                                                       session.getLocalAddress().getPort())
                                 | _Identity,
                                 x104_payload,
                                 0);
                publish(new X104_ExchangeIdentity(x104_payload));
                break;
        }
        startHeartBeat(session);
        int haIndex = session.getHaIndex();
        _HaTimerTickArray[0][0][haIndex] += 1;

    }

    public void addChannel(IChannelLogic logic) {
        if (_LogicMap.get(logic.getChannelId()) != logic) {
            IChannelLogic preLogic = _LogicMap.put(logic.getChannelId(), logic);
            log.info("register channel: " + logic + " /pre: " + preLogic);
        }
        else log.info("same logic add: " + logic + " @" + logic.getChannelId());
    }

    @Override
    public String[] getHaRemoteAddressArray() {
        return _ServerAddress;
    }

    @Override
    public boolean handleResult(ITaskResult taskOrResult, TaskService service) {
        return true;
    }

    @Override
    public boolean exCaught(ITaskResult task, TaskService service) {
        switch (task.getSerialNum()) {
            case _PushTaskCode:
            case _ConsumerTaskCode:
                // TODO consistentHandle read message error
        }
        return true;
    }

    @Override
    public int getBindSerial() {
        return _BindSerial;
    }

    public interface IChannelLogic
    {
        Collection<ICommand> handle(ICommand inCommand);

        int getChannelId();

        default ICommand onConnectedServer() {
            return XF000_NULL.INSTANCE;
        }
    }

    private class _AioConnector
            extends
            AioConnector
    {

        _AioConnector(final IAioClient client,
                      IConnectMode.OPERATION_MODE mode,
                      String localAddressStr,
                      int haIndex,
                      String... remoteAddressStr) {
            super(client, mode, localAddressStr, haIndex, 0, remoteAddressStr);
        }

        @Override
        public void completed(Void result, AsynchronousSocketChannel channel) {
            ISession session = null;
            try {
                ISessionCreator creator = getCreator(getRemoteAddress(), getMode());
                session = creator.createSession(channel, this);
            }
            catch (Exception e) {
                log.log(Level.WARNING, "create session failed", e);
                try {
                    channel.close();
                }
                catch (Exception e1) {
                    e1.printStackTrace();
                }
                finally {
                    onError(this);
                }
                return;
            }

            if (session != null) {
                try {
                    session.readNext(new AioReadHandler());
                }
                catch (Exception e) {
                    log.log(Level.SEVERE, "session read next error", e);
                    close(session);
                    return;
                }
            }
            onCreate(session);
            if (!_LogicMap.isEmpty()) {
                for (IChannelLogic _Logic : _LogicMap.values())
                    publish(_Logic.onConnectedServer());
            }
            _TaskService.requestService(new WriteTask(session), false, getBindSerial());
            log.info("session create success");
        }

        @Override
        public void failed(Throwable exc, AsynchronousSocketChannel channel) {
            exc.printStackTrace();
            try {
                channel.close();
            }
            catch (Exception e) {
                log.log(Level.INFO, "connect failed", e);
            }
            finally {
                onError(this);
            }
        }

    }

    private class AioReadHandler
            implements
            CompletionHandler<Integer, ISession>,
            IPipeRead
    {

        @Override
        public void completed(Integer result, ISession session) {
            switch (result) {
                case -1:
                    log.info("read negative");
                    close(session);
                    break;
                case 0:
                    try {
                        session.readNext(this);
                    }
                    catch (Exception e) {
                        e.printStackTrace();
                        log.info("read zero -> next error");
                        close(session);
                    }
                    break;
                default:
                    try {
                        WsContext ctx = (WsContext) session.getContext();
                        ICommand[] commands = filterRead(new AioPackage(session.read(result)), _Filter, ctx);
                        if (commands != null && commands.length > 0) {
                            for (ICommand cmd : commands) {
                                if (cmd.getSerialNum() == X101_Close.COMMAND) throw new IOException("received close control");
                                int channel = cmd.getChannel();
                                IChannelLogic logic = _LogicMap.get(channel);
                                if (logic != null) {
                                    log.info("received: " + cmd + " -> " + logic);
                                    ReadTask readTask = new ReadTask(logic, cmd);
                                    _TaskService.requestTimerService(readTask, getBindSerial());
                                }
                                else log.warning("no register channel: " + channel + " | " + cmd + " dropped!");
                            }
                        }
                        session.readNext(this);
                    }
                    catch (Exception e) {
                        log.log(Level.WARNING, "session read ex: ", e);
                        close(session);
                    }
                    break;
            }
        }

        @Override
        public void failed(Throwable exc, ISession session) {
            exc.printStackTrace();
            log.info("read failed error");
            close(session);
        }
    }

    private class AioWriteHandler
            implements
            CompletionHandler<Integer, ISession>
    {
        @Override
        public void completed(Integer result, ISession session) {

            switch (result) {
                case -1:
                    log.info("write negative");
                    close(session);
                    break;
                default:
                    try {
                        session.writeNext(result, this);
                    }
                    catch (Exception e) {
                        e.printStackTrace();
                        log.info("write zero /-> next error");
                        close(session);
                    }
                    break;
            }

        }

        @Override
        public void failed(Throwable exc, ISession session) {
            exc.printStackTrace();
            log.info("write failed error");
            close(session);
        }
    }

    private class WriteTask
            extends
            Task
    {
        final ISession        session;
        final AioWriteHandler writeHandler = new AioWriteHandler();

        WriteTask(ISession session) {
            super(_PushTaskCode);
            this.session = session;
        }

        @Override
        public void initTask() {
            isBlocker = true;
            super.initTask();
        }

        @Override
        public void run() throws Exception {
            for (;;) {
                ICommand cmd = take();
                if (cmd == null) return;
                boolean error = session == null || session.isClosed();
                if (!error) {
                    if (cmd.getSerialNum() == XF000_NULL.COMMAND) continue;
                    try {
                        Throwable t = sessionWrite(cmd, _Filter, session, writeHandler);
                        if (t != null) {
                            log.log(Level.SEVERE, "session write error" + cmd, t);
                            close(session);
                            error = true;
                        }
                    }
                    catch (Exception e) {
                        log.log(Level.SEVERE, "encode error", e);
                        close(session);
                        error = true;
                    }
                }
                if (error) {
                    // 此处需要保持 SEQ 不变所以不使用 publish 方法
                    log.warning("write task error cmd retry -> queue" + "\r\n" + cmd);
                    _PublishQueue.offer(cmd);
                    return;
                }
            }
        }

        @Override
        public int getSerialNum() {
            return _PushTaskCode;
        }
    }

    public class ReadTask
            extends
            Task
    {
        final ICommand      _Body;
        final IChannelLogic _Logic;

        ReadTask(final IChannelLogic logic, ICommand inCommand) {
            super(logic.getChannelId());
            _Logic = logic;
            _Body = inCommand;
        }

        @Override
        public void initTask() {
            isBlocker = true;
            super.initTask();
        }

        @Override
        public int getSerialNum() {
            return _ConsumerTaskCode;
        }

        @Override
        public void run() throws Exception {
            Collection<ICommand> result = _Logic != null ? _Logic.handle(_Body) : null;
            if (result != null) for (ICommand out : result)
                publish(out);
        }
    }

}
