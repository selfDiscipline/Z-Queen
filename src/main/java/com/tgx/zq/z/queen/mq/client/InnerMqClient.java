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
package com.tgx.zq.z.queen.mq.client;

import java.io.IOException;
import java.net.SocketAddress;
import java.nio.channels.AsynchronousSocketChannel;
import java.util.MissingResourceException;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.tgx.zq.z.queen.base.util.Configuration;
import com.tgx.zq.z.queen.base.util.IPParser;
import com.tgx.zq.z.queen.base.util.IoUtil;
import com.tgx.zq.z.queen.client.socket.aio.AioSingleClient;
import com.tgx.zq.z.queen.client.socket.aio.AioSingleClient.IChannelLogic;
import com.tgx.zq.z.queen.db.bdb.impl.BerkeleyDBEnv;
import com.tgx.zq.z.queen.io.impl.AioCreator;
import com.tgx.zq.z.queen.io.impl.AioSession;
import com.tgx.zq.z.queen.io.inf.ICommand;
import com.tgx.zq.z.queen.io.inf.IConnectActive;
import com.tgx.zq.z.queen.io.inf.IConnectMode;
import com.tgx.zq.z.queen.io.inf.IContext;
import com.tgx.zq.z.queen.io.inf.ISession;
import com.tgx.zq.z.queen.io.inf.ISessionCreator;
import com.tgx.zq.z.queen.io.inf.ISessionOption;
import com.tgx.zq.z.queen.io.ws.filter.WsControlFilter;
import com.tgx.zq.z.queen.io.ws.filter.ZCommandFilter;
import com.tgx.zq.z.queen.io.ws.protocol.WsContext;
import com.tgx.zq.z.queen.io.ws.protocol.bean.mq.X80_MqTopicReg;
import com.tgx.zq.z.queen.io.ws.protocol.bean.mq.X81_MqTopicRegResult;
import com.tgx.zq.z.queen.mq.operations.MQ_READ;

/**
 * @author William.d.zk
 */
class InnerMqClient
{
    private final static String   ConfigFileName = "MqClientConfig";
    private final BerkeleyDBEnv   _MqEnv;
    private final AioSingleClient _MqClient;
    protected Logger              log            = Logger.getLogger(getClass().getName());
    private String[]              MQ_SERVER_HA_ADDRESS_ARRAY;
    private String                BDB_ENV_HOME;
    private long                  BDB_CACHE_LIMIT;
    private String                LOCAL_BIND_HOST;
    private long                  XID;

    InnerMqClient(String... start_argv) throws IOException {
        parseParams(start_argv);
        _MqEnv = BerkeleyDBEnv.openBdbEnv(getConfig_MqClientBdbEnv(), getConfig_MqClientBdbCacheLimit()).open();
        _MqClient = new AioSingleClient(getConfig_XID(),
                                        IConnectMode.OPERATION_MODE.CONNECT_MQ,
                                        getConfig_LocalBindHost(),
                                        getConfig_MqServerHaAddress())
        {

            final AioCreator _Creator = new AioCreator()
            {
                @Override
                public IContext createContext(ISessionOption option, IConnectMode.OPERATION_MODE mode) {
                    return new WsContext(option, mode);
                }

                @Override
                public ISession createSession(AsynchronousSocketChannel socketChannel, IConnectActive active) throws IOException {
                    return new AioSession(socketChannel, active, this, this, _MqClient, MQ_READ.INSTANCE);
                }
            };

            @Override
            public ISessionCreator getCreator(SocketAddress address, IConnectMode.OPERATION_MODE mode) {
                return _Creator;
            }
        };

        _MqClient.getFilterChain().linkFront(new ZCommandFilter(command -> {
            switch (command) {
                case X80_MqTopicReg.COMMAND:
                    return new X80_MqTopicReg();
                case X81_MqTopicRegResult.COMMAND:
                    return new X81_MqTopicRegResult();
                default:
                    log.warning("command is not cluster consistentHandle: X" + Integer.toHexString(command).toUpperCase());
                    break;
            }
            return null;
        })).linkFront(new WsControlFilter());

    }

    private void parseParams(String[] args) {
        if (args == null || args.length == 0 || args[0] == null || "".equals(args[0])) { return; }
        for (String s : args) {
            if (s == null || "".equals(s)) { return; }
            String[] argv = s.split("--");
            int argc = 0;
            int nArgs = argv.length;

            while (argc < nArgs) {
                String thisArg = argv[argc++];
                switch (thisArg) {
                    case "":
                        break;
                    case "-bdb_env_home":
                        if (argc < nArgs) {
                            BDB_ENV_HOME = argv[argc++];
                        }
                        log.info("BDB_ENV_HOME: " + BDB_ENV_HOME);
                        break;
                    case "-bdb_cache_limit":
                        if (argc < nArgs) {
                            BDB_CACHE_LIMIT = IoUtil.readStorage(argv[argc++]);
                        }
                        log.info("BDB_CACHE_LIMIT: " + argv[argc]);
                        break;
                    case "-local_bind_host":
                        if (argc < nArgs) {
                            LOCAL_BIND_HOST = argv[argc++];
                        }
                        log.info("LOCAL_BIND_HOST: " + LOCAL_BIND_HOST);
                        break;
                    case "-mq_server_address_list":
                        if (argc < nArgs) {
                            String noSplit = argv[argc++];
                            String[] split = !noSplit.equals("") ? noSplit.split(",") : null;
                            if (split != null) for (String address : split)
                                if (address.equals("")
                                    || IPParser.parse(address) == null) throw new IllegalArgumentException("mq server remote address config error!");
                            MQ_SERVER_HA_ADDRESS_ARRAY = !noSplit.equals("") ? noSplit.split(",") : null;
                            log.info("MQ_SERVER_HA_ADDRESS_ARRAY: " + noSplit);
                        }
                        break;
                    case "-mq_xid":
                        if (argc < nArgs) {
                            XID = Integer.parseInt(argv[argc++], 16);
                            XID <<= 48;
                        }
                        log.info("XID: " + XID);
                        break;
                    case "help":
                        System.out.println("-bdb_env_home berkeley-db environment home path [string]"
                                           + "\r\n\t -bdb_cache_limit berkeley-db memory cache limit [string]"
                                           + "\r\n\t -local_bind_host message queue service node local bind address [string]"
                                           + "\r\n\t -mq_xid mq client unique identify [long]"
                                           + "\r\n\t -mq_server_address_list mq servers remote connect address array [string by split ',']");
                        System.out.println("ex: -bdb_env_home--./mq-client"
                                           + " -bdb_cache_limit--200MB"
                                           + " -local_bind_host--0.0.0.0:0"
                                           + " -mq_server_address_list--10.10.0.3:6225,10.10.0.4:6225"
                                           + " -mq_xid--0x4081");
                        break;
                    default:
                        log.warning("Unknown connection manager argument; " + thisArg + " use '--help' ");
                }
            }
        }
    }

    long getConfig_XID() {
        try {
            if (XID == 0) {
                XID = Configuration.readConfigHexInteger("MQ_XID", ConfigFileName);
                return XID <<= 48;
            }
            return XID;
        }
        catch (MissingResourceException |
               ClassCastException e) {
            log.log(Level.ALL, "XID hasn't been set", e);
            return 0;
        }
    }

    String getConfig_LocalBindHost() {
        try {
            return LOCAL_BIND_HOST == null ? LOCAL_BIND_HOST = Configuration.readConfigString("LOCAL_BIND_HOST", ConfigFileName)
                                           : LOCAL_BIND_HOST;
        }
        catch (MissingResourceException |
               ClassCastException e) {
            // ipv6 address ?
            return "0.0.0.0";
        }
    }

    long getConfig_MqClientBdbCacheLimit() {
        try {
            return BDB_CACHE_LIMIT == 0 ? BDB_CACHE_LIMIT = Configuration.readConfigStorage("BDB_CACHE_LIMIT", ConfigFileName)
                                        : BDB_CACHE_LIMIT;

        }
        catch (MissingResourceException |
               ClassCastException e) {
            return 1L << 23;
        }

    }

    String getConfig_MqClientBdbEnv() {
        try {
            return BDB_ENV_HOME == null ? BDB_ENV_HOME = Configuration.readConfigString("BDB_ENV_HOME", ConfigFileName) : BDB_ENV_HOME;
        }
        catch (MissingResourceException |
               ClassCastException e) {
            return "./mq-client";
        }
    }

    String[] getConfig_MqServerHaAddress() {
        try {

            if (MQ_SERVER_HA_ADDRESS_ARRAY == null) {
                String noSplit = Configuration.readConfigString("MQ_SERVER_HA_ADDRESS", ConfigFileName);
                String[] split = !noSplit.equals("") ? noSplit.split(",") : null;
                if (split != null) for (String address : split)
                    if (address.equals("")
                        || IPParser.parse(address) == null) throw new IllegalArgumentException("mq server remote address config error!");
                MQ_SERVER_HA_ADDRESS_ARRAY = split;
            }
            return MQ_SERVER_HA_ADDRESS_ARRAY;
        }
        catch (MissingResourceException |
               ClassCastException e) {
            e.printStackTrace();
            return null;
        }
    }

    public void start() {
        _MqClient.start();
    }

    public void close() {
        if (_MqEnv != null) _MqEnv.close();
    }

    public void publish(ICommand content) {
        _MqClient.publish(content);
    }

    public void registerChannel(IChannelLogic logic) {
        _MqClient.addChannel(logic);
    }
}
