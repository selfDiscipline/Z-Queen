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
package com.tgx.zq.z.queen.io.inf;

import java.io.IOException;
import java.net.StandardSocketOptions;
import java.nio.channels.AsynchronousSocketChannel;
import java.util.concurrent.TimeUnit;

import com.tgx.zq.z.queen.base.util.Configuration;

/**
 * @author William.d.zk
 */
public interface ISessionCreator
        extends
        ISessionOption,
        IContextCreator
{

    ISession createSession(AsynchronousSocketChannel socketChannel, IConnectActive active) throws IOException;

    @Override
    default int setSNF() {
        try {
            return Configuration.readConfigInteger(StandardSocketOptions.SO_SNDBUF.name(), "SocketOption");
        }
        catch (Exception e) {
            return INC_SEND_SIZE;
        }
    }

    @Override
    default int setRCV() {
        try {
            return Configuration.readConfigInteger(StandardSocketOptions.SO_RCVBUF.name(), "SocketOption");
        }
        catch (Exception e) {
            return INC_RECV_SIZE;
        }
    }

    @Override
    default void setOptions(AsynchronousSocketChannel channel) {
        if (channel != null) try {
            channel.setOption(StandardSocketOptions.TCP_NODELAY, true);
            channel.setOption(StandardSocketOptions.SO_RCVBUF, setRCV());
            channel.setOption(StandardSocketOptions.SO_SNDBUF, setSNF());
            channel.setOption(StandardSocketOptions.SO_KEEPALIVE, setKeepAlive());
        }
        catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    default int setQueueMax() {
        try {
            return Configuration.readConfigInteger("MAX_SEND_QUEUE_SIZE", "SocketOption");
        }
        catch (Exception e) {
            return INC_QUEUE_SIZE;
        }
    }

    @Override
    default int setReadTimeOut() {
        try {
            return Configuration.readConfigInteger("TCP_READ_TIMEOUT", "SocketOption");
        }
        catch (Exception e) {
            return (int) TimeUnit.MINUTES.toSeconds(15);
        }

    }

    @Override
    default int setWriteTimeOut() {
        try {
            return Configuration.readConfigInteger("TCP_WRITE_TIMEOUT", "SocketOption");
        }
        catch (Exception e) {
            return 30;
        }
    }

    @Override
    default boolean setKeepAlive() {
        try {
            return Configuration.readConfigBoolean("KEEP_ALIVE", "SocketOption");
        }
        catch (Exception e) {
            return false;
        }
    }
}
