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
package com.tgx.zq.z.queen.io.impl;

import java.net.StandardSocketOptions;
import java.util.concurrent.TimeUnit;

import com.tgx.zq.z.queen.base.util.Configuration;
import com.tgx.zq.z.queen.io.inf.ISessionCreator;

public abstract class AioCreator
        implements
        ISessionCreator
{
    private final int     _SO_SNDBUF;
    private final int     _SO_RCVBUF;
    private final int     _MAX_SEND_QUEUE_SIZE;
    private final int     _TCP_READ_TIMEOUT;
    private final int     _TCP_WRITE_TIMEOUT;
    private final boolean _KEEP_ALIVE;
    private final String  _FILE_NAME;

    public AioCreator() {
        this("SocketOption");
    }

    public AioCreator(String fileName) {
        _FILE_NAME = fileName;
        _SO_SNDBUF = loadSNF();
        _SO_RCVBUF = loadRCV();
        _MAX_SEND_QUEUE_SIZE = loadQueueMax();
        _TCP_READ_TIMEOUT = loadReadTimeOut();
        _TCP_WRITE_TIMEOUT = loadWriteTimeOut();
        _KEEP_ALIVE = loadKeepAlive();
    }

    @Override
    public int setSNF() {
        return _SO_SNDBUF;
    }

    private int loadSNF() {
        try {
            return Configuration.readConfigInteger(StandardSocketOptions.SO_SNDBUF.name(), _FILE_NAME);
        }
        catch (Exception e) {
            return INC_SEND_SIZE;
        }
    }

    @Override
    public int setRCV() {
        return _SO_RCVBUF;
    }

    private int loadRCV() {
        try {
            return Configuration.readConfigInteger(StandardSocketOptions.SO_RCVBUF.name(), _FILE_NAME);
        }
        catch (Exception e) {
            return INC_RECV_SIZE;
        }
    }

    @Override
    public int setQueueMax() {
        return _MAX_SEND_QUEUE_SIZE;
    }

    private int loadQueueMax() {
        try {
            return Configuration.readConfigInteger("MAX_SEND_QUEUE_SIZE", _FILE_NAME);
        }
        catch (Exception e) {
            return INC_QUEUE_SIZE;
        }
    }

    @Override
    public int setReadTimeOut() {
        return _TCP_READ_TIMEOUT;
    }

    private int loadReadTimeOut() {
        try {
            return Configuration.readConfigInteger("TCP_READ_TIMEOUT", _FILE_NAME);
        }
        catch (Exception e) {
            return (int) TimeUnit.MINUTES.toSeconds(15);
        }

    }

    @Override
    public int setWriteTimeOut() {
        return _TCP_WRITE_TIMEOUT;
    }

    private int loadWriteTimeOut() {
        try {
            return Configuration.readConfigInteger("TCP_WRITE_TIMEOUT", _FILE_NAME);
        }
        catch (Exception e) {
            return 30;
        }
    }

    @Override
    public boolean setKeepAlive() {
        return _KEEP_ALIVE;
    }

    private boolean loadKeepAlive() {
        try {
            return Configuration.readConfigBoolean("KEEP_ALIVE", _FILE_NAME);
        }
        catch (Exception e) {
            return false;
        }
    }
}
