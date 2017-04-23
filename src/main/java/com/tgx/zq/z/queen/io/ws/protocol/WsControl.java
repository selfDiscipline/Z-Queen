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
package com.tgx.zq.z.queen.io.ws.protocol;

import com.tgx.zq.z.queen.base.inf.ISerialTick;
import com.tgx.zq.z.queen.io.inf.ICommand;
import com.tgx.zq.z.queen.io.inf.IRouteLv4;
import com.tgx.zq.z.queen.io.inf.ISession;

/**
 * @author William.d.zk
 */
public abstract class WsControl
        implements
        ICommand,
        IRouteLv4
{
    public final static int SerialNum = 0x1FF;
    protected final byte[]  mMsg;
    protected byte          mCtrlCode;
    private ISession        session;

    public WsControl(String msg) {
        mMsg = msg.getBytes();
    }

    public WsControl(byte[] msg) {
        mMsg = msg;
    }

    public WsControl() {
        mMsg = null;
    }

    public byte[] getPayload() {
        return mMsg == null ? null : mMsg;
    }

    @Override
    public int getPriority() {
        return QOS_00_NETWORK_CONTROL;
    }

    @Override
    public int getSuperSerialNum() {
        return SerialNum;
    }

    public byte getControl() {
        return mCtrlCode;
    }

    @Override
    public ISession getSession() {
        return session;
    }

    @Override
    public WsControl setSession(ISession session) {
        this.session = session;
        return this;
    }

    @Override
    public WsControl duplicate() {
        return null;
    }

    @Override
    public int dataLength() {
        return mMsg != null ? mMsg.length : 0;
    }

    @Override
    public String toString() {
        int command = getSerialNum();
        return "CMD: X" + (command < 0xF ? 0 : "") + Integer.toHexString(command).toUpperCase() + CRLF_TAB;
    }
}
