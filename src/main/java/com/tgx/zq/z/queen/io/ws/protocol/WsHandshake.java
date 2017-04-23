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

import com.tgx.zq.z.queen.io.inf.ICommand;

public abstract class WsHandshake
        implements
        ICommand
{

    public final static int SerialNum = 0x2FF;
    private final String    mMsg;
    private String          xMsg;

    public WsHandshake(String msg) {
        mMsg = msg;
    }

    @Override
    public int getPriority() {
        return QOS_00_NETWORK_CONTROL;
    }

    @Override
    public int getSuperSerialNum() {
        return SerialNum;
    }

    public String getMessage() {
        return mMsg != null ? mMsg : xMsg;
    }

    public byte getControl() {
        return WsFrame.frame_op_code_ctrl_handshake;
    }

    public byte[] getPayload() {
        return mMsg == null ? xMsg == null ? null : xMsg.getBytes() : mMsg.getBytes();
    }

    @Override
    public String toString() {
        return new StringBuilder(getClass().getSimpleName()).append("->")
                                                            .append(Integer.toHexString(getSerialNum()))
                                                            .append(CRLF)
                                                            .append(mMsg)
                                                            .toString();
    }

    @Override
    public int dataLength() {
        return mMsg != null ? mMsg.getBytes().length : xMsg != null ? xMsg.getBytes().length : 0;
    }

    public WsHandshake append(String x) {
        xMsg = xMsg == null ? x : xMsg + x;
        return this;
    }

    public WsHandshake ahead(String x) {
        xMsg = xMsg == null ? x : x + xMsg;
        return this;
    }

    @Override
    public byte[] encode() {
        return getPayload();
    }

    @Override
    public void dispose() {
        xMsg = null;
    }
}
