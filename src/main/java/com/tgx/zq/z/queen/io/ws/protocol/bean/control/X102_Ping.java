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
package com.tgx.zq.z.queen.io.ws.protocol.bean.control;

import com.tgx.zq.z.queen.io.ws.protocol.WsControl;
import com.tgx.zq.z.queen.io.ws.protocol.WsFrame;

/**
 * @author William.d.zk
 */
public class X102_Ping
        extends
        WsControl
{

    public final static int COMMAND = 0x102;

    public X102_Ping() {
        super();
        mCtrlCode = WsFrame.frame_op_code_ctrl_ping;
    }

    public X102_Ping(byte[] payload) {
        super(payload);
        mCtrlCode = WsFrame.frame_op_code_ctrl_ping;
    }

    @Override
    public int getSerialNum() {
        return COMMAND;
    }

    @Override
    public X102_Ping duplicate() {
        return new X102_Ping(getPayload());
    }
}
