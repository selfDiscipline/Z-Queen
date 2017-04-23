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
package com.tgx.zq.z.queen.io.ws.filter;

import com.tgx.zq.z.queen.base.inf.ISerialTick;
import com.tgx.zq.z.queen.io.impl.AioFilterChain;
import com.tgx.zq.z.queen.io.inf.IContext.DecodeState;
import com.tgx.zq.z.queen.io.inf.IContext.EncodeState;
import com.tgx.zq.z.queen.io.inf.IProtocol;
import com.tgx.zq.z.queen.io.ws.protocol.Command;
import com.tgx.zq.z.queen.io.ws.protocol.WsContext;
import com.tgx.zq.z.queen.io.ws.protocol.WsControl;
import com.tgx.zq.z.queen.io.ws.protocol.WsFrame;
import com.tgx.zq.z.queen.io.ws.protocol.WsHandshake;
import com.tgx.zq.z.queen.io.ws.protocol.bean.control.X101_Close;
import com.tgx.zq.z.queen.io.ws.protocol.bean.control.X102_Ping;
import com.tgx.zq.z.queen.io.ws.protocol.bean.control.X103_Pong;
import com.tgx.zq.z.queen.io.ws.protocol.bean.control.X104_ExchangeIdentity;

public class WsControlFilter
        extends
        AioFilterChain<WsContext>
{
    public WsControlFilter() {
        name = "network-control-filter";
    }

    @Override
    public ResultType preEncode(WsContext context, IProtocol output) {
        if (context == null || output == null) return ResultType.ERROR;
        if (!context.getEncodeState().equals(EncodeState.ENCODING_FRAME)) return ResultType.IGNORE;
        if (!(output instanceof ISerialTick)) return ResultType.ERROR;
        ISerialTick tick = (ISerialTick) output;
        switch (tick.getSuperSerialNum()) {
            case Command.SerialNum:
                return ResultType.IGNORE;
            case WsControl.SerialNum:
                return ResultType.NEXT_STEP;
            case WsHandshake.SerialNum:
            case WsFrame.SerialNum:
                return ResultType.IGNORE;
            default:
                return ResultType.ERROR;
        }
    }

    @Override
    public WsFrame encode(WsContext context, IProtocol output) {
        WsFrame frame = new WsFrame();
        WsControl control = (WsControl) output;
        frame.setPayload(control.getPayload());
        frame.setCtrl(control.getControl());
        return frame;
    }

    @Override
    public ResultType preDecode(WsContext context, IProtocol input) {
        if (context == null || input == null) return ResultType.ERROR;
        if (!context.getDecodeState().equals(DecodeState.DECODING_FRAME)) return ResultType.IGNORE;
        if (!(input instanceof WsFrame)) return ResultType.ERROR;
        WsFrame frame = (WsFrame) input;
        if (frame.isNoCtrl()) return ResultType.IGNORE;
        WsFrame wsFrame = (WsFrame) input;
        switch (wsFrame.frame_op_code & 0x0F) {
            case WsFrame.frame_op_code_ctrl_close:
            case WsFrame.frame_op_code_ctrl_ping:
            case WsFrame.frame_op_code_ctrl_pong:
            case WsFrame.frame_op_code_ctrl_cluster:
                return ResultType.HANDLED;
            default:
                return ResultType.ERROR;
        }
    }

    @Override
    public WsControl decode(WsContext context, IProtocol input) {
        WsFrame wsFrame = (WsFrame) input;
        switch (wsFrame.frame_op_code & 0x0F) {
            case WsFrame.frame_op_code_ctrl_close:
                return new X101_Close(wsFrame.getPayload());
            case WsFrame.frame_op_code_ctrl_ping:
                return new X102_Ping(wsFrame.getPayload());
            case WsFrame.frame_op_code_ctrl_pong:
                return new X103_Pong(wsFrame.getPayload());
            case WsFrame.frame_op_code_ctrl_cluster:
                return new X104_ExchangeIdentity(wsFrame.getPayload());
            default:
                return null;
        }
    }
}
