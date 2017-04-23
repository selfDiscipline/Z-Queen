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

package com.tgx.zq.z.queen.io.ws.protocol.bean.ntp;

import com.tgx.zq.z.queen.base.util.IoUtil;
import com.tgx.zq.z.queen.io.ws.protocol.Command;
import com.tgx.zq.z.queen.io.ws.protocol.WsContext;

public class X0B_NTP_Response
        extends
        Command<WsContext>
{
    public final static int COMMAND = 0x0B;
    public long             reqArrivedTime;
    public long             time;

    public X0B_NTP_Response() {
        super(COMMAND, false);
    }

    @Override
    public int getPriority() {
        return QOS_14_NO_CONFIRM_MESSAGE;
    }

    @Override
    public int decodec(byte[] data, int pos) {
        reqArrivedTime = IoUtil.readLong(data, pos);
        pos += 8;
        time = IoUtil.readLong(data, pos);
        pos += 8;
        return pos;
    }

    @Override
    public int encodec(byte[] data, int pos) {
        pos += IoUtil.writeLong(reqArrivedTime, data, pos);
        pos += IoUtil.writeLong(time, data, pos);
        return pos;
    }

    @Override
    public void afterDecode(WsContext ctx) {
        ctx.ntp(0, reqArrivedTime, time, System.currentTimeMillis());
    }

    @Override
    public void afterEncode(WsContext ctx) {
        ctx.ntp(0, 0, time, -1);
    }

    @Override
    public int dataLength() {
        return super.dataLength();
    }

}
