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
package com.tgx.zq.z.queen.io.ws.protocol.bean.control;

import com.tgx.zq.z.queen.io.ws.protocol.WsHandshake;

public class X200_HandShake
        extends
        WsHandshake
{
    public final static int COMMAND = 0x200;

    public X200_HandShake(String host, String secKey, int version) {
        super(new StringBuilder("GET /ws_service HTTP/1.1").append(CRLF)
                                                           .append("Host: ")
                                                           .append(host)
                                                           .append(CRLF)
                                                           .append("Upgrade: websocket")
                                                           .append(CRLF)
                                                           .append("Connection: Upgrade")
                                                           .append(CRLF)
                                                           .append("Sec-WebSocket-Key: ")
                                                           .append(secKey)
                                                           .append(CRLF)
                                                           .append("Origin: http://")
                                                           .append(host)
                                                           .append(CRLF)
                                                           .append("Sec-WebSocket-Protocol: z-push, z-chat")
                                                           .append(CRLF)
                                                           .append("Sec-WebSocket-Version: ")
                                                           .append(version)
                                                           .append(CRLF)
                                                           .append(CRLF)
                                                           .toString());
    }

    public X200_HandShake(String handshake) {
        super(handshake);
    }

    public X200_HandShake() {
        super(null);
    }

    @Override
    public int getSerialNum() {
        return COMMAND;
    }

}
