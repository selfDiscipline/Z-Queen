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

import java.nio.ByteBuffer;
import java.util.logging.Logger;

import com.tgx.zq.z.queen.io.impl.AioFilterChain;
import com.tgx.zq.z.queen.io.impl.AioPackage;
import com.tgx.zq.z.queen.io.inf.IConnectMode;
import com.tgx.zq.z.queen.io.inf.IContext;
import com.tgx.zq.z.queen.io.inf.IPoS;
import com.tgx.zq.z.queen.io.inf.IProtocol;
import com.tgx.zq.z.queen.io.ws.protocol.WsContext;
import com.tgx.zq.z.queen.io.ws.protocol.WsHandshake;
import com.tgx.zq.z.queen.io.ws.protocol.bean.control.X200_HandShake;
import com.tgx.zq.z.queen.io.ws.protocol.bean.control.X201_SslHandShake;

public class WsHandShakeFilter
        extends
        AioFilterChain<WsContext>
{
    private final String                      CRLF = "\r\n";
    private final IConnectMode.OPERATION_MODE _Mode;
    Logger                                    log  = Logger.getLogger(getClass().getSimpleName());

    public WsHandShakeFilter(IConnectMode.OPERATION_MODE mode) {
        name = "web-socket-header-filter-" + mode.name();
        _Mode = mode;
    }

    @Override
    public ResultType preEncode(WsContext context, IProtocol output) {
        if (context == null || output == null) return ResultType.ERROR;
        if (context.hasHandshake()
            && context.getEncodeState().equals(IContext.EncodeState.ENCODE_HANDSHAKE)
            && output instanceof WsHandshake) { return ResultType.NEXT_STEP; }
        return ResultType.IGNORE;
    }

    @Override
    public ResultType preDecode(WsContext context, IProtocol input) {
        if (context == null || input == null || !(input instanceof IPoS)) return ResultType.ERROR;
        if (context.hasHandshake() && context.getDecodeState().equals(IContext.DecodeState.DECODE_HANDSHAKE)) {
            WsHandshake handshake = context.getHandshake();
            ByteBuffer recvBuf = ((IPoS) input).getBuffer();
            ByteBuffer cRvBuf = context.getRvBuffer();
            byte c;
            while (recvBuf.hasRemaining()) {
                c = recvBuf.get();
                cRvBuf.put(c);
                if (c == '\n') {
                    cRvBuf.flip();
                    String x = new String(cRvBuf.array(), cRvBuf.position(), cRvBuf.limit());
                    log.info(x);
                    cRvBuf.clear();
                    switch (_Mode) {
                        case ACCEPT_SERVER:
                            if (handshake == null) handshake = new X200_HandShake();
                        case ACCEPT_SERVER_SSL:
                            if (handshake == null) handshake = new X201_SslHandShake();
                            context.setHandshake(handshake);
                            String[] split = x.split(" ", 2);
                            String httpKey = split[0].toUpperCase();
                            switch (httpKey) {
                                case "GET":
                                    split = x.split(" ");
                                    if (!split[2].equalsIgnoreCase("HTTP/1.1\r\n")) {
                                        log.warning("http protocol version is low than 1.1");
                                        return ResultType.ERROR;
                                    }
                                    context.updateHandshakeState(WsContext.HS_State_GET);
                                    break;
                                case "UPGRADE:":
                                    if (!split[1].equalsIgnoreCase("websocket\r\n")) {
                                        log.warning("upgrade no web-socket");
                                        return ResultType.ERROR;
                                    }
                                    context.updateHandshakeState(WsContext.HS_State_UPGRADE);
                                    handshake.append(x);
                                    break;
                                case "CONNECTION:":
                                    if (!split[1].equalsIgnoreCase("Upgrade\r\n")) {
                                        log.warning("connection no upgrade");
                                        return ResultType.ERROR;
                                    }
                                    context.updateHandshakeState(WsContext.HS_State_CONNECTION);
                                    handshake.append(x);
                                    break;
                                case "SEC-WEBSOCKET-PROTOCOL:":
                                    if (!split[1].contains("z-push") && !split[1].contains("z-chat")) {
                                        log.warning("sec-websokcet-protocol failed");
                                        return ResultType.ERROR;
                                    }
                                    context.updateHandshakeState(WsContext.HS_State_SEC_PROTOCOL);
                                    handshake.append(x);
                                    break;
                                case "SEC-WEBSOCKET-VERSION:":
                                    if (!split[1].contains("13")) {
                                        log.warning("sec-websokcet-version to low");
                                        return ResultType.ERROR;
                                    }
                                    else if (split[1].contains("7") || split[1].contains("8")) break;
                                    // TODO multi version response
                                    // Sec-WebSocket-Version: 13, 7, 8 ->
                                    // Sec-WebSocket-Version:13\r\nSec-WebSocket-Version: 7, 8\r\n
                                    context.updateHandshakeState(WsContext.HS_State_SEC_VERSION);
                                    break;
                                case "HOST:":
                                    context.updateHandshakeState(WsContext.HS_State_HOST);
                                    break;
                                case "ORIGIN:":
                                    context.updateHandshakeState(WsContext.HS_State_ORIGIN);
                                    break;
                                case "SEC-WEBSOCKET-KEY:":
                                    String sec_key = split[1].replace(CRLF, "");
                                    String sec_accept_expect = context.getSecAccept(sec_key);
                                    handshake.append("Sec-WebSocket-Accept: " + sec_accept_expect + CRLF);
                                    context.updateHandshakeState(WsContext.HS_State_SEC_KEY);
                                    break;
                                case CRLF:
                                    if (context.checkState(WsContext.HS_State_CLIENT_OK)) {
                                        context.setDecodeState(IContext.DecodeState.DECODING_FRAME);
                                        handshake.ahead("HTTP/1.1 101 Switching Protocols\r\n").append(CRLF);
                                    }
                                    else handshake.ahead("HTTP/1.1 400 Bad Request\r\n").append(CRLF);
                                    return ResultType.HANDLED;

                            }
                            break;
                        case CONNECT_CONSUMER:
                            if (handshake == null) handshake = new X200_HandShake();
                        case CONNECT_CONSUMER_SSL:
                            if (handshake == null) handshake = new X201_SslHandShake();
                            context.setHandshake(handshake);
                            split = x.split(" ", 2);
                            httpKey = split[0].toUpperCase();
                            switch (httpKey) {
                                case "HTTP/1.1":
                                    if (!split[1].contains("101 Switching Protocols\r\n")) {
                                        log.warning("handshake error !:");
                                        return ResultType.ERROR;
                                    }
                                    context.updateHandshakeState(WsContext.HS_State_HTTP_101);
                                    break;
                                case "UPGRADE:":
                                    if (!split[1].equalsIgnoreCase("websocket\r\n")) {
                                        log.warning("upgrade no web-socket");
                                        return ResultType.ERROR;
                                    }
                                    context.updateHandshakeState(WsContext.HS_State_UPGRADE);
                                    break;
                                case "CONNECTION:":
                                    if (!split[1].equalsIgnoreCase("Upgrade\r\n")) {
                                        log.warning("connection no upgrade");
                                        return ResultType.ERROR;
                                    }
                                    context.updateHandshakeState(WsContext.HS_State_CONNECTION);
                                    break;
                                case "SEC-WEBSOCKET-ACCEPT:":
                                    if (!split[1].startsWith(context.mSecAcceptExpect)) {
                                        log.warning("key error: expect-> " + context.mSecAcceptExpect + " | result-> " + split[1]);
                                        return ResultType.ERROR;
                                    }
                                    context.updateHandshakeState(WsContext.HS_State_SEC_ACCEPT);
                                    break;
                                case CRLF:
                                    if (context.checkState(WsContext.HS_State_ACCEPT_OK)) {
                                        context.setDecodeState(IContext.DecodeState.DECODING_FRAME);
                                        context.setEncodeState(IContext.EncodeState.ENCODING_FRAME);
                                        return ResultType.HANDLED;
                                    }
                                    log.warning("client handshake error!");
                                    return ResultType.ERROR;
                            }
                        default:
                            break;

                    }

                }

                if (!recvBuf.hasRemaining()) return ResultType.NEED_DATA;
            }

        }
        return ResultType.IGNORE;
    }

    @Override
    public IProtocol encode(WsContext context, IProtocol output) {
        if ((_Mode.equals(IConnectMode.OPERATION_MODE.ACCEPT_SERVER) || _Mode.equals(IConnectMode.OPERATION_MODE.ACCEPT_SERVER_SSL))
            && context.getDecodeState()
                      .equals(IContext.DecodeState.DECODING_FRAME)) context.setEncodeState(IContext.EncodeState.ENCODING_FRAME);
        return new AioPackage(ByteBuffer.wrap(output.encode()));
    }

    @Override
    public IProtocol decode(WsContext context, IProtocol input) {
        WsHandshake handshake = context.getHandshake();
        context.setHandshakeNull();
        return handshake;
    }

}
