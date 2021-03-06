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

import com.tgx.zq.z.queen.base.inf.ISerialTick;
import com.tgx.zq.z.queen.io.impl.AioFilterChain;
import com.tgx.zq.z.queen.io.impl.AioPackage;
import com.tgx.zq.z.queen.io.inf.IContext.DecodeState;
import com.tgx.zq.z.queen.io.inf.IContext.EncodeState;
import com.tgx.zq.z.queen.io.inf.IPoS;
import com.tgx.zq.z.queen.io.inf.IProtocol;
import com.tgx.zq.z.queen.io.ws.protocol.WsContext;
import com.tgx.zq.z.queen.io.ws.protocol.WsFrame;
import com.tgx.zq.z.queen.io.ws.protocol.bean.control.X200_HandShake;
import com.tgx.zq.z.queen.io.ws.protocol.bean.control.X201_SslHandShake;

/**
 * @author William.d.zk
 */
public class WsFrameFilter
        extends
        AioFilterChain<WsContext>
{
    Logger log = Logger.getLogger(WsFrameFilter.class.getSimpleName());

    public WsFrameFilter() {
        name = "web-socket-frame-filter-";
    }

    @Override
    public ResultType preEncode(WsContext context, IProtocol output) {
        if (context == null || output == null) return ResultType.ERROR;
        if (!context.getEncodeState().equals(EncodeState.ENCODING_FRAME)) return ResultType.IGNORE;
        if (!(output instanceof ISerialTick)) return ResultType.ERROR;
        ISerialTick tick = (ISerialTick) output;
        switch (tick.getSerialNum()) {
            case X200_HandShake.COMMAND:
            case X201_SslHandShake.COMMAND:
                return ResultType.IGNORE;
            case WsFrame.SerialNum:
                return ResultType.NEXT_STEP;
            default:
                return ResultType.ERROR;
        }

    }

    @Override
    public IProtocol encode(WsContext context, IProtocol output) {
        WsFrame toEncode = (WsFrame) output;
        toEncode.setMask(null);
        ByteBuffer toWrite = ByteBuffer.allocate(toEncode.dataLength());
        toWrite.put(toEncode.getFrameFin());
        toWrite.put(toEncode.getPayloadLengthArray());
        if (toEncode.getMaskLength() > 0) toWrite.put(toEncode.getMask());
        if (toEncode.getPayloadLength() > 0) {
            toEncode.doMask();
            toWrite.put(toEncode.getPayload());
        }
        toWrite.flip();
        return new AioPackage(toWrite);

    }

    @Override
    public ResultType preDecode(WsContext context, IProtocol input) {
        if (context == null || input == null) return ResultType.ERROR;
        if (!context.getDecodeState().equals(DecodeState.DECODING_FRAME)) return ResultType.IGNORE;
        if (!(input instanceof IPoS)) return ResultType.ERROR;
        IPoS _package = (IPoS) input;
        ByteBuffer recvBuf = _package.getBuffer();
        ByteBuffer cRvBuf = context.getRvBuffer();
        WsFrame carrier = context.getCarrier();
        int lack = context.lack();
        switch (context.position()) {
            case -1:
                if (lack > 0 && !recvBuf.hasRemaining()) return ResultType.NEED_DATA;
                context.setCarrier(carrier = new WsFrame());
                byte value = recvBuf.get();
                carrier.frame_op_code = WsFrame.getOpCode(value);
                carrier.frame_fin = WsFrame.isFrameFin(value);
                lack = context.lackLength(1, 1);
            case 0:
                if (lack > 0 && !recvBuf.hasRemaining()) return ResultType.NEED_DATA;
                carrier.setMaskCode(recvBuf.get());
                cRvBuf.put(carrier.getLengthCode());
                lack = context.lackLength(1, carrier.payloadLengthLack() + context.position());
            case 1:
            default:
                if (lack > 0 && !recvBuf.hasRemaining()) return ResultType.NEED_DATA;
                int target = context.position() + lack;
                do {
                    int remain = recvBuf.remaining();
                    int length = Math.min(remain, lack);
                    for (int i = 0; i < length; i++)
                        cRvBuf.put(recvBuf.get());
                    lack = context.lackLength(length, target);
                    if (lack > 0 && !recvBuf.hasRemaining()) return ResultType.NEED_DATA;
                    cRvBuf.flip();

                    // decoding
                    if (carrier.getPayloadLength() < 0) {
                        switch (target) {
                            case WsFrame.frame_payload_length_7_no_mask_position:
                                carrier.setPayloadLength(cRvBuf.get());
                                carrier.setMask(null);
                                break;
                            case WsFrame.frame_payload_length_16_no_mask_position:
                                carrier.setPayloadLength(cRvBuf.getShort(1) & 0xFFFF);
                                carrier.setMask(null);
                                break;
                            case WsFrame.frame_payload_length_7_mask_position:
                                carrier.setPayloadLength(cRvBuf.get());
                                byte[] mask = new byte[4];
                                cRvBuf.get(mask);
                                carrier.setMask(mask);
                                break;
                            case WsFrame.frame_payload_length_16_mask_position:
                                carrier.setPayloadLength(cRvBuf.getShort(1) & 0xFFFF);
                                mask = new byte[4];
                                cRvBuf.get(mask);
                                carrier.setMask(mask);
                                break;
                            case WsFrame.frame_payload_length_63_no_mask_position:
                                carrier.setPayloadLength(cRvBuf.getLong(1));
                                carrier.setMask(null);
                                break;
                            case WsFrame.frame_payload_length_63_mask_position:
                                carrier.setPayloadLength(cRvBuf.getLong(1));
                                mask = new byte[4];
                                cRvBuf.get(mask);
                                carrier.setMask(mask);
                                break;
                            default:
                                break;
                        }

                        target = context.position() + (int) carrier.getPayloadLength();
                        lack = context.lackLength(0, target);
                        cRvBuf.clear();
                        if (carrier.getPayloadLength() > context.getMaxPayloadSize() - target) {
                            log.warning("payload is too large");
                            return ResultType.ERROR;
                        }
                        else if (carrier.getPayloadLength() == 0) {
                            context.finish();
                            return ResultType.NEXT_STEP;
                        }
                    }
                    else {
                        if (carrier.getPayloadLength() > 0) {
                            byte[] payload = new byte[(int) carrier.getPayloadLength()];
                            cRvBuf.get(payload);
                            carrier.setPayload(payload);
                        }
                        cRvBuf.clear();
                        context.finish();
                        return ResultType.NEXT_STEP;
                    }
                }
                while (recvBuf.hasRemaining());
                return ResultType.NEED_DATA;
        }
    }

    @Override
    public IProtocol decode(WsContext context, IProtocol input) {
        log.warning("frame ok -> ");
        WsFrame frame = context.getCarrier();
        context.setCarrierNull();
        return frame;
    }

}
