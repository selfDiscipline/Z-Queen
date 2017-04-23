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

import java.util.Arrays;
import java.util.logging.Logger;

import com.tgx.zq.z.queen.base.constant.QueenCode;
import com.tgx.zq.z.queen.base.inf.ISerialTick;
import com.tgx.zq.z.queen.base.util.Pair;
import com.tgx.zq.z.queen.io.impl.AioFilterChain;
import com.tgx.zq.z.queen.io.inf.ICommand;
import com.tgx.zq.z.queen.io.inf.IContext.DecodeState;
import com.tgx.zq.z.queen.io.inf.IContext.EncodeState;
import com.tgx.zq.z.queen.io.inf.IEncryptHandler;
import com.tgx.zq.z.queen.io.inf.IProtocol;
import com.tgx.zq.z.queen.io.ws.protocol.Command;
import com.tgx.zq.z.queen.io.ws.protocol.WsContext;
import com.tgx.zq.z.queen.io.ws.protocol.WsControl;
import com.tgx.zq.z.queen.io.ws.protocol.WsFrame;
import com.tgx.zq.z.queen.io.ws.protocol.WsHandshake;
import com.tgx.zq.z.queen.io.ws.protocol.bean.tls.X01_EncryptRequest;
import com.tgx.zq.z.queen.io.ws.protocol.bean.tls.X02_AsymmetricPub;
import com.tgx.zq.z.queen.io.ws.protocol.bean.tls.X03_Cipher;
import com.tgx.zq.z.queen.io.ws.protocol.bean.tls.X04_EncryptConfirm;
import com.tgx.zq.z.queen.io.ws.protocol.bean.tls.X05_EncryptStart;
import com.tgx.zq.z.queen.io.ws.protocol.bean.tls.X06_PlainStart;

/**
 * @author William.d.zk
 */
public class ZCommandFilter
        extends
        AioFilterChain<WsContext>
{

    private final CommandFactory factory;
    Logger                       log = Logger.getLogger(getClass().getSimpleName());

    public ZCommandFilter() {
        this(null);
    }

    public ZCommandFilter(CommandFactory factory) {
        this.factory = factory;
        name = "queen-command-filter";
    }

    @Override
    public ResultType preEncode(WsContext context, IProtocol output) {
        if (output == null || context == null) return ResultType.ERROR;
        if (!context.getEncodeState().equals(EncodeState.ENCODING_FRAME)) return ResultType.IGNORE;
        if (!(output instanceof ISerialTick)) return ResultType.ERROR;
        ISerialTick tick = (ISerialTick) output;
        switch (tick.getSuperSerialNum()) {
            case Command.SerialNum:
                return ResultType.NEXT_STEP;
            case WsControl.SerialNum:
            case WsHandshake.SerialNum:
            case WsFrame.SerialNum:
                return ResultType.IGNORE;
            default:
                return ResultType.ERROR;
        }
    }

    @Override
    public ResultType preDecode(WsContext context, IProtocol input) {
        if (context == null || input == null) return ResultType.ERROR;
        if (!context.getDecodeState().equals(DecodeState.DECODING_FRAME)) return ResultType.IGNORE;
        if (!(input instanceof WsFrame)) return ResultType.ERROR;
        WsFrame frame = (WsFrame) input;
        return frame.isNoCtrl() ? ResultType.HANDLED : ResultType.IGNORE;
    }

    @Override
    public IProtocol encode(WsContext context, IProtocol output) {
        WsFrame frame = new WsFrame();
        @SuppressWarnings("unchecked")
        Command<WsContext> command = (Command<WsContext>) output;
        frame.setPayload(command.encode(context));
        frame.setCtrl(command.getControl());
        return frame;
    }

    @Override
    public IProtocol decode(WsContext context, IProtocol input) {
        WsFrame frame = (WsFrame) input;
        int command = frame.getPayload()[1] & 0xFF;
        @SuppressWarnings("unchecked")
        Command<WsContext> _command = (Command<WsContext>) createCommand(command);
        _command.decode(frame.getPayload(), context);
        ICommand inCommand = _command;
        switch (command) {
            case X01_EncryptRequest.COMMAND:
                X01_EncryptRequest x01 = (X01_EncryptRequest) inCommand;
                IEncryptHandler encryptHandler = context.getEncryptHandler();
                if (encryptHandler == null || !x01.isEncrypt()) return new X06_PlainStart(QueenCode.PLAIN_UNSUPPORTED);
                Pair<Integer, byte[]> keyPair = encryptHandler.getAsymmetricPubKey(x01.pubKeyId);
                if (keyPair != null) {
                    X02_AsymmetricPub x02 = new X02_AsymmetricPub();
                    context.setPubKeyId(keyPair.first());
                    x02.setPubKey(keyPair.first(), keyPair.second());
                    return x02;
                }
                else throw new NullPointerException("create public-key failed!");
            case X02_AsymmetricPub.COMMAND:
                X02_AsymmetricPub x02 = (X02_AsymmetricPub) inCommand;
                encryptHandler = context.getEncryptHandler();
                if (encryptHandler == null) return new X06_PlainStart(QueenCode.PLAIN_UNSUPPORTED);
                byte[] symmetricKey = context.getSymmetricEncrypt().createKey("z-tls-rc4");
                if (symmetricKey == null) throw new NullPointerException("create symmetric-key failed!");
                keyPair = encryptHandler.getCipher(x02.pubKey, symmetricKey);
                if (keyPair != null) {
                    context.setPubKeyId(x02.pubKeyId);
                    context.setSymmetricKeyId(keyPair.first());
                    context.reRollKey(symmetricKey);
                    X03_Cipher x03 = new X03_Cipher();
                    x03.pubKeyId = x02.pubKeyId;
                    x03.symmetricKeyId = keyPair.first();
                    x03.cipher = keyPair.second();
                    return x03;
                }
                else throw new NullPointerException("encrypt symmetric-key failed!");
            case X03_Cipher.COMMAND:
                X03_Cipher x03 = (X03_Cipher) inCommand;
                encryptHandler = context.getEncryptHandler();
                if (context.getPubKeyId() == x03.pubKeyId) {
                    symmetricKey = encryptHandler.getSymmetricKey(x03.pubKeyId, x03.cipher);
                    if (symmetricKey == null) throw new NullPointerException("decrypt symmetric-key failed!");
                    context.setSymmetricKeyId(x03.symmetricKeyId);
                    context.reRollKey(symmetricKey);
                    X04_EncryptConfirm x04 = new X04_EncryptConfirm();
                    x04.symmetricKeyId = x03.symmetricKeyId;
                    x04.response = QueenCode.SYMMETRIC_KEY_OK;
                    x04.setSign(encryptHandler.getSymmetricKeySign(symmetricKey));
                    return x04;
                }
                else {
                    keyPair = encryptHandler.getAsymmetricPubKey(x03.pubKeyId);
                    if (keyPair != null) {
                        x02 = new X02_AsymmetricPub();
                        context.setPubKeyId(keyPair.first());
                        x02.setPubKey(keyPair.first(), keyPair.second());
                        return x02;
                    }
                    else throw new NullPointerException("create public-key failed!");
                }
            case X04_EncryptConfirm.COMMAND:
                X04_EncryptConfirm x04 = (X04_EncryptConfirm) inCommand;
                encryptHandler = context.getEncryptHandler();
                if (x04.symmetricKeyId == context.getSymmetricKeyId()
                    && Arrays.equals(encryptHandler.getSymmetricKeySign(context.getReRollKey()), x04.getSign())) {
                    X05_EncryptStart x05 = new X05_EncryptStart();
                    x05.symmetricKeyId = x04.symmetricKeyId;
                    x05.salt = encryptHandler.nextRandomInt();
                    return x05;
                }
                else {
                    context.reset();
                    return new X01_EncryptRequest();
                }
            case X05_EncryptStart.COMMAND:
                X05_EncryptStart x05 = (X05_EncryptStart) inCommand;
                if (context.getSymmetricKeyId() != x05.symmetricKeyId) throw new IllegalStateException("symmetric key id is not equals");
                log.info("encrypt start");
            case X06_PlainStart.COMMAND:
                return null;
            default:
                return _command;
        }
    }

    Command<? extends WsContext> createCommand(int command) {
        switch (command) {
            case X01_EncryptRequest.COMMAND:
                return new X01_EncryptRequest();
            case X02_AsymmetricPub.COMMAND:
                return new X02_AsymmetricPub();
            case X03_Cipher.COMMAND:
                return new X03_Cipher();
            case X04_EncryptConfirm.COMMAND:
                return new X04_EncryptConfirm();
            case X05_EncryptStart.COMMAND:
                return new X05_EncryptStart();
            case X06_PlainStart.COMMAND:
                return new X06_PlainStart();
            case 0xFF:
                throw new UnsupportedOperationException();
            default:
                return factory != null ? factory.createCommand(command) : null;
        }
    }

    public interface CommandFactory
    {
        Command<? extends WsContext> createCommand(int command);
    }

}
