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

import com.tgx.zq.z.queen.base.util.Rc4;
import com.tgx.zq.z.queen.io.inf.IConnectMode;
import com.tgx.zq.z.queen.io.inf.IEncryptHandler;
import com.tgx.zq.z.queen.io.inf.ISessionOption;

/**
 * @author William.d.zk
 */
public class ZContext
        extends
        WsContext
{

    private int             mPubKeyId        = -2;
    private boolean         mUpdateKeyIn, mUpdateKeyOut;
    private EncryptState    mEncryptInState  = EncryptState.PLAIN;
    private EncryptState    mEncryptOutState = EncryptState.PLAIN;
    private int             mSymmetricKeyId;
    private byte[]          mSymmetricKeyIn, mSymmetricKeyOut, mSymmetricKeyReroll;
    private Rc4             mEncryptRc4, mDecryptRc4;
    private IEncryptHandler mEncryptHandler;

    public ZContext(ISessionOption option, IConnectMode.OPERATION_MODE mode) {
        super(option, mode);
    }

    @Override
    public void reset() {
        super.reset();
        mEncryptInState = mEncryptOutState = EncryptState.PLAIN;
        mUpdateKeyIn = false;
        mUpdateKeyOut = false;
        mPubKeyId = -2;
        mSymmetricKeyId = 0;
    }

    @Override
    public void dispose() {
        super.dispose();
        mEncryptHandler = null;
        mSymmetricKeyIn = mSymmetricKeyOut = mSymmetricKeyReroll = null;
        if (mEncryptRc4 != null) mEncryptRc4.reset();
        if (mDecryptRc4 != null) mDecryptRc4.reset();
        mEncryptRc4 = mDecryptRc4 = null;
    }

    @Override
    public Rc4 getSymmetricDecrypt() {
        return mDecryptRc4 == null ? mDecryptRc4 = new Rc4() : mDecryptRc4;
    }

    @Override
    public Rc4 getSymmetricEncrypt() {
        return mEncryptRc4 == null ? mEncryptRc4 = new Rc4() : mEncryptRc4;
    }

    @Override
    public int getSymmetricKeyId() {
        return mSymmetricKeyId;
    }

    @Override
    public void setSymmetricKeyId(int symmetricKeyId) {
        mSymmetricKeyId = symmetricKeyId;
    }

    @Override
    public byte[] getSymmetricKeyIn() {
        return mSymmetricKeyIn;
    }

    @Override
    public byte[] getSymmetricKeyOut() {
        return mSymmetricKeyOut;
    }

    @Override
    public byte[] getReRollKey() {
        return mSymmetricKeyReroll;
    }

    @Override
    public boolean needUpdateKeyIn() {
        if (mUpdateKeyIn) {
            mUpdateKeyIn = false;
            return true;
        }
        return false;
    }

    @Override
    public boolean needUpdateKeyOut() {
        if (mUpdateKeyOut) {
            mUpdateKeyOut = false;
            return true;
        }
        return false;
    }

    @Override
    public void updateKeyIn() {
        mUpdateKeyIn = true;
    }

    @Override
    public void updateKeyOut() {
        mUpdateKeyOut = true;
    }

    @Override
    public EncryptState inState() {
        return mEncryptInState;
    }

    @Override
    public EncryptState outState() {
        return mEncryptOutState;
    }

    @Override
    public void cryptIn() {
        mEncryptInState = EncryptState.ENCRYPTED;
    }

    @Override
    public void cryptOut() {
        mEncryptOutState = EncryptState.ENCRYPTED;
    }

    @Override
    public void reRollKey(byte[] key) {
        mSymmetricKeyReroll = key;
    }

    @Override
    public void swapKeyIn(byte[] key) {
        mSymmetricKeyIn = key;
    }

    @Override
    public void swapKeyOut(byte[] key) {
        mSymmetricKeyOut = key;
    }

    @Override
    public int getPubKeyId() {
        return mPubKeyId;
    }

    @Override
    public void setPubKeyId(int pubKeyId) {
        mPubKeyId = pubKeyId;
    }

    @Override
    public IEncryptHandler getEncryptHandler() {
        return mEncryptHandler;
    }

    @Override
    public void setEncryptHandler(IEncryptHandler handler) {
        mEncryptHandler = handler;
    }
}
