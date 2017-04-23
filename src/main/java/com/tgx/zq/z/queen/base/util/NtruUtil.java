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
package com.tgx.zq.z.queen.base.util;

import com.securityinnovation.jNeo.CiphertextBadLengthException;
import com.securityinnovation.jNeo.DecryptionFailureException;
import com.securityinnovation.jNeo.FormatNotSupportedException;
import com.securityinnovation.jNeo.NoPrivateKeyException;
import com.securityinnovation.jNeo.NtruException;
import com.securityinnovation.jNeo.OID;
import com.securityinnovation.jNeo.ObjectClosedException;
import com.securityinnovation.jNeo.ParamSetNotSupportedException;
import com.securityinnovation.jNeo.PlaintextBadLengthException;
import com.securityinnovation.jNeo.Random;
import com.securityinnovation.jNeo.ntruencrypt.KeyParams;
import com.securityinnovation.jNeo.ntruencrypt.NtruEncryptKey;

/**
 * @author William.d.zk
 */
public class NtruUtil
{
    static boolean     LOADEDLIB = false;
    static int         chiperBufLen;
    static int         plainTextMax;
    private static OID oid       = OID.ees401ep1;

    static {
        try {
            LOADEDLIB = true;
            switch (oid) {
                case ees401ep1:
                    chiperBufLen = 552;
                    plainTextMax = 60;
                    break;
                default:
                    break;
            }
            KeyParams.getKeyParams(oid);
        }
        catch (Throwable e) {
            // Ignore
        }

    }

    public byte[] getChiperBuf() {
        return new byte[chiperBufLen];
    }

    public byte[][] getKeys(byte[] seed) throws NtruException {
        Random prng = new Random(seed);
        NtruEncryptKey key = NtruEncryptKey.genKey(oid, prng);
        return new byte[][] { key.getPubKey(),
                              key.getPrivKey()

        };
    }

    public byte[] encrypt(byte[] message, byte[] pubKey) {// 客户端用的
        byte[] seed = new byte[32];
        java.util.Random rs = new java.util.Random();
        rs.nextBytes(seed);
        Random prng = new Random(seed);

        try {
            NtruEncryptKey ntruKey = new NtruEncryptKey(pubKey);
            return ntruKey.encrypt(message, prng);
        }
        catch (FormatNotSupportedException |
               ParamSetNotSupportedException |
               ObjectClosedException |
               PlaintextBadLengthException e) {
            e.printStackTrace();
            return null;
        }
    }

    public byte[] decrypt(byte[] chiper, byte[] priKey) {
        try {
            NtruEncryptKey ntruKey = new NtruEncryptKey(priKey);
            return ntruKey.decrypt(chiper);
        }
        catch (FormatNotSupportedException |
               ParamSetNotSupportedException |
               ObjectClosedException |
               NoPrivateKeyException |
               CiphertextBadLengthException |
               DecryptionFailureException e) {
            e.printStackTrace();
            return null;
        }
    }
}
