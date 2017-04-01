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
package com.tgx.zq.z.queen.io.inf;

import com.tgx.zq.z.queen.base.inf.IDisposable;

public interface ICommand
        extends
        IQoS,
        IMoS,
        ISerialProtocol,
        IChannel,
        IDisposable
{
    long _DEFAULT_TRANSACTION_KEY = -1;

    default IPoS translate() {
        return null;
    }

    default long getUID() {
        return -1;
    }

    default void setUID(long _uid) {
    }

    default long getTransactionKey() {
        return -1;
    }

    default void setTransactionKey(long _key) {
    }

    default ICommand duplicate() {
        return this;
    }

    @Override
    default ICommand setSession(ISession session) {
        return this;
    }
}
