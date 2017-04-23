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

package com.tgx.zq.z.queen.cluster.consistent.bean;

import com.tgx.zq.z.queen.io.inf.IValid;

/**
 * @author William.d.zk
 */
public class ConsistentTransaction<E extends IValid>
{
    private final long _TransactionKey, _Timeout;
    private final E    _Entity;

    public ConsistentTransaction(long transactionKey, long timeout, E entity) {
        _TransactionKey = transactionKey;
        _Timeout = timeout;
        _Entity = entity;

    }

    public boolean isTimeout(long currentTime) {
        return currentTime > _Timeout;
    }

    public E getEntity() {
        return _Entity;
    }

    public long getTransactionKey() {
        return _TransactionKey;
    }
}
