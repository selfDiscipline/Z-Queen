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
package com.tgx.zq.z.queen.db.bdb.inf;

import com.tgx.zq.z.queen.io.inf.IProtocol;

public interface IDbStorageProtocol
        extends
        IProtocol
{
    byte _OP_NULL    = 0;
    byte _OP_INSERT  = 5;
    byte _OP_APPEND  = 13;
    byte _OP_REMOVE  = 9;
    byte _OP_DELETE  = 15;
    byte _OP_MODIFY  = 8;
    byte _OP_INVALID = 3;

    default void override(IDbStorageProtocol src) {
    }

    default long getPrimaryKey() {
        return -1;
    }

    default byte[] getSecondaryByteArrayKey() {
        return null;
    }

    default long getSecondaryLongKey() {
        return -1;
    }

    default Operation getOperation() {
        return Operation.OP_NULL;
    }

    default void setOperation(Operation op) {
    }

    default byte parseOperation(Operation operation) {
        switch (operation) {
            case OP_INSERT:
                return _OP_INSERT;
            case OP_APPEND:
                return _OP_APPEND;
            case OP_REMOVE:
                return _OP_REMOVE;
            case OP_DELETE:
                return _OP_DELETE;
            case OP_MODIFY:
                return _OP_MODIFY;
            case OP_INVALID:
                return _OP_INVALID;
            default:
                return _OP_NULL;
        }
    }

    enum Operation
    {
        OP_NULL,
        OP_INSERT,
        OP_APPEND,
        OP_REMOVE,
        OP_DELETE,
        OP_MODIFY,
        OP_INVALID
    }

}
