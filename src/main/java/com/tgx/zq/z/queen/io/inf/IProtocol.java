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
package com.tgx.zq.z.queen.io.inf;

public interface IProtocol
{
    /**
     * 
     * @return max in encoding min in decoding
     */
    int dataLength();

    default int decodec(byte[] data, int pos) {
        return pos;
    }

    default int encodec(byte[] data, int pos) {
        return pos;
    }

    default byte[] encode() {
        int len = dataLength();
        if (len < 0 || len == 0) throw new IllegalArgumentException("data length is negative or zero");
        byte[] a = new byte[len];
        encodec(a, 0);
        return a;
    }

    default int encode(byte[] buf, int pos, int length) {
        int len = dataLength();
        if (len < 0 || len == 0) throw new IllegalArgumentException("data length is negative or zero");
        if (buf == null) throw new NullPointerException();
        else if (len > length
                 || buf.length < len
                 || pos + length > buf.length) throw new ArrayIndexOutOfBoundsException("data length is too long for input buf");
        pos = encodec(buf, pos);
        return pos;
    }

    default int decode(byte[] data, int pos, int length) {
        if (data == null) throw new NullPointerException();
        int len = dataLength();
        if (len > length || data.length < len || pos + length > data.length) throw new ArrayIndexOutOfBoundsException();
        return decodec(data, pos);
    }

    default int decode(byte[] data) {
        if (data == null) throw new NullPointerException();
        if (data.length < dataLength()) throw new ArrayIndexOutOfBoundsException();
        return decodec(data, 0);
    }

    default boolean idempotent(int bitIdempotent) {
        return false;
    }
}
