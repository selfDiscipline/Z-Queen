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
package com.tgx.zq.z.queen.base.util;

import static java.lang.System.arraycopy;

import java.io.InputStream;
import java.nio.ByteBuffer;

public class IoUtil
{
    private static final char HEX_DIGITS[] = { '0',
                                               '1',
                                               '2',
                                               '3',
                                               '4',
                                               '5',
                                               '6',
                                               '7',
                                               '8',
                                               '9',
                                               'A',
                                               'B',
                                               'C',
                                               'D',
                                               'E',
                                               'F' };

    public final static String bin2Hex(byte[] b) {
        return bin2Hex(b, 0, b.length);
    }

    public final static String bin2Hex(byte[] b, int pos, int length) {
        if (length < 0 || length > b.length || pos + length > b.length) throw new ArrayIndexOutOfBoundsException();
        StringBuilder sb = new StringBuilder(length * 2);
        for (int i = pos, size = pos + length; i < size; i++) {

            sb.append(HEX_DIGITS[(b[i] & 0xf0) >>> 4]);
            sb.append(HEX_DIGITS[b[i] & 0x0f]);
        }
        return sb.toString();
    }

    public final static byte[] hex2bin(String hex) {
        return hex2bin(hex, null, 0);
    }

    public final static byte[] hex2bin(String hex, byte[] b, int pos) {
        int len = hex.length() >> 1;
        if (len > 0) {
            if (b == null) {
                b = new byte[len];
                pos = 0;
            }
            else if (b.length - pos < len) return null;
            for (int i = 0, j = pos, hPos = 0; i < len; i++, hPos = i << 1, j++) {
                b[j] = (byte) Integer.parseInt(hex.substring(hPos, hPos + 2), 16);
            }
            return b;
        }
        return null;
    }

    public final static String longToMacStr(long mac) {
        StringBuilder sb = new StringBuilder();
        sb.append(Long.toHexString((mac >> 40) & 0xFF));
        sb.append(Long.toHexString((mac >> 32) & 0xFF));
        sb.append(Long.toHexString((mac >> 24) & 0xFF));
        sb.append(Long.toHexString((mac >> 16) & 0xFF));
        sb.append(Long.toHexString((mac >> 8) & 0xFF));
        sb.append(Long.toHexString(mac & 0xFF));

        return sb.toString();
    }

    public final static int writeIp(String ipAddr) {
        if (ipAddr == null) return 0;
        String[] ipx = ipAddr.split("\\p{Punct}");
        int a = Integer.parseInt(ipx[0]);
        int b = Integer.parseInt(ipx[1]);
        int c = Integer.parseInt(ipx[2]);
        int d = Integer.parseInt(ipx[3]);
        return a << 24 | b << 16 | c << 8 | d;
    }

    public final static long writeInetAddr(String ipAddr, int port) {
        if (ipAddr == null) return 0;
        String[] ipx = ipAddr.split("\\p{Punct}");
        int a = Integer.parseInt(ipx[0]);
        int b = Integer.parseInt(ipx[1]);
        int c = Integer.parseInt(ipx[2]);
        int d = Integer.parseInt(ipx[3]);
        long ip = a << 24 | b << 16 | c << 8 | d | 0xFFFFFFFFL;
        return ip << 16 | port;
    }

    public final static long writeInet4Addr(byte[] ipAddr, int port) {
        if (ipAddr == null) return 0;
        long ip = ipAddr[0] & 0xFF;
        for (int i = 1; i < 4; i++)
            ip = (ip << 8) | (ipAddr[i] & 0xFF);
        return (ip << 16) | (port & 0xFFFF);
    }

    public final static String readIp(int ip) {
        int a, b, c, d;
        a = (ip >>> 24) & 0xFF;
        b = (ip >>> 16) & 0xFF;
        c = (ip >>> 8) & 0xFF;
        d = ip & 0xFF;
        return new StringBuffer().append(a).append('.').append(b).append('.').append(c).append('.').append(d).toString();
    }

    public final static String readInetAddr(long inetAddr) {
        int a, b, c, d, port;
        a = (int) (inetAddr >> 40) & 0xFF;
        b = (int) (inetAddr >> 32) & 0xFF;
        c = (int) (inetAddr >> 24) & 0xFF;
        d = (int) (inetAddr >> 16) & 0xFF;
        port = (int) (inetAddr & 0xFFFF);
        return new StringBuffer().append(a)
                                 .append('.')
                                 .append(b)
                                 .append('.')
                                 .append(c)
                                 .append('.')
                                 .append(d)
                                 .append(':')
                                 .append(port)
                                 .toString();
    }

    public final static byte[] variableLength(int length) {
        if (length == 0) return new byte[] { 0 };
        int resLength = 0;
        int result = 0;
        do {
            result |= (length & 0x7F) << 24;
            length >>>= 7;
            resLength++;
            if (length > 0) {
                result >>>= 8;
                result |= 0x80000000;
            }
        }
        while (length > 0);
        byte[] res = new byte[resLength];
        for (int i = 0, move = 24; i < resLength; i++) {
            res[i] = (byte) (result >>> move);
            move -= 8;
        }
        return res;
    }

    public final static byte[] variableLength(long length) {
        if (length == 0) return new byte[] { 0 };
        int resLength = 0;
        long result = 0;
        do {
            result |= (length & 0x7F) << 56;
            length >>>= 7;
            resLength++;
            if (length > 0) {
                result >>>= 8;
                result |= 0x80000000;
            }
        }
        while (length > 0);
        byte[] res = new byte[resLength];
        for (int i = 0, move = 56; i < resLength; i++) {
            res[i] = (byte) (result >>> move);
            move -= 8;
        }
        return res;
    }

    public final static int readVariableLength(InputStream is) {
        int length = 0;
        int cur;
        try {
            do {
                cur = is.read();
                if (cur < 0) break;
                length |= (cur & 0x7F);
                if ((cur & 0x80) != 0) length <<= 7;
            }
            while ((cur & 0x80) != 0);
            return length;
        }
        catch (Exception e) {
            return 0;
        }
    }

    public final static long readVariableLongLength(InputStream is) {
        long length = 0;
        int cur;
        try {
            do {
                cur = is.read();
                if (cur < 0) break;
                length |= (cur & 0x7F);
                if ((cur & 0x80) != 0) length <<= 7;
            }
            while ((cur & 0x80) != 0);
            return length;
        }
        catch (Exception e) {
            return 0;
        }
    }

    public final static int readVariableLength(ByteBuffer buf) {
        int length = 0;
        int cur;
        if (buf.hasRemaining()) do {
            cur = buf.get();
            length |= (cur & 0x7F);
            if ((cur & 0x80) != 0) length <<= 7;
        }
        while ((cur & 0x80) != 0 && buf.hasRemaining());
        return length;
    }

    public final static long readVariableLongLength(ByteBuffer buf) {
        long length = 0;
        int cur;
        if (buf.hasRemaining()) do {
            cur = buf.get();
            length |= (cur & 0x7F);
            if ((cur & 0x80) != 0) length <<= 7;
        }
        while ((cur & 0x80) != 0 && buf.hasRemaining());
        return length;
    }

    public final static int writeLongArray(long[] v, byte[] b, int off) {
        if (v == null) return 0;
        for (int i = 0; i < v.length; i++)
            off += writeLong(v[i], b, off);
        return v.length << 3;
    }

    public final static int writeShortArray(short[] v, byte[] b, int off) {
        if (v == null) return 0;
        for (int i = 0; i < v.length; i++)
            off += writeShort(v[i] & 0xFFFF, b, off);
        return v.length << 1;
    }

    public final static int writeLong(long v, byte[] b, int off) {
        b[off] = (byte) (0xFF & (v >>> 56));
        b[off + 1] = (byte) (0xFF & (v >>> 48));
        b[off + 2] = (byte) (0xFF & (v >>> 40));
        b[off + 3] = (byte) (0xFF & (v >>> 32));
        b[off + 4] = (byte) (0xFF & (v >>> 24));
        b[off + 5] = (byte) (0xFF & (v >>> 16));
        b[off + 6] = (byte) (0xFF & (v >>> 8));
        b[off + 7] = (byte) (0xFF & v);
        return 8;
    }

    public final static int writeMac(long v, byte[] b, int off) {
        return write6BLong(v, b, off);
    }

    public final static int write6BLong(long v, byte[] b, int off) {
        b[off] = (byte) (0xFF & (v >> 40));
        b[off + 1] = (byte) (0xFF & (v >> 32));
        b[off + 2] = (byte) (0xFF & (v >> 24));
        b[off + 3] = (byte) (0xFF & (v >> 16));
        b[off + 4] = (byte) (0xFF & (v >> 8));
        b[off + 5] = (byte) (0xFF & v);
        return 6;
    }

    public final static int writeInt(int v, byte[] b, int off) {
        b[off] = (byte) (0xFF & (v >>> 24));
        b[off + 1] = (byte) (0xFF & (v >>> 16));
        b[off + 2] = (byte) (0xFF & (v >>> 8));
        b[off + 3] = (byte) (0xFF & v);
        return 4;
    }

    public final static int writeShort(int v, byte[] b, int off) {
        b[off] = (byte) (0xFF & (v >>> 8));
        b[off + 1] = (byte) (0xFF & v);
        return 2;
    }

    public final static int writeByte(int v, byte[] b, int off) {
        b[off] = (byte) v;
        return 1;
    }

    public final static int writeByte(byte v, byte[] b, int off) {
        b[off] = v;
        return 1;
    }

    public final static int write(byte[] v, int src_off, byte[] b, int off, int len) {
        if (v == null || v.length == 0 || len == 0) return 0;
        else if (len > b.length) throw new ArrayIndexOutOfBoundsException();
        if (len > v.length) len = v.length;
        arraycopy(v, src_off, b, off, len);
        return len;
    }

    public static int write(byte[] v, byte[] data, int pos) {
        return write(v, 0, data, pos, v.length);
    }

    public final static int writeIpAdr(String ipAdr, byte[] b, int off) {
        String[] split = ipAdr.split("\\p{Punct}", 4);
        for (int i = 0; i < 4; i++)
            writeByte(Integer.parseInt(split[i]), b, off++);
        return 4;
    }

    public final static <T> void addArray(T[] src, T[] dst, @SuppressWarnings("unchecked") T... add) {
        arraycopy(src, 0, dst, 0, src.length);
        if (add != null) arraycopy(add, 0, dst, src.length, add.length);
    }

    public final static void addArray(Object[] src, Object[] dst, int pos) {
        if (src != null) arraycopy(src, 0, dst, pos, dst.length);
    }

    public final static void addArray(Object[] dst, int pos, Object... add) {
        if (add != null) arraycopy(add, 0, dst, pos, add.length);
    }

    public final static short readShort(byte[] src, int off) {
        return (short) ((src[off] & 0xFF) << 8 | (src[off + 1] & 0xFF));
    }

    public final static int readUnsignedShort(byte[] src, int off) {
        return ((src[off] & 0xFF) << 8 | (src[off + 1] & 0xFF));
    }

    public final static int readInt(byte[] src, int off) {
        return (src[off] & 0xFF) << 24 | (src[off + 1] & 0xFF) << 16 | (src[off + 2] & 0xFF) << 8 | (src[off + 3] & 0xFF);
    }

    public final static long readLong(byte[] src, int off) {
        return (src[off] & 0xFFL) << 56
               | (src[off + 1] & 0xFFL) << 48
               | (src[off + 2] & 0xFFL) << 40
               | (src[off + 3] & 0xFFL) << 32
               | (src[off + 4] & 0xFFL) << 24
               | (src[off + 5] & 0xFFL) << 16
               | (src[off + 6] & 0xFFL) << 8
               | (src[off + 7] & 0xFFL);
    }

    public final static int readLongArray(byte[] src, int src_off, long[] dst) {
        if (dst == null) return src_off;
        if ((dst.length << 3) + src_off > src.length) throw new ArrayIndexOutOfBoundsException();
        for (int i = 0; i < dst.length; i++, src_off += 8)
            dst[i] = readLong(src, src_off);
        return src_off;
    }

    public final static int readShortArray(byte[] src, int src_off, short[] dst) {
        if (dst == null) return src_off;
        if ((dst.length << 1) + src_off > src.length) throw new ArrayIndexOutOfBoundsException();
        for (int i = 0; i < dst.length; i++, src_off += 2)
            dst[i] = readShort(src, src_off);
        return src_off;
    }

    public final static int readUnsignedShortArray(byte[] src, int src_off, int[] dst) {
        if (dst == null) return src_off;
        if ((dst.length << 1) + src_off > src.length) throw new ArrayIndexOutOfBoundsException();
        for (int i = 0; i < dst.length; i++, src_off += 2)
            dst[i] = readUnsignedShort(src, src_off);
        return src_off;
    }

    public final static long read6BLong(byte[] src, int off) {
        return (src[off] & 0xFFL) << 40
               | (src[off + 1] & 0xFFL) << 32
               | (src[off + 2] & 0xFFL) << 24
               | (src[off + 3] & 0xFFL) << 16
               | (src[off + 4] & 0xFFL) << 8
               | (src[off + 5] & 0xFFL);

    }

    public final static long readMac(byte[] src, int off) {
        return read6BLong(src, off);
    }

    public final static String readIpAdr(byte[] src, int off) {
        StringBuffer buf = new StringBuffer();
        for (int i = 0; i < 4; i++) {
            buf.append(src[off++] & 0xFF);
            if (i < 3) buf.append('.');
        }
        return buf.toString();
    }

    public final static int read(byte[] src, int src_off, byte[] dst, int dst_off, int len) {
        arraycopy(src, src_off, dst, dst_off, len);
        return src_off + len;
    }

    public final static int read(byte[] src, int src_off, byte[] dst) {
        return read(src, src_off, dst, 0, dst.length);
    }

    public final static int readUnsignedByte(byte[] src, int off) {
        return src[off++] & 0xFF;
    }

    public final static long readStorage(String src) {
        if (src == null || src.equals("")) return 0L;
        src = src.toLowerCase();
        int index;
        if ((index = src.indexOf('g')) > 0) return Long.parseLong(src.substring(0, index)) << 30;
        else if ((index = src.indexOf('m')) > 0) return Long.parseLong(src.substring(0, index)) << 20;
        else if ((index = src.indexOf('k')) > 0) return Long.parseLong(src.substring(0, index)) << 10;
        else if ((index = src.indexOf('b')) > 0) return Long.parseLong(src.substring(0, index));
        return Long.parseLong(src);
    }

    public final static String replace(String from, String to, String source) {
        if (source == null || from == null || to == null) return null;
        if (source.indexOf(from) < 0) return source;
        StringBuffer bf = new StringBuffer();
        int index = -1;
        while ((index = source.indexOf(from)) != -1) {
            bf.append(source.substring(0, index) + to);
            source = source.substring(index + from.length());
            index = -1;
        }
        bf.append(source);
        return bf.toString();
    }

}
