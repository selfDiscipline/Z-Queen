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

import com.tgx.zq.z.queen.base.inf.IDisposable;

public class Triple<FIRST, SECOND, THIRD>
        implements
        IDisposable
{
    private FIRST  first;
    private SECOND second;
    private THIRD  third;

    public Triple(FIRST first, SECOND second, THIRD third) {
        this.first = first;
        this.second = second;
        this.third = third;
    }

    public FIRST first() {
        return first;
    }

    public SECOND second() {
        return second;
    }

    public THIRD third() {
        return third;
    }

    public void setFirst(FIRST f) {
        first = f;
    }

    public void setSecond(SECOND s) {
        second = s;
    }

    public void setThird(THIRD t) {
        third = t;
    }

    @Override
    public void dispose() {
        first = null;
        second = null;
        third = null;
    }

    public Triple<FIRST, SECOND, THIRD> clone() {
        return new Triple<>(first, second, third);
    }

}
