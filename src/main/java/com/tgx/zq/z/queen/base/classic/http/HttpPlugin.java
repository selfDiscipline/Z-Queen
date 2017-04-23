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
package com.tgx.zq.z.queen.base.classic.http;

import java.io.InputStream;

import com.tgx.zq.z.queen.base.classic.task.AbstractResult;

/**
 * @author William.d.zk
 */
public abstract class HttpPlugin
        extends
        AbstractResult
{
    public final static int SerialDomain = -HttpCoreTask.SerialDomain;
    protected String        url;
    protected String        CHARSET      = "UTF-8";
    byte[]                  requestData;
    InputStream             requestDataInputStream;
    long                    dataLength;
    boolean                 isCycle;
    private int             mCode;

    public HttpPlugin(String url) {
        this(url, false);
    }

    public HttpPlugin(String url, boolean isCycle) {
        super();
        this.url = url;
        this.isCycle = isCycle;
    }

    public String getUrl() {
        return url;
    }

    public void setPostData(byte[] data) {
        this.requestData = data;
    }

    public void setPostData(InputStream requestDataInputStream, long dataLength) {
        this.requestDataInputStream = requestDataInputStream;
        this.dataLength = dataLength;
    }

    /**
     * 解析网络返回的数据
     *
     * @param responseData
     * @return handled
     */
    public boolean parseData(byte[] responseData) throws Exception {
        return false;
    }

    public int getCode() {
        return mCode;
    }

    public HttpPlugin setCode(int code) {
        mCode = code;
        return this;
    }

    @Override
    public void dispose() {
        if (!isCycle) {
            url = null;
            requestData = null;
            requestDataInputStream = null;
        }
        super.dispose();
    }

    @Override
    public String toString() {
        return getClass().getName() + "|URL: " + url;
    }
}
