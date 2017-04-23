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

import java.io.IOException;
import java.net.URI;

import org.apache.hc.core5.http.ClassicHttpRequest;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.ConnectionReuseStrategy;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.HttpConnection;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.HttpException;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.HttpRequest;
import org.apache.hc.core5.http.HttpResponse;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.http.config.SocketConfig;
import org.apache.hc.core5.http.impl.DefaultConnectionReuseStrategy;
import org.apache.hc.core5.http.impl.Http1StreamListener;
import org.apache.hc.core5.http.impl.io.DefaultBHttpClientConnection;
import org.apache.hc.core5.http.impl.io.HttpRequestExecutor;
import org.apache.hc.core5.http.impl.io.bootstrap.HttpRequester;
import org.apache.hc.core5.http.impl.io.bootstrap.RequesterBootstrap;
import org.apache.hc.core5.http.io.entity.ByteArrayEntity;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hc.core5.http.io.entity.InputStreamEntity;
import org.apache.hc.core5.http.message.BasicClassicHttpRequest;
import org.apache.hc.core5.http.message.RequestLine;
import org.apache.hc.core5.http.message.StatusLine;
import org.apache.hc.core5.http.protocol.HttpCoreContext;
import org.apache.hc.core5.http.protocol.HttpProcessor;
import org.apache.hc.core5.http.protocol.HttpProcessorBuilder;
import org.apache.hc.core5.http.protocol.RequestConnControl;
import org.apache.hc.core5.http.protocol.RequestContent;
import org.apache.hc.core5.http.protocol.RequestExpectContinue;
import org.apache.hc.core5.http.protocol.RequestTargetHost;
import org.apache.hc.core5.http.protocol.RequestUserAgent;

import com.tgx.zq.z.queen.base.classic.task.Task;

public class HttpCoreTask
        extends
        Task
{

    protected final static int   SerialDomain = -0x4000;
    public final static int      SerialNum    = SerialDomain + 1;
    DefaultBHttpClientConnection conn         = new DefaultBHttpClientConnection(8 * 1024);
    ConnectionReuseStrategy      connStrategy = DefaultConnectionReuseStrategy.INSTANCE;
    HttpProcessor                httpproc     = HttpProcessorBuilder.create()
                                                                    .add(new RequestContent())
                                                                    .add(new RequestTargetHost())
                                                                    .add(new RequestConnControl())
                                                                    .add(new RequestUserAgent("Tgx/1.0"))
                                                                    .add(new RequestExpectContinue())
                                                                    .build();
    HttpRequestExecutor          httpexecutor = new HttpRequestExecutor();
    HttpEntity                   httpEntity;
    HttpHost                     host;
    HttpPlugin                   hp;
    boolean                      get;
    URI                          mUri;

    public HttpCoreTask(HttpPlugin httpPlugin) {
        super(SerialDomain);
        hp = httpPlugin;
        mUri = URI.create(httpPlugin.url);
        host = new HttpHost(mUri.getHost(), mUri.getPort(), mUri.getScheme());
        get = httpPlugin.requestData == null && httpPlugin.requestDataInputStream == null;
        if (!get) httpEntity = httpPlugin.requestData != null ? new ByteArrayEntity(httpPlugin.requestData,
                                                                                    ContentType.APPLICATION_OCTET_STREAM)
                                                              : new InputStreamEntity(httpPlugin.requestDataInputStream,
                                                                                      ContentType.APPLICATION_OCTET_STREAM);
    }

    @Override
    public void initTask() {
        super.initTask();
        isBlocker = true;
    }

    @Override
    public void run() throws Exception {
        HttpRequester httpRequester = RequesterBootstrap.bootstrap().setStreamListener(new Http1StreamListener()
        {

            @Override
            public void onRequestHead(final HttpConnection connection, final HttpRequest request) {
                System.out.println(connection + " " + new RequestLine(request));

            }

            @Override
            public void onResponseHead(final HttpConnection connection, final HttpResponse response) {
                System.out.println(connection + " " + new StatusLine(response));
            }

            @Override
            public void onExchangeComplete(final HttpConnection connection, final boolean keepAlive) {
                if (keepAlive) {
                    System.out.println(connection + " can be kept alive");
                }
                else {
                    System.out.println(connection + " cannot be kept alive");
                }
            }

        }).create();
        HttpCoreContext coreContext = HttpCoreContext.create();
        ClassicHttpRequest postRequest;
        ClassicHttpRequest request;
        if (get) request = new BasicClassicHttpRequest("GET", mUri.toString());
        else {
            postRequest = new BasicClassicHttpRequest("POST", mUri.toString());
            postRequest.setEntity(httpEntity);
            request = postRequest;
        }
        SocketConfig socketConfig = SocketConfig.custom().setConnectTimeout(5000).setSoTimeout(5000).build();
        try {
            ClassicHttpResponse response = httpRequester.execute(host, request, socketConfig, coreContext);
            hp.setCode(response.getCode());
            if (response.getCode() == HttpStatus.SC_OK) hp.parseData(EntityUtils.toByteArray(response.getEntity()));
        }
        catch (HttpException |
               IOException e) {
            e.printStackTrace();
            hp.setError(e);
        }
        finally {
            commitResult(hp, CommitAction.WAKE_UP);
        }

    }

    @Override
    public int getSerialNum() {
        return SerialNum;
    }

    @Override
    public void dispose() {
        mUri = null;
        hp = null;
        httpproc = null;
        httpexecutor = null;
        host = null;
        httpEntity = null;
        if (conn != null) try {
            conn.close();
        }
        catch (IOException e) {
            e.printStackTrace();
        }
        finally {
            conn = null;
        }
        connStrategy = null;
        super.dispose();
    }

}
