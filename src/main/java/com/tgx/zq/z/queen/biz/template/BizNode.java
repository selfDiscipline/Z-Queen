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

package com.tgx.zq.z.queen.biz.template;

import java.io.IOException;
import java.util.Map;

import com.tgx.zq.z.queen.base.disruptor.inf.IEventOp;
import com.tgx.zq.z.queen.base.util.Pair;
import com.tgx.zq.z.queen.cluster.node.ClusterNode;
import com.tgx.zq.z.queen.db.bdb.inf.IBizDao;
import com.tgx.zq.z.queen.db.bdb.inf.IDbStorageProtocol;
import com.tgx.zq.z.queen.io.inf.IConnectError;
import com.tgx.zq.z.queen.io.inf.ISession;
import com.tgx.zq.z.queen.io.inf.ISessionCreated;
import com.tgx.zq.z.queen.io.inf.ISessionDismiss;
import com.tgx.zq.z.queen.io.manager.QueenManager;
import com.tgx.zq.z.queen.mq.server.MqServer;

public abstract class BizNode<E extends IDbStorageProtocol, D extends IBizDao<E>>
        extends
        QueenManager
        implements
        ISessionCreated,
        ISessionDismiss,
        IConnectError,
        IEventOp.Factory
{

    private final ClusterNode<E, D, ? extends BizNode<E, D>> mClusterNode;
    private final D                                   mBizDao;
    private final MqServer<E, D>                      mMqServer = new MqServer<E, D>(this)
                                                                {
                                                                };

    public BizNode(final ClusterNode<E, D, ? extends BizNode<E, D>> clusterNode,
                   final Map<Long, ISession> sessionMap,
                   final Map<Long, Pair<Long, Boolean>> routeMap,
                   final D bizDao)
            throws IOException {
        super(sessionMap, routeMap);
        mClusterNode = clusterNode;
        mBizDao = bizDao;
    }

    public ClusterNode<E, D, ? extends BizNode<E, D>> getClusterNode() {
        return mClusterNode;
    }

    public D getBizDao() {
        return mBizDao;
    }

    public MqServer<E, D> getMqServer() {
        return mMqServer;
    }
}
