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

package com.tgx.zq.z.queen.cluster.election.raft;

import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import com.tgx.zq.z.queen.base.classic.task.Task;
import com.tgx.zq.z.queen.base.classic.task.inf.ITaskRun;
import com.tgx.zq.z.queen.cluster.election.raft.inf.IElector;

/**
 * @author William.d.zk
 */
public class ElectTimer
        extends
        Task
{
    private final static int  SerialNum                   = 0x856F7B3F;
    public final static int   NET_TRANSPORT_TIMER         = SerialNum + 1;
    public final static int   LEASE_TIMER                 = NET_TRANSPORT_TIMER + 1;
    public final static int   LEADER_LEASE_TIMER          = LEASE_TIMER + 1;
    public final static int   RANDOM_WAIT_TIMER           = LEADER_LEASE_TIMER + 1;
    public final static int   NODE_DISCOVER_TIMER         = RANDOM_WAIT_TIMER + 1;
    public final static int   NODE_DISMISS_TIMER          = NODE_DISCOVER_TIMER + 1;

    public static long        NODE_EXCHANGE_TIMEOUT       = 75;
    public static long        NODE_ELECT_RANDOM_WAIT_MIN  = NODE_EXCHANGE_TIMEOUT << 1;
    public static long        NODE_ELECT_RANDOM_WAIT_MAX  = NODE_ELECT_RANDOM_WAIT_MIN << 1;
    public static long        NODE_LEASE_TIMEOUT          = 5000;
    public static long        NODE_LEADER_LEASE_SAP       = NODE_LEASE_TIMEOUT >> 1;
    public static long        NODE_CONNECT_RETRY_WAIT_MAX = NODE_LEASE_TIMEOUT >> 1;
    public static long        NODE_CONNECT_RETRY_WAIT_MIN = NODE_CONNECT_RETRY_WAIT_MAX >> 1;
    public static long        NODE_CONNECT_SAP            = NODE_EXCHANGE_TIMEOUT << 1;
    public static long        NODE_DISCOVER_TIMEOUT       = NODE_LEASE_TIMEOUT;
    public static long        NODE_DISMISS_TIMEOUT        = NODE_DISCOVER_TIMEOUT;

    private final RaftStage   _Stage;
    private final IElector<?> _Elector;
    Logger                    log                         = Logger.getLogger(getClass().getSimpleName());

    private ElectTimer(RaftStage stage, IElector<?> elector, long delay, TimeUnit timeUnit) {
        super(0);
        _Stage = stage;
        _Elector = elector;
        setDelay(delay, timeUnit);
    }

    protected ElectTimer(RaftStage stage, IElector<?> elector, long delay) {
        this(stage, elector, delay, TimeUnit.MILLISECONDS);
    }

    public static long getNextRetryGap(long previousDuration, long currentDuration) {
        return currentDuration > previousDuration ? currentDuration + previousDuration : previousDuration - currentDuration;
    }

    public boolean checkStage(RaftStage stage) {
        return _Stage.equals(stage) || RaftStage.NULL.equals(_Stage);
    }

    @Override
    public void run() throws Exception {
        if (checkStage(_Elector.getCurrentStage())) {
            commitResult(this, ITaskRun.CommitAction.WAKE_UP);
        }
        else {
            log.info(_Stage.name() + " timer invalid");
        }
    }

    @Override
    public int getSerialNum() {
        return SerialNum;
    }

}
