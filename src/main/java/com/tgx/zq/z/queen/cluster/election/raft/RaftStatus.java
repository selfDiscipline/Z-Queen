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

/**
 * @author William.d.zk
 */
public enum RaftStatus
{
    CLIENT_RECEIVED,
    CLIENT_REQ,
    CLIENT_RES,
    DISCOVER_WAIT,
    RANDOM_WAIT,
    PROPOSAL,
    VOTING,
    LEADER_LEASE,
    LEADER_JOINT_CONSENSUS,
    LEADER_ACK,
    LEADER_APPEND,
    FOLLOWER_REVERT,
    FOLLOWER_APPEND,
    FOLLOWER_JOINT_CONSENSUS,
    FOLLOWER_ACCEPT,
    FOLLOWER_REJECT,
    FOLLOWER_ACK,
    LEADER_COMMITED,
    FOLLOWER_COMMITED,
    CLIENT_COMMITED,
}
