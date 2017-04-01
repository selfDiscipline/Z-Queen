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

package com.tgx.zq.z.queen.cluster.replication.bean.raft;

import com.tgx.zq.z.queen.cluster.election.raft.RaftStage;

public class NodeEntity
{
    private final long _Identify;
    private RaftStage  mStage;
    private int        mSessionCount;
    private long       mTermId;
    private long       mLastCommittedSlotIndex;
    private long       mLastAppendedSlotIndex;
    private long       mBallotId;

    public NodeEntity(long identify) {
        _Identify = identify;
        mStage = RaftStage.DISCOVER;
        mBallotId = identify;
        mSessionCount = 1;
    }

    public void leader() {
        mStage = RaftStage.LEADER;
        mBallotId = _Identify;
    }

    public void follower() {
        mStage = RaftStage.FOLLOWER;
    }

    public void candidate() {
        mStage = RaftStage.CANDIDATE;
        mBallotId = _Identify;
    }

    public RaftStage getStage() {
        return mStage;
    }

    public long getIdentify() {
        return _Identify;
    }

    public void sessionIncrement() {
        mSessionCount++;
    }

    public boolean sessionDecrement() {
        mSessionCount--;
        return mSessionCount == 0;
    }

    public long getAppendSlotIndex() {
        return mLastAppendedSlotIndex;
    }

    public void setAppendSlotIndex(long slotIndex) {
        mLastAppendedSlotIndex = Math.max(slotIndex, mLastAppendedSlotIndex);
    }

    public long getCommittedSlotIndex() {
        return mLastCommittedSlotIndex;
    }

    public void setCommittedSlotIndex(long committedSlotIndex) {
        mLastCommittedSlotIndex = Math.max(committedSlotIndex, mLastCommittedSlotIndex);
    }

    public boolean updateBallot(long termId, long ballotId) {
        if (termId > mTermId) {
            mTermId = termId;
            mBallotId = ballotId;
            return true;
        }
        return false;
    }

    public long getBallot() {
        return mBallotId;
    }
}
