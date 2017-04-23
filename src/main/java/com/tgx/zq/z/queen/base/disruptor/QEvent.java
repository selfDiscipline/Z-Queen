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
package com.tgx.zq.z.queen.base.disruptor;

import java.util.List;

import com.lmax.disruptor.EventFactory;
import com.tgx.zq.z.queen.base.disruptor.inf.IEvent;
import com.tgx.zq.z.queen.base.disruptor.inf.IEventOp;
import com.tgx.zq.z.queen.base.inf.IReset;
import com.tgx.zq.z.queen.base.util.Pair;
import com.tgx.zq.z.queen.base.util.Triple;

/**
 * @author William.d.zk
 */
public class QEvent
        implements
        IReset,
        IEvent
{

    public static final EventFactory<QEvent> EVENT_FACTORY = new TheEventFactory();
    private IEventOp.Type                    mType         = IEventOp.Type.NULL;
    private Type                             mErrType      = Type.NO_ERROR;
    private IEventOp<?, ?>                   mOperator;
    private Pair<?, ?>                       mContent;
    private List<?>                          mContentList;

    @Override
    public void reset() {
        mType = IEventOp.Type.NULL;
        mErrType = Type.NO_ERROR;
        mOperator = null;
        if (mContent != null) mContent.dispose();
        mContent = null;
        if (mContentList != null) {
            mContentList.clear();
            mContentList = null;
        }
    }

    public void transfer(QEvent dest) {
        dest.mType = mType;
        dest.mErrType = mErrType;
        dest.mOperator = mOperator;
        dest.mContent = mContent.clone();
    }

    @Override
    public IEventOp.Type getEventType() {
        return mType;
    }

    @Override
    public Type getErrorType() {
        return mErrType;
    }

    @Override
    public <V, A> void produce(IEventOp.Type t, V v, A a, IEventOp<V, A> operator) {
        mType = t;
        mOperator = operator;
        mContent = new Pair<>(v, a);
        mErrType = Type.NO_ERROR;
    }

    @Override
    public <V, A> void produce(IEventOp.Type t, List<Triple<V, A, IEventOp<V, A>>> cp) {
        mType = t;
        mContentList = cp;
        mErrType = Type.NO_ERROR;
        mOperator = null;
        mContent = null;
    }

    @Override
    public <E, H> void error(Type t, E e, H h, IEventOp<E, H> operator) {
        mErrType = t;
        mOperator = operator;
        mContent = new Pair<>(e, h);
        mType = IEventOp.Type.NULL;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <V, A> IEventOp<V, A> getEventOp() {
        return (IEventOp<V, A>) mOperator;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <V, A> Pair<V, A> getContent() {
        return (Pair<V, A>) mContent;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <V, A> List<Triple<V, A, IEventOp<V, A>>> getContentList() {
        return (List<Triple<V, A, IEventOp<V, A>>>) mContentList;
    }

    public final static class TheEventFactory
            implements
            EventFactory<QEvent>
    {
        @Override
        public QEvent newInstance() {
            return new QEvent();
        }
    }

}
