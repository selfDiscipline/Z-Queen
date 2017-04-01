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
package com.tgx.zq.z.queen.io.disruptor;

import java.util.MissingResourceException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.lmax.disruptor.BatchEventProcessor;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.LiteBlockingWaitStrategy;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.SequenceBarrier;
import com.lmax.disruptor.YieldingWaitStrategy;
import com.tgx.zq.z.queen.base.classic.task.TaskService;
import com.tgx.zq.z.queen.base.classic.task.inf.ITaskListener;
import com.tgx.zq.z.queen.base.classic.task.inf.ITaskResult;
import com.tgx.zq.z.queen.base.disruptor.MultiBufferBatchEventProcessor;
import com.tgx.zq.z.queen.base.disruptor.QEvent;
import com.tgx.zq.z.queen.base.util.Configuration;
import com.tgx.zq.z.queen.base.util.IoUtil;
import com.tgx.zq.z.queen.biz.template.BizNode;
import com.tgx.zq.z.queen.cluster.node.ClusterNode;
import com.tgx.zq.z.queen.db.bdb.inf.IBizDao;
import com.tgx.zq.z.queen.db.bdb.inf.IDbStorageProtocol;
import com.tgx.zq.z.queen.io.disruptor.handler.AioDispatchHandler;
import com.tgx.zq.z.queen.io.disruptor.handler.ClusterHandler;
import com.tgx.zq.z.queen.io.disruptor.handler.DecodeHandler;
import com.tgx.zq.z.queen.io.disruptor.handler.EncodeHandler;
import com.tgx.zq.z.queen.io.disruptor.handler.LinkHandler;
import com.tgx.zq.z.queen.io.disruptor.handler.ReadDispatchHandler;
import com.tgx.zq.z.queen.io.disruptor.handler.WriteDispatchHandler;
import com.tgx.zq.z.queen.io.disruptor.handler.WriteEndHandler;

public abstract class ExecutorCore<E extends IDbStorageProtocol, D extends IBizDao<E>, N extends BizNode<E, D>>
        extends
        ThreadPoolExecutor
        implements
        ITaskListener
{

    static Logger                                           log                   = Logger.getLogger(ExecutorCore.class.getName());
    private final RingBuffer<QEvent>[]                      _IoProducerBuffers;
    private final SequenceBarrier[]                         _IoProducerBarriers;
    private final int                                       _ProducerCount;
    private final int                                       _ClusterCount;
    private final int                                       _ProducerBufferSize   = 1 << 17;
    private final RingBuffer<QEvent>                        _ClusterLocalCloseBuffer;
    private final RingBuffer<QEvent>                        _BizLocalCloseBuffer;
    private final RingBuffer<QEvent>                        _ClusterLocalSendBuffer;
    private final RingBuffer<QEvent>                        _BizLocalSendBuffer;
    private final RingBuffer<QEvent>                        _ClusterWriteBuffer;
    private final RingBuffer<QEvent>                        _LinkWriteBuffer;
    private final RingBuffer<QEvent>                        _ConsistentWriteBuffer;
    private final RingBuffer<QEvent>                        _ConsistentResultBuffer;
    private final RingBuffer<QEvent>[]                      _ReadBuffers;
    private final SequenceBarrier[]                         _ReadBarriers;
    private final RingBuffer<QEvent>[]                      _LogicBuffers;
    private final SequenceBarrier[]                         _LogicBarriers;
    private final ConcurrentLinkedQueue<RingBuffer<QEvent>> _WorkCacheConcurrentQueue;
    private final ConcurrentLinkedQueue<RingBuffer<QEvent>> _ClusterCacheConcurrentQueue;
    private final ThreadFactory                             _WorkerThreadFactory  = new ThreadFactory()
                                                                                  {
                                                                                      int count;

                                                                                      @Override
                                                                                      public Thread newThread(Runnable r) {
                                                                                          return new AioWorker(r,
                                                                                                               ExecutorCore.this,
                                                                                                               "AioWorker.work."
                                                                                                                                  + (count++),
                                                                                                               AioWorker.WorkType.SERVICE);
                                                                                      }
                                                                                  };
    private final ThreadFactory                             _ClusterThreadFactory = new ThreadFactory()
                                                                                  {
                                                                                      int count;

                                                                                      @Override
                                                                                      public Thread newThread(Runnable r) {
                                                                                          return new AioWorker(r,
                                                                                                               ExecutorCore.this,
                                                                                                               "AioWorker.cluster."
                                                                                                                                  + (count++),
                                                                                                               AioWorker.WorkType.CLUSTER);
                                                                                      }
                                                                                  };
    private final int                                       _BindSerial;

    private final N                                         _BizNode;

    @SuppressWarnings("unchecked")
    public ExecutorCore(final int producerCount, final int clusterCount, final ClusterNode<E, D, N> clusterNode, final N bizNode) {
        super(poolSize(), poolSize(), 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<>());
        _ProducerCount = producerCount;
        _ClusterCount = clusterCount;
        _IoProducerBuffers = new RingBuffer[producerCount + clusterCount];
        _IoProducerBarriers = new SequenceBarrier[producerCount + clusterCount];
        _WorkCacheConcurrentQueue = new ConcurrentLinkedQueue<>();
        _ClusterCacheConcurrentQueue = new ConcurrentLinkedQueue<>();
        for (int i = 0; i < _IoProducerBuffers.length; i++) {
            _IoProducerBuffers[i] = RingBuffer.createSingleProducer(QEvent.EVENT_FACTORY, _ProducerBufferSize, new YieldingWaitStrategy());
            _IoProducerBarriers[i] = _IoProducerBuffers[i].newBarrier();
            if (i < producerCount) _WorkCacheConcurrentQueue.offer(_IoProducerBuffers[i]);
            else _ClusterCacheConcurrentQueue.offer(_IoProducerBuffers[i]);
        }
        _ReadBuffers = new RingBuffer[getReadBufferSizeConfig()];
        _ReadBarriers = new SequenceBarrier[getReadBufferSizeConfig()];
        _LogicBuffers = new RingBuffer[getLogicBufferSizeConfig()];
        _LogicBarriers = new SequenceBarrier[getLogicBufferSizeConfig()];

        _ClusterLocalCloseBuffer = clusterNode.getLocalBackBuffer();
        _ClusterLocalSendBuffer = clusterNode.getLocalSendBuffer();
        _BizLocalCloseBuffer = bizNode.getLocalBackBuffer();
        _BizLocalSendBuffer = bizNode.getLocalSendBuffer();

        _ClusterWriteBuffer = RingBuffer.createSingleProducer(QEvent.EVENT_FACTORY,
                                                              getClusterQueueSizeConfig(),
                                                              new YieldingWaitStrategy());
        _ConsistentWriteBuffer = RingBuffer.createSingleProducer(QEvent.EVENT_FACTORY,
                                                                 getLinkQueueSizeConfig(),
                                                                 new YieldingWaitStrategy());

        _LinkWriteBuffer = RingBuffer.createSingleProducer(QEvent.EVENT_FACTORY, getLinkQueueSizeConfig(), new YieldingWaitStrategy());

        _ConsistentResultBuffer = RingBuffer.createSingleProducer(QEvent.EVENT_FACTORY,
                                                                  getLinkQueueSizeConfig(),
                                                                  new YieldingWaitStrategy());
        /*---------------------------------------------------------------------------------------------------------------------------------------------------*/
        _BindSerial = hashCode();
        TaskService.getInstance(true).setRecycle(this);
        initialize(clusterNode,
                   this._BizNode = bizNode,
                   getClusterHandler(_ClusterWriteBuffer, _ConsistentResultBuffer, clusterNode),
                   getLinkBindHandler(_LinkWriteBuffer, _ConsistentWriteBuffer, bizNode));
    }

    public static int getLinkQueueSizeConfig() {
        try {
            return 1 << Configuration.readConfigInteger("LQ_POWER", "CoreConfig");
        }
        catch (MissingResourceException |
               ClassCastException e) {
            log.log(Level.WARNING, "getLinkQueueSizeConfig-error", e);
            return 1 << 14;
        }
    }

    public static int getClusterQueueSizeConfig() {
        try {
            return 1 << Configuration.readConfigInteger("CQ_POWER", "CoreConfig");
        }
        catch (MissingResourceException |
               ClassCastException e) {
            log.log(Level.WARNING, "getClusterQueueSizeConfig-error", e);
            return 1 << 14;
        }
    }

    public static int getReadBufferSizeConfig() {
        try {
            return 1 << Configuration.readConfigInteger("RB_POWER", "CoreConfig");
        }
        catch (MissingResourceException |
               ClassCastException e) {
            log.log(Level.WARNING, "getReadBufferSizeConfig-error", e);
            return 1;
        }
    }

    public static int getLogicBufferSizeConfig() {
        try {
            return 1 << Configuration.readConfigInteger("LG_POWER", "CoreConfig");
        }
        catch (MissingResourceException |
               ClassCastException e) {
            log.log(Level.WARNING, "getLogicBufferSizeConfig-error", e);
            return 1;
        }
    }

    public static int getLogicQueueSizeConfig() {
        try {
            return 1 << Configuration.readConfigInteger("LGQ_POWER", "CoreConfig");
        }
        catch (MissingResourceException |
               ClassCastException e) {
            log.log(Level.WARNING, "getLogicQueueSizeConfig-error", e);
            return 1;
        }

    }

    public static int getWriteBufferSizeConfig() {
        try {
            return 1 << Configuration.readConfigInteger("WR_POWER", "CoreConfig");
        }
        catch (MissingResourceException |
               ClassCastException e) {
            log.log(Level.WARNING, "getWriteBufferSizeConfig-error", e);
            return 1;
        }
    }

    public static int getWriteQueueSizeConfig() {
        try {
            return 1 << Configuration.readConfigInteger("WQ_POWER", "CoreConfig");
        }
        catch (MissingResourceException |
               ClassCastException e) {
            log.log(Level.WARNING, "getWriteQueueSizeConfig-error", e);
            return 1;
        }
    }

    public static int poolSize() {
        return 1 // aioDispatch
               + getReadBufferSizeConfig() // read-decode
               + 1 // cluster-single
               + 1 // link-single
               + 1 // read-command-dispatch
               + getLogicBufferSizeConfig()// logic-
               + 1 // write-dispatch
               + getWriteBufferSizeConfig()// write-encode
               + 1 // write-end
        ;
    }

    public final RingBuffer<QEvent> getProducerBuffer() {
        if (_WorkCacheConcurrentQueue.isEmpty()) throw new IllegalAccessError("check produce count~");
        return _WorkCacheConcurrentQueue.poll();
    }

    public final RingBuffer<QEvent> getClusterBuffer() {
        if (_ClusterCacheConcurrentQueue.isEmpty()) throw new IllegalAccessError("cluster count wrong");
        return _ClusterCacheConcurrentQueue.poll();
    }

    @Override
    protected void afterExecute(Runnable r, Throwable t) {
        if (t != null) {
            log.log(Level.FINEST, "Execute->" + r + "Has Error", t);
            submit(r);
        }
    }

    public final ThreadFactory getWorkThreadFactory() {
        return _WorkerThreadFactory;
    }

    public final ThreadFactory getClusterThreadFactory() {
        return _ClusterThreadFactory;
    }

    public final void reWorkAvailable(final RingBuffer<QEvent> buf) {
        _WorkCacheConcurrentQueue.offer(buf);
    }

    public final void reClusterAvailable(final RingBuffer<QEvent> buf) {
        _ClusterCacheConcurrentQueue.offer(buf);
    }

    @SuppressWarnings({ "unchecked" })
    private void initialize(final ClusterNode<E, D, N> clusterNode,
                            final N bizNode,
                            final ClusterHandler<E, D, N> clusterHandler,
                            final LinkHandler<E, D, N> linkHandler) {

        RingBuffer<QEvent> wroteBuffer = RingBuffer.createSingleProducer(QEvent.EVENT_FACTORY,
                                                                         _ProducerBufferSize << 1,
                                                                         new YieldingWaitStrategy());
        RingBuffer<QEvent> linkBuffer = RingBuffer.createSingleProducer(QEvent.EVENT_FACTORY,
                                                                        getLinkQueueSizeConfig(),
                                                                        new YieldingWaitStrategy());
        RingBuffer<QEvent> clusterBuffer = RingBuffer.createSingleProducer(QEvent.EVENT_FACTORY,
                                                                           getClusterQueueSizeConfig(),
                                                                           new YieldingWaitStrategy());

        RingBuffer<QEvent> writeErrBuffer = RingBuffer.createSingleProducer(QEvent.EVENT_FACTORY, 1 << 10, new YieldingWaitStrategy());

        for (int i = 0, size = getReadBufferSizeConfig(); i < size; i++) {
            _ReadBuffers[i] = RingBuffer.createSingleProducer(QEvent.EVENT_FACTORY,
                                                              Math.min(getLinkQueueSizeConfig(), getClusterQueueSizeConfig()),
                                                              new LiteBlockingWaitStrategy());
            _ReadBarriers[i] = _ReadBuffers[i].newBarrier();
        }

        RingBuffer<QEvent>[] ioBuffers = new RingBuffer[getWorkAllProducerCount() + 3];
        SequenceBarrier[] ioBarriers = new SequenceBarrier[ioBuffers.length];
        IoUtil.addArray(_IoProducerBuffers, ioBuffers, _ClusterLocalCloseBuffer, _BizLocalCloseBuffer, writeErrBuffer);
        IoUtil.addArray(_IoProducerBarriers,
                        ioBarriers,
                        _ClusterLocalCloseBuffer.newBarrier(),
                        _BizLocalCloseBuffer.newBarrier(),
                        writeErrBuffer.newBarrier());

        MultiBufferBatchEventProcessor<QEvent> aioDispatch = new MultiBufferBatchEventProcessor<>(ioBuffers,
                                                                                                  ioBarriers,
                                                                                                  new AioDispatchHandler(clusterBuffer,
                                                                                                                         linkBuffer,
                                                                                                                         wroteBuffer,
                                                                                                                         _ReadBuffers));
        for (int i = 0; i < ioBuffers.length; i++)
            ioBuffers[i].addGatingSequences(aioDispatch.getSequences()[i]);

        BatchEventProcessor<QEvent>[] readProcessors = new BatchEventProcessor[getReadBufferSizeConfig()];
        for (int i = 0, size = getReadBufferSizeConfig(); i < size; i++)
            readProcessors[i] = new BatchEventProcessor<>(_ReadBuffers[i], _ReadBarriers[i], new DecodeHandler());

        RingBuffer<QEvent> clusterReadBuffer = RingBuffer.createSingleProducer(QEvent.EVENT_FACTORY,
                                                                               getClusterQueueSizeConfig(),
                                                                               new YieldingWaitStrategy());
        RingBuffer<QEvent> linkReadBuffer = RingBuffer.createSingleProducer(QEvent.EVENT_FACTORY,
                                                                            getClusterQueueSizeConfig(),
                                                                            new YieldingWaitStrategy());

        for (int i = 0, size = getLogicBufferSizeConfig(); i < size; i++) {
            _LogicBuffers[i] = RingBuffer.createSingleProducer(QEvent.EVENT_FACTORY,
                                                               getLogicQueueSizeConfig(),
                                                               new LiteBlockingWaitStrategy());
            _LogicBarriers[i] = _LogicBuffers[i].newBarrier();
        }

        BatchEventProcessor<QEvent>[] logicProcessors = new BatchEventProcessor[getLogicBufferSizeConfig()];
        for (int i = 0, size = getLogicBufferSizeConfig(); i < size; i++)
            logicProcessors[i] = new BatchEventProcessor<>(_LogicBuffers[i],
                                                           _LogicBarriers[i],
                                                           getLogicPipeLineHandler(clusterNode, bizNode)[i]);

        RingBuffer<QEvent>[] clusterBuffers = new RingBuffer[] { clusterBuffer,
                                                                 clusterReadBuffer,
                                                                 _ConsistentWriteBuffer };
        SequenceBarrier[] clusterBarriers = new SequenceBarrier[] { clusterBuffer.newBarrier(),
                                                                    clusterReadBuffer.newBarrier(),
                                                                    _ConsistentWriteBuffer.newBarrier() };

        MultiBufferBatchEventProcessor<QEvent> clusterProcessor = new MultiBufferBatchEventProcessor<>(clusterBuffers,
                                                                                                       clusterBarriers,
                                                                                                       clusterHandler);
        for (int i = 0, size = clusterBuffers.length; i < size; i++)
            clusterBuffers[i].addGatingSequences(clusterProcessor.getSequences()[i]);

        RingBuffer<QEvent>[] linkBuffers = new RingBuffer[] { linkBuffer,
                                                              linkReadBuffer,
                                                              _ConsistentResultBuffer };
        SequenceBarrier[] linkBarriers = new SequenceBarrier[] { linkBuffer.newBarrier(),
                                                                 linkReadBuffer.newBarrier(),
                                                                 _ConsistentResultBuffer.newBarrier() };

        MultiBufferBatchEventProcessor<QEvent> linkProcessor = new MultiBufferBatchEventProcessor<>(linkBuffers, linkBarriers, linkHandler);
        for (int i = 0, size = linkBuffers.length; i < size; i++)
            linkBuffers[i].addGatingSequences(linkProcessor.getSequences()[i]);

        SequenceBarrier[] readDispatchBarriers = new SequenceBarrier[getReadBufferSizeConfig()];
        for (int i = 0, size = getReadBufferSizeConfig(); i < size; i++)
            readDispatchBarriers[i] = _ReadBuffers[i].newBarrier(readProcessors[i].getSequence());
        MultiBufferBatchEventProcessor<QEvent> readDispatcher = new MultiBufferBatchEventProcessor<>(_ReadBuffers,
                                                                                                     readDispatchBarriers,
                                                                                                     new ReadDispatchHandler(clusterHandler,
                                                                                                                             linkHandler,
                                                                                                                             clusterReadBuffer,
                                                                                                                             linkReadBuffer,
                                                                                                                             _LogicBuffers));
        for (int i = 0, size = getReadBufferSizeConfig(); i < size; i++)
            _ReadBuffers[i].addGatingSequences(readDispatcher.getSequences()[i]);

        RingBuffer<QEvent>[] toWriteBuffers = new RingBuffer[5 + getLogicBufferSizeConfig()];
        IoUtil.addArray(_LogicBuffers,
                        toWriteBuffers,
                        wroteBuffer,
                        _ClusterWriteBuffer,
                        _LinkWriteBuffer,
                        _ClusterLocalSendBuffer,
                        _BizLocalSendBuffer);

        SequenceBarrier[] toWriteBarriers = new SequenceBarrier[toWriteBuffers.length];
        for (int i = 0, size = _LogicBarriers.length; i < size; i++)
            toWriteBarriers[i] = _LogicBuffers[i].newBarrier(logicProcessors[i].getSequence());

        IoUtil.addArray(toWriteBarriers,
                        _LogicBarriers.length,
                        wroteBuffer.newBarrier(),
                        _ClusterWriteBuffer.newBarrier(),
                        _LinkWriteBuffer.newBarrier(),
                        _ClusterLocalSendBuffer.newBarrier(),
                        _BizLocalSendBuffer.newBarrier());

        RingBuffer<QEvent>[] writeBuffers = new RingBuffer[getWriteBufferSizeConfig()];
        for (int i = 0, size = getWriteBufferSizeConfig(); i < size; i++) {
            writeBuffers[i] = RingBuffer.createSingleProducer(QEvent.EVENT_FACTORY, getWriteQueueSizeConfig());
        }
        MultiBufferBatchEventProcessor<QEvent> writeDispatcher = new MultiBufferBatchEventProcessor<>(toWriteBuffers,
                                                                                                      toWriteBarriers,
                                                                                                      new WriteDispatchHandler(writeBuffers));
        for (int i = 0, size = toWriteBuffers.length; i < size; i++)
            toWriteBuffers[i].addGatingSequences(writeDispatcher.getSequences()[i]);
        BatchEventProcessor<QEvent>[] encodeProcessors = new BatchEventProcessor[getWriteBufferSizeConfig()];
        for (int i = 0, size = getWriteBufferSizeConfig(); i < size; i++)
            encodeProcessors[i] = new BatchEventProcessor<>(writeBuffers[i], writeBuffers[i].newBarrier(), new EncodeHandler());
        SequenceBarrier[] writeEndBarriers = new SequenceBarrier[getWriteBufferSizeConfig()];
        for (int i = 0, size = getWriteBufferSizeConfig(); i < size; i++)
            writeEndBarriers[i] = writeBuffers[i].newBarrier(encodeProcessors[i].getSequence());
        MultiBufferBatchEventProcessor<QEvent> writeEnd = new MultiBufferBatchEventProcessor<>(writeBuffers,
                                                                                               writeEndBarriers,
                                                                                               new WriteEndHandler(writeErrBuffer));
        for (int i = 0, size = getWriteBufferSizeConfig(); i < size; i++)
            writeBuffers[i].addGatingSequences(writeEnd.getSequences()[i]);
        /*---------------------------------------------------------------------------------------------------------------------------------------------------*/
        submit(aioDispatch);
        for (BatchEventProcessor<QEvent> rp : readProcessors)
            submit(rp);
        submit(readDispatcher);
        for (BatchEventProcessor<QEvent> lp : logicProcessors)
            submit(lp);
        submit(clusterProcessor);
        submit(linkProcessor);
        submit(writeDispatcher);
        for (BatchEventProcessor<QEvent> ep : encodeProcessors)
            submit(ep);
        submit(writeEnd);

    }

    public final int getClusterAioProducerSize() {
        return _ClusterCount;
    }

    private int getWorkAllProducerCount() {
        return _ProducerCount + _ClusterCount;
    }

    public final int getWorkAioProduceSize() {
        return _ProducerCount;
    }

    public abstract EventHandler<QEvent>[] getLogicPipeLineHandler(final ClusterNode<E, D, N> clusterNode, final N bizNode);

    public abstract LinkHandler<E, D, N> getLinkBindHandler(final RingBuffer<QEvent> linkWriter,
                                                            final RingBuffer<QEvent> consistentWriteBuffer,
                                                            final N bizNode);

    public abstract ClusterHandler<E, D, N> getClusterHandler(final RingBuffer<QEvent> clusterWriter,
                                                              final RingBuffer<QEvent> consistentResultBuffer,
                                                              final ClusterNode<E, D, N> clusterNode);

    public N getBizNode() {
        return _BizNode;
    }

    @Override
    public boolean handleResult(ITaskResult taskOrResult, TaskService service) {
        return true;
    }

    @Override
    public boolean exCaught(ITaskResult task, TaskService service) {
        return true;
    }

    @Override
    public int getBindSerial() {
        return _BindSerial;
    }

}
