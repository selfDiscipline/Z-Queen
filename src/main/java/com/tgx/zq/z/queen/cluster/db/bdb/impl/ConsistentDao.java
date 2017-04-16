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

package com.tgx.zq.z.queen.cluster.db.bdb.impl;

import java.util.logging.Level;
import java.util.logging.Logger;

import com.sleepycat.bind.EntryBinding;
import com.sleepycat.bind.tuple.LongBinding;
import com.sleepycat.collections.StoredMap;
import com.sleepycat.collections.StoredSortedMap;
import com.sleepycat.je.Database;
import com.sleepycat.je.DatabaseConfig;
import com.sleepycat.je.DatabaseEntry;
import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Environment;
import com.sleepycat.je.SecondaryConfig;
import com.sleepycat.je.SecondaryDatabase;
import com.sleepycat.je.SecondaryKeyCreator;
import com.tgx.zq.z.queen.base.util.Pair;
import com.tgx.zq.z.queen.cluster.replication.bean.raft.LogEntry;
import com.tgx.zq.z.queen.cluster.replication.bean.raft.MetaEntry;
import com.tgx.zq.z.queen.cluster.replication.bean.raft.SnapShotEntry;
import com.tgx.zq.z.queen.db.bdb.inf.IDbStorageProtocol;

public class ConsistentDao<E extends IDbStorageProtocol>
{
    private final Environment                       _Environment;
    private final long                              nodeId;
    private Logger                                  log = Logger.getLogger(getClass().getSimpleName());
    private Database                                mLogMetaDB;
    private Database                                mLogDB;
    private SecondaryDatabase                       mLogIdempotentDB;
    private Database                                mLogSnapshotDB;
    private Database                                mClientLogDB;
    private SecondaryDatabase                       mClientLogIdempotentDB;

    private StoredMap<Long, MetaEntry>              mLogMetaMap;
    private StoredMap<Long, LogEntry<E>>            mLogMap;
    private StoredMap<Long, LogEntry<E>>            mLogIdempotentMap;
    private StoredMap<Long, LogEntry<E>>            mClientLogIdempotentMap;
    private StoredMap<Long, LogEntry<E>>            mClientLogMap;
    private StoredSortedMap<Long, SnapShotEntry<E>> mLogSnapshotMap;
    private MetaEntry                               mMetaEntry;

    public ConsistentDao(Environment env, long nodeId) {
        this.nodeId = nodeId;
        LongBinding longBinding = new LongBinding();
        LogEntry.LogEntryBinding<E> leb = new LogEntry.LogEntryBinding<>();
        _Environment = env;
        try {
            DatabaseConfig dbConfig = new DatabaseConfig();
            dbConfig.setTransactional(false).setAllowCreate(true).setReadOnly(false).setSortedDuplicates(false);
            final String dbName = "com.z.queen.consistent@" + Long.toHexString(nodeId).toUpperCase();
            mLogMetaDB = _Environment.openDatabase(null, dbName + ".meta", dbConfig);
            mLogDB = _Environment.openDatabase(null, dbName + ".log", dbConfig);
            mLogSnapshotDB = _Environment.openDatabase(null, dbName + ".snapshot", dbConfig);
            SecondaryConfig dbsConfig = new SecondaryConfig();
            dbsConfig.setKeyCreator(new LogIdempotentSecondaryKeyCreator(longBinding, leb));
            dbsConfig.setAllowCreate(true).setSortedDuplicates(false).setReadOnly(false);
            mLogIdempotentDB = _Environment.openSecondaryDatabase(null, dbName + ".log.secondary", mLogDB, dbsConfig);
            mClientLogDB = _Environment.openDatabase(null, dbName + ".client", dbConfig);
            mClientLogIdempotentDB = _Environment.openSecondaryDatabase(null, dbName + ".client.secondary", mClientLogDB, dbsConfig);

            mLogMetaMap = new StoredMap<>(mLogMetaDB, longBinding, new MetaEntry.MetaEntryBinding(), true);
            mLogSnapshotMap = new StoredSortedMap<>(mLogSnapshotDB, longBinding, new SnapShotEntry.SnapShotEntryBinding<>(), true);
            mLogMap = new StoredMap<>(mLogDB, longBinding, leb, true);
            mLogIdempotentMap = new StoredMap<>(mLogIdempotentDB, longBinding, leb, true);
            mClientLogMap = new StoredMap<>(mClientLogDB, longBinding, leb, true);
            mClientLogIdempotentMap = new StoredMap<>(mClientLogIdempotentDB, longBinding, leb, true);
        }
        catch (Exception e) {
            e.printStackTrace();
            log.log(Level.FINE, "open db error", e);
        }
    }

    public MetaEntry loadMetaData() {
        return mMetaEntry = mLogMetaMap.get(nodeId);
    }

    public void updateMeta() {
        mLogMetaMap.put(nodeId, mMetaEntry = new MetaEntry(nodeId));
        _Environment.sync();
    }

    public long updateMetaTermId(long termId) {
        mMetaEntry.termId = termId;
        mLogMetaMap.put(nodeId, mMetaEntry);
        _Environment.sync();
        return termId;
    }

    public long updateMetaBallot(long leaderId) {
        mMetaEntry.ballotId = leaderId;
        mLogMetaMap.put(nodeId, mMetaEntry);
        return leaderId;
    }

    public void updateMetaClientSlotIndex(long clientSlotIndex) {
        mMetaEntry.lastClientSlotIndex = clientSlotIndex;
        mLogMetaMap.put(nodeId, mMetaEntry);
    }

    public void updateMetaSlotIndex(long termId, long slotIndex) {
        mMetaEntry.termId = termId;
        mMetaEntry.slotIndex = slotIndex;
        mLogMetaMap.put(nodeId, mMetaEntry);
    }

    public void updateMetaLastCommittedSlotIndex(long termId, long slotIndex) {
        mMetaEntry.lastCommittedTermId = termId;
        mMetaEntry.lastCommittedSlotIndex = slotIndex;
        mLogMetaMap.put(nodeId, mMetaEntry);
        _Environment.sync();
    }

    public Pair<Long, Long> getLastCommitted(long identify) {
        mMetaEntry = mLogMetaMap.get(identify);
        return new Pair<>(mMetaEntry.lastCommittedTermId, mMetaEntry.lastCommittedSlotIndex);
    }

    public long getLastCommittedTerm(long identify) {
        return mLogMetaMap.get(identify).lastCommittedTermId;
    }

    public boolean checkConsistent(long slotIndex, long termId) {
        LogEntry<E> logEntry = mLogMap.get(slotIndex);
        return (logEntry != null && logEntry.termId == termId);
    }

    public void removeEntry(long slotIndex, long lastLogSlotIndex) {
        if (slotIndex > lastLogSlotIndex) throw new IllegalStateException("slot index less than last log slot index: "
                                                                          + slotIndex
                                                                          + " | "
                                                                          + lastLogSlotIndex);
        for (long slot = slotIndex; slot <= lastLogSlotIndex; slot++)
            mLogMap.remove(slot);
    }

    public long appendClientEntry(LogEntry<E> logEntry) {
        mClientLogMap.put(logEntry.getSlotIndex(), logEntry);
        updateMetaClientSlotIndex(logEntry.getSlotIndex());
        return logEntry.getSlotIndex();
    }

    public LogEntry<E> getEntryBySlotIndex(long slotIndex) {
        return mLogMap.get(slotIndex);
    }

    public LogEntry<E> getEntryByIdempotent(long idempotent) {
        return mLogIdempotentMap.get(idempotent);
    }

    /**
     * @param logEntry
     * @return next slot index
     */
    public long appendEntry(LogEntry<E> logEntry) {
        logEntry.append();
        mLogMap.put(logEntry.getSlotIndex(), logEntry);
        return logEntry.slotIndex;
    }

    public long jointConsensus(LogEntry<E> logEntry, long[] oldConfig, long[] newConfig) {
        logEntry.joinConsensus(oldConfig, newConfig);
        mLogMap.put(logEntry.getPrimaryKey(), logEntry);
        return logEntry.slotIndex;
    }

    public boolean commitEntry(long slotIndex) {
        if (slotIndex > mMetaEntry.lastCommittedSlotIndex) {
            LogEntry<E> logEntry = mLogMap.get(slotIndex);
            if (logEntry == null) {
                log.warning("slot has no entry : " + slotIndex);
                return false;
            }
            logEntry.commit();
            mLogMap.put(slotIndex, logEntry);
        }
        return true;
    }

    public long transferLogEntry(long clientSlotIndex, long logSlotIndex, long logTermId) {
        LogEntry<E> logEntry = mClientLogMap.get(clientSlotIndex);
        logEntry.slotIndex = logSlotIndex;
        logEntry.termId = logTermId;
        return appendEntry(logEntry);
    }

    public LogEntry<E> clearClientEntryByIdempotent(long idempotent) {
        return mClientLogIdempotentMap.remove(idempotent);
    }

    public LogEntry<E> clearClientEntryBySlot(long slotIndex) {
        return mClientLogMap.remove(slotIndex);
    }

    public void close() {
        try {
            if (mLogDB != null) mLogDB.close();
            if (mLogMetaDB != null) mLogMetaDB.close();
            if (mLogSnapshotDB != null) mLogSnapshotDB.close();
            if (mClientLogDB != null) mClientLogDB.close();
        }
        catch (DatabaseException e) {
            log.log(Level.WARNING, "cluster consistent dao.database close error ", e);
        }
        finally {
            if (_Environment != null && !_Environment.isClosed()) try {
                _Environment.sync();
                _Environment.cleanLog();
                _Environment.close();
            }
            catch (DatabaseException e) {
                log.log(Level.SEVERE, "cluster consistent dao.env close error ", e);
            }
        }

    }

    private class LogIdempotentSecondaryKeyCreator
            implements
            SecondaryKeyCreator
    {
        private final LongBinding               secKeyBinding;
        private final EntryBinding<LogEntry<E>> dataBinding;

        LogIdempotentSecondaryKeyCreator(LongBinding secKeyBinding, EntryBinding<LogEntry<E>> dataBinding) {
            this.secKeyBinding = secKeyBinding;
            this.dataBinding = dataBinding;
        }

        @Override
        public boolean createSecondaryKey(SecondaryDatabase secondaryDb,
                                          DatabaseEntry keyEntry,
                                          DatabaseEntry dataEntry,
                                          DatabaseEntry resultEntry) {
            LogEntry<E> logEntry = dataBinding.entryToObject(dataEntry);
            if (logEntry.idempotent != 0) {
                secKeyBinding.objectToEntry(logEntry.idempotent, resultEntry);
                return true;
            }
            return false;
        }

    }
}
