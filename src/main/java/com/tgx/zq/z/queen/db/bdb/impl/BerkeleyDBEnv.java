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
package com.tgx.zq.z.queen.db.bdb.impl;

import java.io.File;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.sleepycat.je.DatabaseException;
import com.sleepycat.je.Durability;
import com.sleepycat.je.Environment;
import com.sleepycat.je.EnvironmentConfig;
import com.sleepycat.je.EnvironmentLockedException;
import com.sleepycat.je.EnvironmentNotFoundException;
import com.sleepycat.je.VersionMismatchException;

public class BerkeleyDBEnv
{
    private final Logger log = Logger.getLogger(BerkeleyDBEnv.class.getSimpleName());
    private final String mEnvHome;
    private final long   mCacheLimit;
    private Environment  mEnvironment;

    private BerkeleyDBEnv(String envHome, long cacheLimit) {
        this.mEnvHome = envHome;
        this.mCacheLimit = cacheLimit;
    }

    public final static BerkeleyDBEnv openBdbEnv(String envHome, long cacheLimit) {
        return new BerkeleyDBEnv(envHome, cacheLimit);
    }

    public final BerkeleyDBEnv open() throws IllegalArgumentException, DatabaseException {
        try {
            EnvironmentConfig enConfig = new EnvironmentConfig();
            enConfig.setAllowCreate(true).setCacheSize(mCacheLimit).setDurability(Durability.COMMIT_WRITE_NO_SYNC);
            File file = new File(mEnvHome);
            if (!file.exists()) file.mkdirs();
            mEnvironment = new Environment(file, enConfig);
        }
        catch (EnvironmentNotFoundException |
               EnvironmentLockedException |
               VersionMismatchException |
               IllegalArgumentException e) {
            log.log(Level.FINER, e.getMessage(), e);
            close();
            throw e;
        }
        return this;
    }

    public void close() {
        try {
            if (mEnvironment != null) mEnvironment.close();
        }
        catch (Exception e) {
            log.log(Level.FINER, e.getMessage(), e);
        }
    }

    public Environment getEnv() {
        return mEnvironment;
    }

}
