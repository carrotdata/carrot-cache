/*
 * Copyright (C) 2024-present Carrot Data, Inc. 
 * <p>This program is free software: you can redistribute it
 * and/or modify it under the terms of the Server Side Public License, version 1, as published by
 * MongoDB, Inc.
 * <p>This program is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
 * PURPOSE. See the Server Side Public License for more details. 
 * <p>You should have received a copy of the Server Side Public License along with this program. If not, see
 * <http://www.mongodb.com/licensing/server-side-public-license>.
 */
package com.carrotdata.cache.index;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotdata.cache.index.MemoryIndex.MutationResult;
import com.carrotdata.cache.util.TestUtils;
import com.carrotdata.cache.util.UnsafeAccess;
import com.carrotdata.cache.util.Utils;

public class TestMemoryIndexMultithreadedBase {
  private static final Logger LOG = LoggerFactory.getLogger(TestMemoryIndexMultithreadedBase.class);

  MemoryIndex memoryIndex;

  int numRecords = 10;
  int numThreads = 2;

  CountDownLatch cdl1;
  CountDownLatch cdl2;

  static ThreadLocal<byte[][]> keysTL = new ThreadLocal<byte[][]>();
  static ThreadLocal<byte[][]> valuesTL = new ThreadLocal<byte[][]>();
  static ThreadLocal<long[]> mKeysTL = new ThreadLocal<long[]>();
  static ThreadLocal<long[]> mValuesTL = new ThreadLocal<long[]>();
  static ThreadLocal<short[]> sidsTL = new ThreadLocal<short[]>();
  static ThreadLocal<int[]> offsetsTL = new ThreadLocal<int[]>();
  static ThreadLocal<int[]> lengthsTL = new ThreadLocal<int[]>();

  static int keySize = 16;
  static int valueSize = 16;

  @BeforeClass
  public static void enableMallocDebug() {
    // UnsafeAccess.setMallocDebugEnabled(true);
  }

  protected void tearDown() {
    memoryIndex.dispose();
    UnsafeAccess.mallocStats.printStats(false);
  }

  protected void joinAll(Thread[] workers) {
    for (Thread t : workers) {
      try {
        t.join();
      } catch (Exception e) {
      }
    }
  }

  protected Thread[] startAll(Runnable r) {
    Thread[] workers = new Thread[numThreads];
    for (int i = 0; i < workers.length; i++) {
      workers[i] = new Thread(r);
      workers[i].start();
    }
    return workers;
  }

  protected void prepareData(int numRecords) {
    this.numRecords = numRecords;
    byte[][] keys = new byte[numRecords][];
    byte[][] values = new byte[numRecords][];
    long[] mKeys = new long[numRecords];
    long[] mValues = new long[numRecords];
    short[] sids = new short[numRecords];
    int[] offsets = new int[numRecords];
    int[] lengths = new int[numRecords];

    Random r = new Random(Thread.currentThread().getId() * 100000 + System.currentTimeMillis());

    for (int i = 0; i < numRecords; i++) {
      keys[i] = TestUtils.randomBytes(keySize, r);
      values[i] = TestUtils.randomBytes(valueSize, r);
      mKeys[i] = TestUtils.randomMemory(keySize, r);
      mValues[i] = TestUtils.randomMemory(valueSize, r);
      sids[i] = (short) r.nextInt(1000);
      offsets[i] = r.nextInt(100000);
      lengths[i] = r.nextInt(10000);
    }
    keysTL.set(keys);
    valuesTL.set(values);
    mKeysTL.set(mKeys);
    mValuesTL.set(mValues);
    sidsTL.set(sids);
    offsetsTL.set(offsets);
    lengthsTL.set(lengths);
  }

  protected void clearData() {
    long[] mKeys = mKeysTL.get();
    long[] mValues = mValuesTL.get();
    Arrays.stream(mKeys).forEach(x -> UnsafeAccess.free(x));
    Arrays.stream(mValues).forEach(x -> UnsafeAccess.free(x));
  }

  protected int deleteIndexBytes() {
    int failed = 0;
    byte[][] keys = keysTL.get();
    long start = System.currentTimeMillis();
    for (int i = 0; i < numRecords; i++) {
      boolean result = memoryIndex.delete(keys[i], 0, keySize);
      if (result != true) {
        failed++;
      }
    }

    long end = System.currentTimeMillis();
    if (failed == 0) {
      LOG.info(Thread.currentThread().getName() + " deleted " + numRecords + " RPS="
          + ((long) numRecords) * 1000 / (end - start));
    } else {
      LOG.error(Thread.currentThread().getName() + " deleted " + numRecords + " RPS="
          + ((long) numRecords) * 1000 / (end - start) + " failed=" + failed);
    }
    return failed;
  }

  protected int deleteIndexMemory() {
    int failed = 0;
    long[] mKeys = mKeysTL.get();
    long start = System.currentTimeMillis();
    for (int i = 0; i < numRecords; i++) {
      boolean result = memoryIndex.delete(mKeys[i], keySize);
      if (result != true) {
        failed++;
      }
    }

    long end = System.currentTimeMillis();
    if (failed == 0) {
      LOG.info(Thread.currentThread().getName() + " deleted " + numRecords + " RPS="
          + ((long) numRecords) * 1000 / (end - start));
    } else {
      LOG.error(Thread.currentThread().getName() + " deleted " + numRecords + " RPS="
          + ((long) numRecords) * 1000 / (end - start) + " failed=" + failed);
    }
    return failed;
  }

  protected int verifyIndexBytesNot() {
    int failed = 0;
    byte[][] keys = keysTL.get();
    int entrySize = memoryIndex.getIndexFormat().indexEntrySize();
    long buf = UnsafeAccess.mallocZeroed(entrySize);
    long start = System.currentTimeMillis();
    for (int i = 0; i < numRecords; i++) {
      int result = (int) memoryIndex.find(keys[i], 0, keySize, false, buf, entrySize);
      if (result != -1) {
        failed++;
      }
    }
    assertEquals(0, failed);

    long end = System.currentTimeMillis();
    LOG.info(Thread.currentThread().getName() + " verify NOT " + numRecords + " RPS="
        + ((long) numRecords) * 1000 / (end - start));
    UnsafeAccess.free(buf);
    return failed;
  }

  protected int verifyIndexMemoryNot() {
    int failed = 0;
    long[] mKeys = mKeysTL.get();
    int entrySize = memoryIndex.getIndexFormat().indexEntrySize();
    long buf = UnsafeAccess.mallocZeroed(entrySize);
    long start = System.currentTimeMillis();
    for (int i = 0; i < numRecords; i++) {
      int result = (int) memoryIndex.find(mKeys[i], keySize, false, buf, entrySize);
      if (result != -1) {
        failed++;
      }
    }
    assertEquals(0, failed);

    long end = System.currentTimeMillis();
    LOG.info(Thread.currentThread().getName() + " verify NOT " + numRecords + " RPS="
        + ((long) numRecords) * 1000 / (end - start));
    UnsafeAccess.free(buf);
    return failed;
  }

  /**
   * Force delete - wait until full rehashing is complete
   * @param key
   * @param offset
   * @param size
   */
  protected boolean forceDelete(byte[] key, int offset, int size) {
    boolean result = false;
    while (result == false) {
      result = memoryIndex.delete(key, offset, size);
      if (!result) {
        // Thread.onSpinWait();
        Utils.onSpinWait(1000);
      }
    }
    return true;
  }

  /**
   * Force delete - wait until full rehashing is complete
   * @param key
   * @param size
   */
  protected boolean forceDelete(long key, int size) {
    boolean result = false;
    while (result == false) {
      result = memoryIndex.delete(key, size);
      if (!result) {
        // Thread.onSpinWait();
        Utils.onSpinWait(1000);
      }
    }
    return true;
  }

  /**
   * Force aarp- wait until full rehashing is complete
   * @param key
   * @param offset
   * @param size
   */
  protected MutationResult forceAarp(byte[] key, int offset, int size) {
    MutationResult result = MutationResult.FAILED;
    while (result != MutationResult.INSERTED) {
      result = memoryIndex.aarp(key, offset, size);
      if (result != MutationResult.INSERTED) {
        // Thread.onSpinWait();
        Utils.onSpinWait(1000);
      }
    }
    return result;
  }

  /**
   * Force aarp - wait until full rehashing is complete
   * @param key
   * @param size
   */
  protected MutationResult forceAarp(long key, int size) {
    MutationResult result = MutationResult.FAILED;
    while (result != MutationResult.INSERTED) {
      result = memoryIndex.aarp(key, size);
      if (result != MutationResult.INSERTED) {
        // Thread.onSpinWait();
        Utils.onSpinWait(1000);
      }
    }
    return result;
  }

  /**
   * Force aarp- wait until full rehashing is complete
   * @param key
   * @param offset
   * @param size
   */
  protected MutationResult forceAarpNot(byte[] key, int offset, int size) {
    MutationResult result = MutationResult.FAILED;
    while (result != MutationResult.DELETED) {
      result = memoryIndex.aarp(key, offset, size);
      if (result != MutationResult.DELETED) {
        // Thread.onSpinWait();
        Utils.onSpinWait(1000);
      }
    }
    return result;
  }

  /**
   * Force aarp - wait until full rehashing is complete
   * @param key
   * @param size
   */
  protected MutationResult forceAarpNot(long key, int size) {
    MutationResult result = MutationResult.FAILED;
    while (result != MutationResult.DELETED) {
      result = memoryIndex.aarp(key, size);
      if (result != MutationResult.DELETED) {
        // Thread.onSpinWait();
        Utils.onSpinWait(1000);
      }
    }
    return result;
  }

  /**
   * Force insert - wait until full rehashing is complete
   * @param key
   * @param offset
   * @param size
   */
  protected MutationResult forceInsert(byte[] key, int offset, int size, long buf, int bufSize) {
    MutationResult result = MutationResult.FAILED;
    while (result != MutationResult.INSERTED) {
      result = memoryIndex.insert(key, offset, size, buf, bufSize);
      if (result != MutationResult.INSERTED) {
        // Thread.onSpinWait();
        Utils.onSpinWait(1000);
      }
    }
    return MutationResult.INSERTED;
  }

  /**
   * Force insert - wait until full rehashing is complete
   * @param key
   * @param size
   */
  protected MutationResult forceInsert(long key, int size, long buf, int bufSize) {
    MutationResult result = MutationResult.FAILED;
    while (result != MutationResult.INSERTED) {
      result = memoryIndex.insert(key, size, buf, bufSize);
      if (result != MutationResult.INSERTED) {
        // Thread.onSpinWait();
        Utils.onSpinWait(1000);
      }
    }
    return MutationResult.INSERTED;
  }

  /**
   * Force update - wait until full rehashing is complete
   * @param key
   * @param offset
   * @param size
   * @param sid
   * @param dataOffset
   */
  protected MutationResult forceUpdate(byte[] key, int offset, int size, short sid,
      int dataOffset) {
    MutationResult result = MutationResult.FAILED;
    while (result != MutationResult.UPDATED) {
      result = memoryIndex.compareAndUpdate(key, offset, size, (short) -1, -1, sid, dataOffset);
      if (result != MutationResult.UPDATED) {
        // Thread.onSpinWait();
        // Utils.onSpinWait(1000);
      }
    }
    return MutationResult.UPDATED;
  }

  /**
   * Force update - wait until full rehashing is complete
   * @param key
   * @param size
   */
  protected MutationResult forceUpdate(long key, int size, short sid, int dataOffset) {
    MutationResult result = MutationResult.FAILED;
    while (result != MutationResult.UPDATED) {
      result = memoryIndex.compareAndUpdate(key, size, (short) -1, -1, sid, dataOffset);
      if (result != MutationResult.INSERTED) {
        // Thread.onSpinWait();
        Utils.onSpinWait(1000);
      }
    }
    return MutationResult.UPDATED;
  }
}
