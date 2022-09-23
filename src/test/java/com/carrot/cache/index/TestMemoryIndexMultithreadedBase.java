/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.carrot.cache.index;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

import org.junit.BeforeClass;

import com.carrot.cache.index.MemoryIndex.MutationResult;
import com.carrot.cache.util.TestUtils;
import com.carrot.cache.util.UnsafeAccess;
import com.carrot.cache.util.Utils;

public class TestMemoryIndexMultithreadedBase {
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
    //UnsafeAccess.setMallocDebugEnabled(true);
  }
  
  protected void tearDown() {
    memoryIndex.dispose();
    UnsafeAccess.mallocStats.printStats(false);
  }
  
  protected void joinAll(Thread[] workers) {
    for (Thread t : workers) {
      try {
        t.join();
      } catch(Exception e) {
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
    for(int i = 0; i < numRecords; i++) {
      boolean result = memoryIndex.delete(keys[i], 0, keySize);
      if (result != true) {
        failed++;
      }
    }

    long end = System.currentTimeMillis();
    if (failed == 0) {
      System.out.println(Thread.currentThread().getName() + " deleted "+ numRecords + 
        " RPS=" + ((long) numRecords) * 1000 / (end - start));
    } else {
      System.err.println(Thread.currentThread().getName() + " deleted "+ numRecords + 
        " RPS=" + ((long) numRecords) * 1000 / (end - start) + " failed="+ failed);
    }
    return failed;
  }
  
  protected int deleteIndexMemory() {
    int failed = 0;
    long[] mKeys = mKeysTL.get();
    long start = System.currentTimeMillis();
    for(int i = 0; i < numRecords; i++) {
      boolean result = memoryIndex.delete(mKeys[i], keySize);
      if (result != true) {
        failed++;
      }
    }

    long end = System.currentTimeMillis();
    if (failed == 0) {
      System.out.println(Thread.currentThread().getName() + " deleted "+ numRecords + 
        " RPS=" + ((long) numRecords) * 1000 / (end - start));
    } else {
      System.err.println(Thread.currentThread().getName() + " deleted "+ numRecords + 
        " RPS=" + ((long) numRecords) * 1000 / (end - start) +" failed="+ failed);
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
        failed ++;
      }
    }
    assertEquals(0, failed);

    long end = System.currentTimeMillis();
    System.out.println(Thread.currentThread().getName() + " verify NOT "+ numRecords + 
      " RPS=" + ((long) numRecords) * 1000 / (end - start));
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
    System.out.println(Thread.currentThread().getName() + " verify NOT "+ numRecords + 
      " RPS=" + ((long) numRecords) * 1000 / (end - start));
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
    while(result == false) {
      result = memoryIndex.delete(key, offset, size);
      if (!result) {
        //Thread.onSpinWait();
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
  protected boolean forceDelete(long key,  int size) {
    boolean result = false;
    while(result == false) {
      result = memoryIndex.delete(key, size);
      if (!result) {
//        Thread.onSpinWait();
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
    while(result != MutationResult.INSERTED) {
      result = memoryIndex.aarp(key, offset, size);
      if (result != MutationResult.INSERTED) {
//        Thread.onSpinWait();
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
    while(result != MutationResult.INSERTED) {
      result = memoryIndex.aarp(key, size);
      if (result != MutationResult.INSERTED) {
//        Thread.onSpinWait();
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
    while(result != MutationResult.DELETED) {
      result = memoryIndex.aarp(key, offset, size);
      if (result != MutationResult.DELETED) {
//        Thread.onSpinWait();
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
    while(result != MutationResult.DELETED) {
      result = memoryIndex.aarp(key, size);
      if (result != MutationResult.DELETED) {
//        Thread.onSpinWait();
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
    while(result != MutationResult.INSERTED) {
      result = memoryIndex.insert(key, offset, size, buf, bufSize);
      if (result != MutationResult.INSERTED) {
//        Thread.onSpinWait();
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
    while(result != MutationResult.INSERTED) {
      result = memoryIndex.insert(key, size, buf, bufSize);
      if (result != MutationResult.INSERTED) {
//        Thread.onSpinWait();
        Utils.onSpinWait(1000);
      }
    }
    return MutationResult.INSERTED;
  }
}
