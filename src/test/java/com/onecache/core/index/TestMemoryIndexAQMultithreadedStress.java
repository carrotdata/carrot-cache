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
package com.onecache.core.index;

import static org.junit.Assert.assertEquals;

import java.util.concurrent.atomic.AtomicLong;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.onecache.core.index.MemoryIndex.MutationResult;
import com.onecache.core.util.UnsafeAccess;
import com.onecache.core.util.Utils;

/**
 * 
 *  Test cases for Memory Index of Admission Queue (AQ) 
 */
public abstract class TestMemoryIndexAQMultithreadedStress extends TestMemoryIndexMultithreadedBase{
  /** Logger */
  private static final Logger LOG = LogManager.getLogger(TestMemoryIndexAQMultithreadedStress.class);
  
  @Before
  public void setUp() {
    UnsafeAccess.debug = false;
    UnsafeAccess.mallocStats.clear();
    memoryIndex = new MemoryIndex("default", MemoryIndex.Type.AQ);
    //memoryIndex.setMaximumSize(10000000);
    numThreads = 4;
  }
  
  private int loadIndexBytes() {
    int failed = 0;
    byte[][] keys = keysTL.get(); 
    long start = System.currentTimeMillis();
    for(int i = 0; i < numRecords; i++) {
      MutationResult result = forceAarp(keys[i], 0, keySize);
      if (result == MutationResult.FAILED) {
        failed ++;
      } else {
        assertEquals(MutationResult.INSERTED, result);
      }
    }
    assertEquals(0, failed);
    
    long end = System.currentTimeMillis();
    System.out.println(Thread.currentThread().getName() + " loaded "+ numRecords + 
      " RPS=" + ((long) numRecords) * 1000 / (end - start));
    return failed;
  }
  
  private int verifyIndexBytes() {
    int failed = 0;
    int entrySize = memoryIndex.getIndexFormat().indexEntrySize();
    long buf = UnsafeAccess.mallocZeroed(entrySize);
    byte[][] keys = keysTL.get(); 
    long start = System.currentTimeMillis();
    for (int i = 0; i < numRecords; i++) {
      long hash = Utils.hash64(keys[i], 0, keySize);
      int result = (int) memoryIndex.find(keys[i], 0, keySize, false, buf, entrySize);
      if (result < 0) {
        failed ++;
      } else {
        assertEquals(entrySize, result);
        assertEquals(hash, UnsafeAccess.toLong(buf));
      }
    }
    assertEquals(0, failed);

    long end = System.currentTimeMillis();
    System.out.println(Thread.currentThread().getName() + " verified "+ numRecords + 
      " RPS=" + ((long) numRecords) * 1000 / (end - start));
    UnsafeAccess.free(buf);
    return failed;
  }
  
  private int loadIndexBytesNot() {
    int failed = 0;
    byte[][] keys = keysTL.get(); 
    long start = System.currentTimeMillis();
    for(int i = 0; i < numRecords; i++) {
      MutationResult result = forceAarpNot(keys[i], 0, keySize);
      if (result != MutationResult.DELETED) {
        failed ++;
      }
    }
    assertEquals(0, failed);

    long end = System.currentTimeMillis();
    System.out.println(Thread.currentThread().getName() + " loaded NOT "+ numRecords + 
      " RPS=" + ((long) numRecords) * 1000 / (end - start));
    return failed;
  }
    
  private int loadIndexMemory() {
    int failed = 0;
    long[] mKeys = mKeysTL.get();
    long start = System.currentTimeMillis();
    for(int i = 0; i < numRecords; i++) {
      MutationResult result = forceAarp(mKeys[i], keySize);
      if (result != MutationResult.INSERTED) {
        failed++;
      }
    }
    assertEquals(0, failed);

    long end = System.currentTimeMillis();
    System.out.println(Thread.currentThread().getName() + " loaded "+ numRecords + 
      " RPS=" + ((long) numRecords) * 1000 / (end - start));
    return failed;
  }
    
  private int verifyIndexMemory() {
    int failed = 0;
    int entrySize = memoryIndex.getIndexFormat().indexEntrySize();
    long buf = UnsafeAccess.mallocZeroed(entrySize);
    long[] mKeys = mKeysTL.get();
    long start = System.currentTimeMillis();
    for (int i = 0; i < numRecords; i++) {
      long hash = Utils.hash64(mKeys[i], keySize);
      int result = (int) memoryIndex.find(mKeys[i], keySize, false, buf, entrySize);
      if (result < 0) {
        failed++;
      } else {
        assertEquals(entrySize, result);
        assertEquals(hash, UnsafeAccess.toLong(buf));
      }
    }
    assertEquals(0, failed);

    long end = System.currentTimeMillis();
    System.out.println(Thread.currentThread().getName() + " verified "+ numRecords + 
      " RPS=" + ((long) numRecords) * 1000 / (end - start));
    UnsafeAccess.free(buf);
    return failed;
  }
  
  private int loadIndexMemoryNot() {
    int failed = 0;
    long[] mKeys = mKeysTL.get();
    long start = System.currentTimeMillis();
    for(int i = 0; i < numRecords; i++) {
      MutationResult result = forceAarpNot(mKeys[i], keySize);
      if (result != MutationResult.DELETED) {
        failed++;
      }
    }
    assertEquals(0, failed);

    long end = System.currentTimeMillis();
    System.out.println(Thread.currentThread().getName() + " loaded NOT "+ numRecords + 
      " RPS=" + ((long) numRecords) * 1000 / (end - start));
    return failed;
  }
  
  
  @Test
  public void testLoadReadWithRehashBytesMTLarge() {
    /*DEBUG*/ System.out.println("testLoadReadWithRehashBytesMTLarge");

    Runnable r = () -> testLoadReadWithRehashBytesLarge();
    Thread[] workers = startAll(r);    
    joinAll(workers);
  }
  
  private void testLoadReadWithRehashBytesLarge() {
    LOG.info("Test load and read with rehash bytes");
    loadReadBytes(10000000);
    clearData();
  }
  
  @Test
  public void testLoadReadWithRehashMemoryMTLarge() {
    /*DEBUG*/ System.out.println("testLoadReadWithRehashMemoryMTLarge");

    Runnable r = () -> testLoadReadWithRehashMemoryLarge();
    Thread[] workers = startAll(r);    
    joinAll(workers);
  }

  private void testLoadReadWithRehashMemoryLarge() {
    LOG.info("Test load and read with rehash memory");
    loadReadMemory(10000000);
    clearData();
  }
  
  @Test
  public void testLoadReadDeleteWithRehashBytesMTLarge() {
    /*DEBUG*/ System.out.println("testLoadReadDeleteWithRehashBytesMTLarge");

    Runnable r = () -> testLoadReadDeleteWithRehashBytesLarge();
    Thread[] workers = startAll(r);    
    joinAll(workers);
  }
  
  private void testLoadReadDeleteWithRehashBytesLarge() {
    LOG.info("Test load and read-delete with rehash bytes");
    int failed = loadReadBytes(10000000);
    int undeleted = deleteIndexBytes();
    assertEquals(failed, undeleted);
    int unverified = verifyIndexBytesNot();
    assertEquals(0, unverified);
    clearData();
  }
  
  @Test
  public void testLoadReadDeleteWithRehashMemoryMTLarge() {
    /*DEBUG*/ System.out.println("testLoadReadDeleteWithRehashMemoryMTLarge");

    Runnable r = () -> testLoadReadDeleteWithRehashMemoryLarge();
    Thread[] workers = startAll(r);    
    joinAll(workers);
  }
  
  private void testLoadReadDeleteWithRehashMemoryLarge() {
    LOG.info("Test load and read with rehash memory");
    int failed = loadReadMemory(10000000);
    int undeleted = deleteIndexMemory();
    assertEquals(failed, undeleted);
    int unverified = verifyIndexMemoryNot();
    assertEquals(0, unverified);
    clearData();
  }
  
  @Test
  public void testDoubleLoadReadWithRehashBytesMTLarge() {
    /*DEBUG*/ System.out.println("testDoubleLoadReadWithRehashBytesMTLarge");

    Runnable r = () -> testDoubleLoadReadWithRehashBytesLarge();
    Thread[] workers = startAll(r);    
    joinAll(workers);
  }
  
  private void testDoubleLoadReadWithRehashBytesLarge() {
    LOG.info("Test double load and read with rehash bytes");
    doubleLoadReadBytes(10000000);
    clearData();
  }
  
  @Test
  public void testDoubleLoadReadWithRehashMemoryMTLarge() {
    /*DEBUG*/ System.out.println("testDoubleLoadReadWithRehashMemoryMTLarge");

    Runnable r = () -> testDoubleLoadReadWithRehashMemoryLarge();
    Thread[] workers = startAll(r);    
    joinAll(workers);
  }
  
  private void testDoubleLoadReadWithRehashMemoryLarge() {
    LOG.info("Test double load and read with rehash memory LARGE");
    doubleLoadReadMemory(10000000);
    clearData();
  }
  
  private AtomicLong evicted1 = new AtomicLong(0);
  private AtomicLong evicted2 = new AtomicLong(0);
  
  @Ignore
  @Test
  public void testEvictionBytes() {
    memoryIndex.setMaximumSize(numThreads * 100000);
    
    Runnable r = () -> evictionBytes();
    Thread[] workers = startAll(r);    
    joinAll(workers);
    //assertEquals((long) numThreads * 100000, evicted1.get() + evicted2.get());
    assertEquals((long) numThreads * 100000, memoryIndex.size());
  }
  
  private void evictionBytes() {
    //FIXME
    LOG.info("Test eviction bytes");

    prepareData(200000);
    loadIndexBytes();
    byte[][] keys = keysTL.get(); 

    
    int entrySize = memoryIndex.getIndexFormat().indexEntrySize();
    long buf = UnsafeAccess.mallocZeroed(entrySize);
    
    for (int i = 0; i < 100000; i++) {
      int result = (int) memoryIndex.find(keys[i], 0, keySize, false, buf, entrySize);
      if (result == -1) evicted1.incrementAndGet();
    }
    
    for (int i = 100000; i < 200000; i++) {
      int result = (int) memoryIndex.find(keys[i], 0, keySize, false, buf, entrySize);
      if (result == -1) evicted2.incrementAndGet();
    }
    System.out.println("evicted1=" + evicted1 + " evicted2="+ evicted2);
    UnsafeAccess.free(buf); 
    clearData();
  }
  
  @Ignore
  @Test
  public void testEvictionMemory() {
    //FIXME
    LOG.info("Test eviction memory");

    memoryIndex.setMaximumSize(100000);
    prepareData(200000);
    loadIndexMemory();
    
    long size = memoryIndex.size();
    assertEquals(100000L, size);
    
    int entrySize = memoryIndex.getIndexFormat().indexEntrySize();
    long buf = UnsafeAccess.mallocZeroed(entrySize);
    long[] mKeys = mKeysTL.get();
    int evicted1 = 0;
    int evicted2 = 0;
    for (int i = 0; i < 100000; i++) {
      int result = (int) memoryIndex.find(mKeys[i], keySize, false, buf, entrySize);
      if (result == -1) evicted1++;
    }
    
    for (int i = 100000; i < 200000; i++) {
      int result = (int) memoryIndex.find(mKeys[i], keySize, false, buf, entrySize);
      if (result == -1) evicted2 ++;
    }
    System.out.println("evicted1=" + evicted1 + " evicted2="+ evicted2);
    assertEquals(100000, evicted1 + evicted2);
    UnsafeAccess.free(buf);
    clearData();
  }
  
  
  private int loadReadBytes(int num) {
    prepareData(num);
    System.out.println(Thread.currentThread().getName() + ": prepare done");
    int failed =loadIndexBytes();
    System.out.println(Thread.currentThread().getName() + ": load done");
    int unverified = verifyIndexBytes();  
    assertEquals(failed, unverified);
    return failed;
  }
  
  private int doubleLoadReadBytes(int num) {
    prepareData(num);
    int failed = loadIndexBytes();
    int unverified = verifyIndexBytes();
    assertEquals(failed, unverified);
    failed = loadIndexBytesNot();
    unverified = verifyIndexBytesNot();
    assertEquals(failed, unverified);
    return failed;
  }
  
  private int loadReadMemory(int num) {
    prepareData(num);
    int failed = loadIndexMemory();
    int unverified = verifyIndexMemory();
    assertEquals(failed, unverified);
    return failed;
  }
  
  private int doubleLoadReadMemory(int num) {
    prepareData(num);
    int failed = loadIndexMemory();
    int unverified = verifyIndexMemory();
    assertEquals(failed, unverified);

    failed = loadIndexMemoryNot();
    unverified = verifyIndexMemoryNot();
    assertEquals(failed, unverified);
    return failed;
  }
}
