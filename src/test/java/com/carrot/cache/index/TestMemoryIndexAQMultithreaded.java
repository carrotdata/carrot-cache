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

import java.util.concurrent.atomic.AtomicLong;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.carrot.cache.index.MemoryIndex.MutationResult;
import com.carrot.cache.util.UnsafeAccess;
import com.carrot.cache.util.Utils;

/**
 * 
 *  Test cases for Memory Index of Admission Queue (AQ) 
 */
public class TestMemoryIndexAQMultithreaded extends TestMemoryIndexMultithreadedBase{
  /** Logger */
  private static final Logger LOG = LogManager.getLogger(TestMemoryIndexAQMultithreaded.class);
  
  @Before
  public void setUp() {
    UnsafeAccess.debug = false;
    UnsafeAccess.mallocStats.clear();
    memoryIndex = new MemoryIndex("default", MemoryIndex.Type.AQ);
    memoryIndex.setMaximumSize(10000000);
    numThreads = 4;
  }
  
  private int loadIndexBytes() {
    int failed = 0;
    byte[][] keys = keysTL.get(); 
    for(int i = 0; i < numRecords; i++) {
      MutationResult result = forceAarp(keys[i], 0, keySize);
      if (result == MutationResult.FAILED) {
        failed ++;
      } else {
        assertEquals(MutationResult.INSERTED, result);
      }
    }
    return failed;
  }
  
  private int verifyIndexBytes() {
    int failed = 0;
    int entrySize = memoryIndex.getIndexFormat().indexEntrySize();
    long buf = UnsafeAccess.mallocZeroed(entrySize);
    byte[][] keys = keysTL.get(); 

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
    UnsafeAccess.free(buf);
    return failed;
  }
  
  private int loadIndexBytesNot() {
    int failed = 0;
    byte[][] keys = keysTL.get(); 
    for(int i = 0; i < numRecords; i++) {
      MutationResult result = forceAarpNot(keys[i], 0, keySize);
      if (result != MutationResult.DELETED) {
        failed ++;
      }
    }
    return failed;
  }
    
  private int loadIndexMemory() {
    int failed = 0;
    long[] mKeys = mKeysTL.get();
    for(int i = 0; i < numRecords; i++) {
      MutationResult result = forceAarp(mKeys[i], keySize);
      if (result != MutationResult.INSERTED) {
        failed++;
      }
    }
    return failed;
  }
    
  private int verifyIndexMemory() {
    int failed = 0;
    int entrySize = memoryIndex.getIndexFormat().indexEntrySize();
    long buf = UnsafeAccess.mallocZeroed(entrySize);
    long[] mKeys = mKeysTL.get();

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
    UnsafeAccess.free(buf);
    return failed;
  }
  
  private int loadIndexMemoryNot() {
    int failed = 0;
    long[] mKeys = mKeysTL.get();
    for(int i = 0; i < numRecords; i++) {
      MutationResult result = forceAarpNot(mKeys[i], keySize);
      if (result != MutationResult.DELETED) {
        failed++;
      }
    }
    return failed;
  }
  
  @Test
  public void testLoadReadNoRehashBytesMT(){
    /*DEBUG*/ System.out.println("testLoadReadNoRehashBytesMT");
    Runnable r = () -> testLoadReadNoRehashBytes();
    Thread[] workers = startAll(r);    
    joinAll(workers);
  }
  
  private void testLoadReadNoRehashBytes() {
    LOG.info(Thread.currentThread().getName() + ": Test load and read no rehash bytes");
    loadReadBytes(100000);
    clearData();
  }
  
  @Test
  public void testLoadReadNoRehashMemoryMT(){
    /*DEBUG*/ System.out.println("testLoadReadNoRehashMemoryMT");

    Runnable r = () -> testLoadReadNoRehashMemory();
    Thread[] workers = startAll(r);    
    joinAll(workers);
  }
  
  private void testLoadReadNoRehashMemory() {
    LOG.info("Test load and read no rehash memory");
    loadReadMemory(100000);
    clearData();
  }
  
  @Test
  public void testLoadReadDeleteNoRehashBytesMT() {
    /*DEBUG*/ System.out.println("testLoadReadDeleteNoRehashBytesMT");
    Runnable r = () -> testLoadReadDeleteNoRehashBytes();
    Thread[] workers = startAll(r);    
    joinAll(workers);
  }

  private void testLoadReadDeleteNoRehashBytes() {
    LOG.info("Test load and read-delete no rehash bytes");
    int failed = loadReadBytes(100000);
    int undeleted = deleteIndexBytes();
    assertEquals(failed, undeleted);
    int unverified = verifyIndexBytesNot();
    assertEquals(0, unverified);
    clearData();
  }
  
  @Test
  public void testLoadReadDeleteNoRehashMemoryMT() {
    /*DEBUG*/ System.out.println("testLoadReadDeleteNoRehashMemoryMT");
    Runnable r = () -> testLoadReadDeleteNoRehashMemory();
    Thread[] workers = startAll(r);    
    joinAll(workers);
  }
  
  private void testLoadReadDeleteNoRehashMemory() {
    LOG.info("Test load and read-delete no rehash memory");
    int failed = loadReadMemory(100000);
    int undeleted = deleteIndexMemory();
    assertEquals(failed, undeleted);
    int unverified = verifyIndexMemoryNot();
    assertEquals(0, unverified);
    clearData();
  }

  @Test
  public void testDoubleLoadReadNoRehashBytesMT() {
    /*DEBUG*/ System.out.println("testDoubleLoadReadNoRehashBytesMT");

    Runnable r = () -> testDoubleLoadReadNoRehashBytes();
    Thread[] workers = startAll(r);    
    joinAll(workers);
  }
  
  private void testDoubleLoadReadNoRehashBytes() {
    LOG.info("Test double load and read no rehash bytes");
    doubleLoadReadBytes(100000);
    clearData();
  }
  
  @Test
  public void testDoubleLoadReadNoRehashMemoryMT() {
    /*DEBUG*/ System.out.println("testDoubleLoadReadNoRehashMemoryMT");

    Runnable r = () -> testDoubleLoadReadNoRehashMemory();
    Thread[] workers = startAll(r);    
    joinAll(workers);
  }
  
  private void testDoubleLoadReadNoRehashMemory() {
    LOG.info("Test double load and read no rehash memory");
    doubleLoadReadMemory(100000);
    clearData();
  }
  
  @Test
  public void testLoadReadWithRehashBytesMT() {
    /*DEBUG*/ System.out.println("testLoadReadWithRehashBytesMT");

    Runnable r = () -> testLoadReadWithRehashBytes();
    Thread[] workers = startAll(r);    
    joinAll(workers);
  }
  
  private void testLoadReadWithRehashBytes() {
    LOG.info("Test load and read with rehash bytes");
    loadReadBytes(1000000);
    clearData();
  }
  
  @Test
  public void testLoadReadWithRehashMemoryMT() {
    /*DEBUG*/ System.out.println("testLoadReadWithRehashMemoryMT");

    Runnable r = () -> testLoadReadWithRehashMemory();
    Thread[] workers = startAll(r);    
    joinAll(workers);
  }

  private void testLoadReadWithRehashMemory() {
    LOG.info("Test load and read with rehash memory");
    loadReadMemory(1000000);
    clearData();
  }
  
  @Test
  public void testLoadReadDeleteWithRehashBytesMT() {
    /*DEBUG*/ System.out.println("testLoadReadDeleteWithRehashBytesMT");

    Runnable r = () -> testLoadReadDeleteWithRehashBytes();
    Thread[] workers = startAll(r);    
    joinAll(workers);
  }
  
  private void testLoadReadDeleteWithRehashBytes() {
    LOG.info("Test load and read-delete with rehash bytes");
    int failed = loadReadBytes(1000000);
    int undeleted = deleteIndexBytes();
    assertEquals(failed, undeleted);
    int unverified = verifyIndexBytesNot();
    assertEquals(0, unverified);
    clearData();
  }
  
  @Test
  public void testLoadReadDeleteWithRehashMemoryMT() {
    /*DEBUG*/ System.out.println("testLoadReadDeleteWithRehashMemoryMT");

    Runnable r = () -> testLoadReadDeleteWithRehashMemory();
    Thread[] workers = startAll(r);    
    joinAll(workers);
  }
  
  private void testLoadReadDeleteWithRehashMemory() {
    LOG.info("Test load and read with rehash memory");
    int failed = loadReadMemory(1000000);
    int undeleted = deleteIndexMemory();
    assertEquals(failed, undeleted);
    int unverified = verifyIndexMemoryNot();
    assertEquals(0, unverified);
    clearData();
  }
  
  @Test
  public void testDoubleLoadReadWithRehashBytesMT() {
    /*DEBUG*/ System.out.println("testDoubleLoadReadWithRehashBytesMT");

    Runnable r = () -> testDoubleLoadReadWithRehashBytes();
    Thread[] workers = startAll(r);    
    joinAll(workers);
  }
  
  private void testDoubleLoadReadWithRehashBytes() {
    LOG.info("Test double load and read with rehash bytes");
    doubleLoadReadBytes(1000000);
    clearData();
  }
  
  @Test
  public void testDoubleLoadReadWithRehashMemoryMT() {
    /*DEBUG*/ System.out.println("testDoubleLoadReadWithRehashMemoryMT");

    Runnable r = () -> testDoubleLoadReadWithRehashMemory();
    Thread[] workers = startAll(r);    
    joinAll(workers);
  }
  
  private void testDoubleLoadReadWithRehashMemory() {
    LOG.info("Test double load and read with rehash memory");
    doubleLoadReadMemory(1000000);
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
