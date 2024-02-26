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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.onecache.core.util.UnsafeAccess;

public class TestMemoryIndexMQMultithreaded extends TestMemoryIndexMultithreadedBase{
  /** Logger */
  private static final Logger LOG = LogManager.getLogger(TestMemoryIndexMQMultithreaded.class);
  
  @Before
  public void setUp() {
    UnsafeAccess.debug = false;
    UnsafeAccess.mallocStats.clear();
    memoryIndex = new MemoryIndex("default", MemoryIndex.Type.MQ);
    numThreads = 4;
  }
  
  private void loadIndexBytes() {
    IndexFormat format = memoryIndex.getIndexFormat();
    int entrySize = format.indexEntrySize();
    long buf = UnsafeAccess.mallocZeroed(entrySize);
    byte[][] keys = keysTL.get();
    byte[][] values = valuesTL.get();
    short[] sids = sidsTL.get();
    int[] offsets = offsetsTL.get();
    int[] lengths = lengthsTL.get();
    long start = System.currentTimeMillis();
    for(int i = 0; i < numRecords; i++) {
      format.writeIndex(0L, buf, keys[i], 0, keys[i].length, values[i], 0, values[i].length, 
        sids[i], offsets[i], lengths[i], 0);
      forceInsert(keys[i], 0, keySize, buf, entrySize);
    }
    long end = System.currentTimeMillis();
    System.out.println(Thread.currentThread().getName() + " loaded "+ numRecords + 
      " RPS=" + ((long) numRecords) * 1000 / (end - start));
    UnsafeAccess.free(buf);
  }
  
  private void loadIndexBytesWithEviction(int evictionStartFrom) {
    IndexFormat format = memoryIndex.getIndexFormat();
    int entrySize = format.indexEntrySize();
    long buf = UnsafeAccess.mallocZeroed(entrySize);
    byte[][] keys = keysTL.get();
    byte[][] values = valuesTL.get();
    short[] sids = sidsTL.get();
    int[] offsets = offsetsTL.get();
    int[] lengths = lengthsTL.get();
    for(int i = 0; i < numRecords; i++) {
      format.writeIndex(0L, buf, keys[i], 0, keys[i].length, values[i], 0, values[i].length, 
        sids[i], offsets[i], lengths[i], 0);
      forceInsert(keys[i], 0, keySize, buf, entrySize);
      if (i == evictionStartFrom - 1) {
        memoryIndex.setEvictionEnabled(true);
      }
    }
    UnsafeAccess.free(buf);
  }
  
  private void verifyIndexBytes() {
    verifyIndexBytes(false);
  }
  
  private void verifyIndexBytes(boolean hit) {
    
    IndexFormat format = memoryIndex.getIndexFormat();
    int entrySize = format.indexEntrySize();
    long buf = UnsafeAccess.mallocZeroed(entrySize);
    byte[][] keys = keysTL.get();
    short[] sids = sidsTL.get();
    int[] offsets = offsetsTL.get();
    int[] lengths = lengthsTL.get();
    int failed = 0;
    long start = System.currentTimeMillis();
    for (int i = 0; i < numRecords; i++) {
      int result = (int) memoryIndex.find(keys[i], 0, keySize, hit, buf, entrySize);
      if (result < 0) {
        failed ++;
        continue;
      }
      assertEquals(entrySize, result);
      short sid = (short)format.getSegmentId(buf);
      int offset = (int) format.getOffset(buf);
      int size = (int) format.getKeyValueSize(buf);
      assertEquals(sids[i], sid);
      assertEquals(offsets[i], offset);
      assertEquals(lengths[i], size);
    }
    UnsafeAccess.free(buf);
    assertEquals(0, failed);
    long end = System.currentTimeMillis();
    System.out.println(Thread.currentThread().getName() + " verify "+ numRecords + 
      " RPS=" + ((long) numRecords) * 1000 / (end - start));
    
  }
  
  private void loadIndexMemory() {
    IndexFormat format = memoryIndex.getIndexFormat();
    int entrySize = format.indexEntrySize();
    long buf = UnsafeAccess.mallocZeroed(entrySize);
    long[] mKeys = mKeysTL.get();
    long[] mValues = mValuesTL.get();
    short[] sids = sidsTL.get();
    int[] offsets = offsetsTL.get();
    int[] lengths = lengthsTL.get();
    long start = System.currentTimeMillis();
    for(int i = 0; i < numRecords; i++) {
      format.writeIndex(0L, buf, mKeys[i], keySize, mValues[i], valueSize, 
        sids[i], offsets[i], lengths[i], 0);
      forceInsert(mKeys[i], keySize, buf, entrySize);
    }
    long end = System.currentTimeMillis();
    System.out.println(Thread.currentThread().getName() + " loaded "+ numRecords + 
      " RPS=" + ((long) numRecords) * 1000 / (end - start));
    UnsafeAccess.free(buf);
  }
  
  private void loadIndexMemoryWithEviction(int evictionStartFrom) {
    IndexFormat format = memoryIndex.getIndexFormat();
    int entrySize = format.indexEntrySize();
    long buf = UnsafeAccess.mallocZeroed(entrySize);
    long[] mKeys = mKeysTL.get();
    long[] mValues = mValuesTL.get();
    short[] sids = sidsTL.get();
    int[] offsets = offsetsTL.get();
    int[] lengths = lengthsTL.get();
    for(int i = 0; i < numRecords; i++) {
      format.writeIndex(0L, buf, mKeys[i], keySize, mValues[i], valueSize, 
        sids[i], offsets[i], lengths[i], 0);
      forceInsert(mKeys[i], keySize, buf, entrySize);
      if (i == evictionStartFrom - 1) {
        memoryIndex.setEvictionEnabled(true);
      }
    }
    UnsafeAccess.free(buf);
  }
  
  private void verifyIndexMemory() {
    verifyIndexMemory(false);
  }
  
  private void verifyIndexMemory(boolean hit) {
    IndexFormat format = memoryIndex.getIndexFormat();
    int entrySize = format.indexEntrySize();
    long buf = UnsafeAccess.mallocZeroed(entrySize);
    long[] mKeys = mKeysTL.get();
    short[] sids = sidsTL.get();
    int[] offsets = offsetsTL.get();
    int[] lengths = lengthsTL.get();
    int failed = 0;
    long start = System.currentTimeMillis();
    for (int i = 0; i < numRecords; i++) {
      int result = (int) memoryIndex.find(mKeys[i], keySize, hit, buf, entrySize);
      if (result < 0) {
        failed++;
        continue;
      }
      assertEquals(entrySize, result);
      short sid = (short)format.getSegmentId(buf);
      int offset = (int) format.getOffset(buf);
      int size = (int) format.getKeyValueSize(buf);
      assertEquals(sids[i], sid);
      assertEquals(offsets[i], offset);
      assertEquals(lengths[i], size);
    }
    assertEquals(0, failed);
    long end = System.currentTimeMillis();
    System.out.println(Thread.currentThread().getName() + " verify "+ numRecords + 
      " RPS=" + ((long) numRecords) * 1000 / (end - start));
    UnsafeAccess.free(buf);
    
  }
  
  @Test
  public void testLoadReadNoRehashBytesMT() {
    /*DEBUG*/ System.out.println("testLoadReadNoRehashBytesMT");
    Runnable r = () -> testLoadReadNoRehashBytes();
    Thread[] workers = startAll(r);    
    joinAll(workers);
  }
  
  private void testLoadReadNoRehashBytes() {
    LOG.info("Test load and read no rehash bytes");
    loadReadBytes(100000);
    clearData();
  }
  
  @Test
  public void testLoadReadNoRehashBytesMTWithHit() {
    /*DEBUG*/ System.out.println("testLoadReadNoRehashBytesMT with hit");
    Runnable r = () -> testLoadReadNoRehashBytesWithHit();
    Thread[] workers = startAll(r);    
    joinAll(workers);
  }
  
  private void testLoadReadNoRehashBytesWithHit() {
    LOG.info("Test load and read no rehash bytes");
    loadReadBytesWithHit(100000);
    clearData();
  }
  
  @Test
  public void testLoadReadNoRehashMemoryMT() {
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
  public void testLoadReadNoRehashMemoryMTWithHit() {
    /*DEBUG*/ System.out.println("testLoadReadNoRehashMemoryMT with hit");
    Runnable r = () -> testLoadReadNoRehashMemoryWithHit();
    Thread[] workers = startAll(r);    
    joinAll(workers);
  }
  
  private void testLoadReadNoRehashMemoryWithHit() {
    LOG.info("Test load and read no rehash memory");
    loadReadMemoryWithHit(100000);
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
    loadReadBytes(100000);
    deleteIndexBytes();
    verifyIndexBytesNot();
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
    loadReadMemory(100000);
    deleteIndexMemory();
    verifyIndexMemoryNot();
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
  public void testLoadReadWithRehashBytesMTWithHit() {
    /*DEBUG*/ System.out.println("testLoadReadWithRehashBytesMT with hit");
    Runnable r = () -> testLoadReadWithRehashBytesWithHit();
    Thread[] workers = startAll(r);    
    joinAll(workers);
  }
  
  private void testLoadReadWithRehashBytesWithHit() {
    LOG.info("Test load and read with rehash memory");
    loadReadBytesWithHit(1000000);
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
  public void testLoadReadWithRehashMemoryMTWithHit() {
    /*DEBUG*/ System.out.println("testLoadReadWithRehashMemoryMT with hit");

    Runnable r = () -> testLoadReadWithRehashMemoryWithHit();
    Thread[] workers = startAll(r);    
    joinAll(workers);
  }
  
  private void testLoadReadWithRehashMemoryWithHit() {
    LOG.info("Test load and read with rehash memory with hit");
    loadReadMemoryWithHit(1000000);
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
    loadReadBytes(1000000);
    deleteIndexBytes();
    verifyIndexBytesNot();
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
    loadReadMemory(1000000);
    deleteIndexMemory();
    verifyIndexMemoryNot();
    clearData();
  }
  
  @Ignore
  @Test
  public void testEvictionBytes() {
    //FIXME
    LOG.info("Test eviction bytes");

    prepareData(200000);
    loadIndexBytesWithEviction(100000);
    
    long size = memoryIndex.size();
    assertEquals(100000L, size);
    
    int entrySize = memoryIndex.getIndexFormat().indexEntrySize();
    long buf = UnsafeAccess.mallocZeroed(entrySize);
    byte[][] keys = keysTL.get();
    
    int evicted1 = 0;
    int evicted2 = 0;
    for (int i = 0; i < 100000; i++) {
      int result = (int) memoryIndex.find(keys[i], 0, keySize, false, buf, entrySize);
      if (result == -1) evicted1++;
    }
    
    for (int i = 100000; i < 200000; i++) {
      int result = (int) memoryIndex.find(keys[i], 0, keySize, false, buf, entrySize);
      if (result == -1) evicted2 ++;
    }
    System.out.println("evicted1=" + evicted1 + " evicted2="+ evicted2);
    assertEquals(100000, evicted1 + evicted2);
    UnsafeAccess.free(buf); 
  }
  
  @Ignore
  @Test
  public void testEvictionMemory() {
    LOG.info("Test eviction memory");

    prepareData(200000);
    loadIndexMemoryWithEviction(100000);
    
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
  }
  
  private void loadReadBytes(int num) {
    prepareData(num);
    loadIndexBytes();
    verifyIndexBytes();
  }
  
  private void loadReadMemory(int num) {
    prepareData(num);
    loadIndexMemory();
    verifyIndexMemory();
  }
  
  private void loadReadBytesWithHit(int num) {
    loadReadBytes(num);
    verifyIndexBytes(true);
    verifyIndexBytes();
  }
  
  private void loadReadMemoryWithHit(int num) {
    loadReadMemory(num);
    verifyIndexMemory(true);
    verifyIndexMemory();
  }
}
