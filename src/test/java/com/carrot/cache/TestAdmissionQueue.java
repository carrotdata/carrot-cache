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
package com.carrot.cache;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.carrot.cache.index.MemoryIndex;
import com.carrot.cache.util.TestUtils;
import com.carrot.cache.util.UnsafeAccess;
import com.carrot.cache.util.Utils;

public class TestAdmissionQueue {
  /** Logger */
  private static final Logger LOG = LogManager.getLogger(TestAdmissionQueue.class);
  AdmissionQueue queue;
  MemoryIndex memoryIndex;
  
  int numRecords = 10;
  byte[][] keys = new byte[numRecords][];
  byte[][] values = new byte[numRecords][];
  long[] mKeys = new long[numRecords];
  long[] mValues = new long[numRecords];
  short[] sids = new short[numRecords];
  int[] offsets = new int[numRecords];
  int[] lengths = new int[numRecords];
  
  static int keySize = 16;
  static int valueSize = 16;
  
  @BeforeClass
  public static void enableMallocDebug() {
//    UnsafeAccess.setMallocDebugEnabled(true);
//    UnsafeAccess.setMallocDebugStackTraceEnabled(true);
//    UnsafeAccess.setStackTraceRecordingFilter(x -> x == 1024);
//    UnsafeAccess.setStackTraceRecordingLimit(20000);
  }
  
  @After
  public void tearDown() {
    queue.dispose();
    Arrays.stream(mKeys).forEach(x -> UnsafeAccess.free(x));
    Arrays.stream(mValues).forEach(x -> UnsafeAccess.free(x));
    UnsafeAccess.mallocStats.printStats();
  }
  
  protected void prepareData(int numRecords) {
    this.numRecords = numRecords;
    keys = new byte[numRecords][];
    values = new byte[numRecords][];
    mKeys = new long[numRecords];
    mValues = new long[numRecords];
    sids = new short[numRecords];
    offsets = new int[numRecords];
    lengths = new int[numRecords];
    
    Random r = new Random();
    long seed = System.currentTimeMillis();
    r.setSeed(seed);
    System.out.println("seed="+ seed);
    
    for (int i = 0; i < numRecords; i++) {
      keys[i] = TestUtils.randomBytes(keySize, r);
      values[i] = TestUtils.randomBytes(valueSize, r);
      mKeys[i] = TestUtils.randomMemory(keySize, r);
      mValues[i] = TestUtils.randomMemory(valueSize, r);
      sids[i] = (short) r.nextInt(1000);
      offsets[i] = r.nextInt(100000);
      lengths[i] = r.nextInt(10000);
    }  
  }
  
  
  protected void deleteIndexBytes() {
    for(int i = 0; i < numRecords; i++) {
      boolean result = memoryIndex.delete(keys[i], 0, keySize);
      assertTrue(result);
    }
    assertEquals(0L, memoryIndex.size());
  }
  
  protected void deleteIndexMemory() {
    for(int i = 0; i < numRecords; i++) {
      boolean result = memoryIndex.delete(mKeys[i], keySize);
      assertTrue(result);
    }
    assertEquals(0L, memoryIndex.size());
  }
  
  protected void verifyIndexBytesNot() {
    int entrySize = memoryIndex.getIndexFormat().indexEntrySize();
    long buf = UnsafeAccess.mallocZeroed(entrySize);
    for (int i = 0; i < numRecords; i++) {
      int result = (int) memoryIndex.find(keys[i], 0, keySize, false, buf, entrySize);
      assertEquals(-1, result);
    }
    UnsafeAccess.free(buf);
  }
  
  protected void verifyIndexMemoryNot() {
    int entrySize = memoryIndex.getIndexFormat().indexEntrySize();
    long buf = UnsafeAccess.mallocZeroed(entrySize);
    
    for (int i = 0; i < numRecords; i++) {
      int result = (int) memoryIndex.find(mKeys[i], keySize, false, buf, entrySize);
      assertEquals(-1, result);
    }
    UnsafeAccess.free(buf);
  }
  @Before
  public void setUp() {
    queue = new AdmissionQueue();
    memoryIndex = queue.getMemoryIndex();
    queue.setGlobalMaximumSize(50000000);
    queue.setMaximumSize(10000000);
  }
  
  private void loadIndexBytes() {
    for(int i = 0; i < numRecords; i++) {
      boolean result = queue.addIfAbsentRemoveIfPresent(keys[i], 0, keySize);
      assertTrue(result);
    }
  }
  
  private void verifyIndexBytes() {
    verifyIndexBytes(false);
  }
  
  private void verifyIndexBytes(boolean hit) {
    int entrySize = memoryIndex.getIndexFormat().indexEntrySize();
    long buf = UnsafeAccess.mallocZeroed(entrySize);
    
    for (int i = 0; i < numRecords; i++) {
      long hash = Utils.hash64(keys[i], 0, keySize);
      int result = (int) memoryIndex.find(keys[i], 0, keySize, hit, buf, entrySize);
      assertEquals(entrySize, result);
      assertEquals(hash, UnsafeAccess.toLong(buf));
    }
    UnsafeAccess.free(buf);
  }
  
  private void loadIndexBytesNot() {
    for(int i = 0; i < numRecords; i++) {
      boolean result = queue.addIfAbsentRemoveIfPresent(keys[i], 0, keySize);
      assertFalse(result);
    }
  }
    
  private void loadIndexMemory() {
    for(int i = 0; i < numRecords; i++) {
      boolean result = queue.addIfAbsentRemoveIfPresent(mKeys[i], keySize);
      assertTrue(result);
    }
  }
    
  private void verifyIndexMemory() {
    verifyIndexMemory(false);
  }
  
  private void verifyIndexMemory(boolean hit) {
    int entrySize = memoryIndex.getIndexFormat().indexEntrySize();
    long buf = UnsafeAccess.mallocZeroed(entrySize);
    
    for (int i = 0; i < numRecords; i++) {
      long hash = Utils.hash64(mKeys[i], keySize);
      int result = (int) memoryIndex.find(mKeys[i], keySize, hit, buf, entrySize);
      assertEquals(entrySize, result);
      assertEquals(hash, UnsafeAccess.toLong(buf));
    }
    UnsafeAccess.free(buf);
  }
  
  private void loadIndexMemoryNot() {
    for(int i = 0; i < numRecords; i++) {
      boolean result = queue.addIfAbsentRemoveIfPresent(mKeys[i], keySize);
      assertFalse(result);
    }
  }
    
  @Test
  public void testLoadReadNoRehashBytes() {
    LOG.info("Test load and read no rehash bytes");
    loadReadBytes(100000);
  }
  
  @Test
  public void testLoadReadNoRehashMemory() {
    LOG.info("Test load and read no rehash memory");
    loadReadMemory(100000);
  }
  
  @Test
  public void testLoadReadNoRehashBytesWithHit() {
    LOG.info("Test load and read no rehash bytes - with hits");
    loadReadBytesWithHit(100000);
  }
  
  @Test
  public void testLoadReadNoRehashMemoryWithHit() {
    LOG.info("Test load and read no rehash memory - with hits");
    loadReadMemoryWithHit(100000);
  }
  
  @Test
  public void testLoadReadDeleteNoRehashBytes() {
    LOG.info("Test load and read-delete no rehash bytes");
    loadReadBytes(100000);
    deleteIndexBytes();
    verifyIndexBytesNot();
  }
  
  @Test
  public void testLoadReadDeleteNoRehashMemory() {
    LOG.info("Test load and read-delete no rehash memory");
    loadReadMemory(100000);
    deleteIndexMemory();
    verifyIndexMemoryNot();
  }
  
  @Test
  public void testDoubleLoadReadNoRehashBytes() {
    LOG.info("Test double load and read no rehash bytes");
    doubleLoadReadBytes(100000);
  }
  
  @Test
  public void testDoubleLoadReadNoRehashMemory() {
    LOG.info("Test double load and read no rehash memory");
    doubleLoadReadMemory(100000);
  }
  
  @Test
  public void testLoadReadWithRehashBytes() {
    LOG.info("Test load and read with rehash bytes");
    loadReadBytes(1000000);
  }
  
  @Test
  public void testLoadReadWithRehashMemory() {
    LOG.info("Test load and read with rehash memory");
    loadReadMemory(1000000);
  }

  @Test
  public void testLoadReadWithRehashBytesWithHits() {
    LOG.info("Test load and read with rehash bytes - with hits");
    loadReadBytesWithHit(1000000);
  }
  
  @Test
  public void testLoadReadWithRehashMemoryWithHit() {
    LOG.info("Test load and read with rehash memory - with hits");
    loadReadMemoryWithHit(1000000);
  }
  
  @Test
  public void testLoadReadDeleteWithRehashBytes() {
    LOG.info("Test load and read-delete with rehash bytes");
    loadReadBytes(1000000);
    deleteIndexBytes();
    verifyIndexBytesNot();
  }
  
  @Test
  public void testLoadReadDeleteWithRehashMemory() {
    LOG.info("Test load and read with rehash memory");
    loadReadMemory(1000000);
    deleteIndexMemory();
    verifyIndexMemoryNot();
  }
  
  @Test
  public void testDoubleLoadReadWithRehashBytes() {
    LOG.info("Test double load and read with rehash bytes");
    doubleLoadReadBytes(1000000);
  }
  
  @Test
  public void testDoubleLoadReadWithRehashMemory() {
    LOG.info("Test double load and read with rehash memory");
    doubleLoadReadMemory(1000000);
  }
  
  @Test
  public void testEvictionBytes() {
    LOG.info("Test eviction bytes");

    queue.setMaximumSize(100000);
    prepareData(200000);
    loadIndexBytes();
    
    long size = queue.size();
    assertEquals(100000L, size);
    
    int entrySize = memoryIndex.getIndexFormat().indexEntrySize();
    long buf = UnsafeAccess.mallocZeroed(entrySize);
    
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
  
  @Test
  public void testEvictionMemory() {
    LOG.info("Test eviction memory");

    queue.setMaximumSize(100000);
    prepareData(200000);
    loadIndexMemory();
    
    long size = memoryIndex.size();
    assertEquals(100000L, size);
    
    int entrySize = memoryIndex.getIndexFormat().indexEntrySize();
    long buf = UnsafeAccess.mallocZeroed(entrySize);
    
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
  
  @Test
  public void testLoadSave() throws IOException {
    LOG.info("Test load save");
    prepareData(200000);
    loadIndexMemory();
    long size = queue.size();
    assertEquals(200000L, size);
    
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    
    queue.save(dos);
    
    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    DataInputStream dis = new DataInputStream(bais);
    
    queue.dispose();
    
    queue = new AdmissionQueue();
    queue.load(dis);
    memoryIndex = queue.getMemoryIndex();
    verifyIndexMemory();

  }
  
  private void loadReadBytes(int num) {
    prepareData(num);
    System.out.println("prepare done");
    loadIndexBytes();
    System.out.println("load done");
    long size = memoryIndex.size();
    assertEquals((long)num, size);
    verifyIndexBytes();
  }
  
  private void loadReadBytesWithHit(int num) {
    loadReadBytes(num);
    verifyIndexBytes(true);
    verifyIndexBytesNot();
  }
  
  private void doubleLoadReadBytes(int num) {
    prepareData(num);
    loadIndexBytes();
    long size = memoryIndex.size();
    assertEquals((long)num, size);
    verifyIndexBytes();
    loadIndexBytesNot();
    size = memoryIndex.size();
    assertEquals(0L, size);
    verifyIndexBytesNot();
  }
  
  private void loadReadMemory(int num) {
    prepareData(num);
    loadIndexMemory();
    long size = memoryIndex.size();
    assertEquals((long)num, size);
    verifyIndexMemory();
  }
  
  private void loadReadMemoryWithHit(int num) {
    loadReadMemory(num);
    verifyIndexMemory(true);
    verifyIndexMemoryNot();
  }
  
  private void doubleLoadReadMemory(int num) {
    prepareData(num);
    loadIndexMemory();
    long size = memoryIndex.size();
    assertEquals((long)num, size);
    verifyIndexMemory();
    loadIndexMemoryNot();
    size = memoryIndex.size();
    assertEquals(0L, size);
    verifyIndexMemoryNot();
  }
}
