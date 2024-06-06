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
package com.carrotdata.cache;

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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import com.carrotdata.cache.util.TestUtils;
import com.carrotdata.cache.controllers.AdmissionQueue;
import com.carrotdata.cache.index.MemoryIndex;
import com.carrotdata.cache.util.CacheConfig;
import com.carrotdata.cache.util.UnsafeAccess;
import com.carrotdata.cache.util.Utils;

public class TestAdmissionQueue {
  /** Logger */
  private static final Logger LOG = LoggerFactory.getLogger(TestAdmissionQueue.class);
  AdmissionQueue queue;
  MemoryIndex memoryIndex;
  CacheConfig conf;

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
    // UnsafeAccess.setMallocDebugEnabled(true);
    // UnsafeAccess.setMallocDebugStackTraceEnabled(true);
    // UnsafeAccess.setStackTraceRecordingFilter(x -> x == 1024);
    // UnsafeAccess.setStackTraceRecordingLimit(20000);
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
    LOG.info("seed=" + seed);

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
    for (int i = 0; i < numRecords; i++) {
      boolean result = memoryIndex.delete(keys[i], 0, keySize);
      assertTrue(result);
    }
    assertEquals(0L, memoryIndex.size());
  }

  protected void deleteIndexMemory() {
    for (int i = 0; i < numRecords; i++) {
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
  public void setUp() throws IOException {
    conf = TestUtils.mockConfigForTests(4 * 1024 * 1024, 80 * 1024 * 1024);
    queue = new AdmissionQueue(conf);
    memoryIndex = queue.getMemoryIndex();
    queue.setGlobalMaxSizeRatio(0.5);
    queue.setCurrentMaxSizeRatio(0.5);
    queue.setGlobalMinSizeRatio(0.0);

    // maximum storage size = 80MB
    // AdmissionQueue virtual size 0.5 * 80MB = 40MB
    // entry size = 34
    // Maximum number 1233618 elements w/o evictions
  }

  private void loadIndexBytes() {
    for (int i = 0; i < numRecords; i++) {
      boolean result = queue.addIfAbsentRemoveIfPresent(keys[i], 0, keySize, valueSize);
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
    for (int i = 0; i < numRecords; i++) {
      boolean result = queue.addIfAbsentRemoveIfPresent(keys[i], 0, keySize, valueSize);
      assertFalse(result);
    }
  }

  private void loadIndexMemory() {
    for (int i = 0; i < numRecords; i++) {
      boolean result = queue.addIfAbsentRemoveIfPresent(mKeys[i], keySize, valueSize);
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
    for (int i = 0; i < numRecords; i++) {
      boolean result = queue.addIfAbsentRemoveIfPresent(mKeys[i], keySize, valueSize);
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
    double sizeRatio = 0.05;
    int expectedNum =
        (int) (conf.getCacheMaximumSize("default") * sizeRatio / Utils.kvSize(keySize, valueSize));

    queue.setCurrentMaxSizeRatio(sizeRatio); // 0.05 * 80MB = 4MB
    int toLoad = expectedNum + 30000;
    prepareData(toLoad);
    loadIndexBytes();

    long size = queue.size();
    // Within 3% of expected number
    assertTrue(Math.abs(size - expectedNum) < 0.03 * expectedNum);

    int entrySize = memoryIndex.getIndexFormat().indexEntrySize();
    long buf = UnsafeAccess.mallocZeroed(entrySize);

    int evicted1 = 0;
    int evicted2 = 0;
    for (int i = 0; i < expectedNum; i++) {
      int result = (int) memoryIndex.find(keys[i], 0, keySize, false, buf, entrySize);
      if (result == -1) evicted1++;
    }

    for (int i = expectedNum; i < toLoad; i++) {
      int result = (int) memoryIndex.find(keys[i], 0, keySize, false, buf, entrySize);
      if (result == -1) evicted2++;
    }
    LOG.info("evicted1=" + evicted1 + " evicted2=" + evicted2);
    assertEquals(0, evicted2);
    assertEquals(toLoad - expectedNum, evicted1);
    UnsafeAccess.free(buf);
  }

  @Test
  public void testEvictionMemory() {
    LOG.info("Test eviction memory");

    double sizeRatio = 0.05;
    int expectedNum =
        (int) (conf.getCacheMaximumSize("default") * sizeRatio / Utils.kvSize(keySize, valueSize));

    queue.setCurrentMaxSizeRatio(sizeRatio); // 0.05 * 80MB = 4MB
    int toLoad = expectedNum + 30000;
    prepareData(toLoad);
    loadIndexMemory();

    long size = memoryIndex.size();
    // Within 1% of expected number
    assertTrue(Math.abs(size - expectedNum) < expectedNum / 100);

    int entrySize = memoryIndex.getIndexFormat().indexEntrySize();
    long buf = UnsafeAccess.mallocZeroed(entrySize);

    int evicted1 = 0;
    int evicted2 = 0;
    for (int i = 0; i < expectedNum; i++) {
      int result = (int) memoryIndex.find(mKeys[i], keySize, false, buf, entrySize);
      if (result == -1) evicted1++;
    }

    for (int i = expectedNum; i < toLoad; i++) {
      int result = (int) memoryIndex.find(mKeys[i], keySize, false, buf, entrySize);
      if (result == -1) evicted2++;
    }
    LOG.info("evicted1=" + evicted1 + " evicted2=" + evicted2);
    assertEquals(0, evicted2);
    assertEquals(toLoad - expectedNum, evicted1);
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
    LOG.info("prepare done");
    loadIndexBytes();
    LOG.info("load done");
    long size = memoryIndex.size();
    assertEquals((long) num, size);
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
    assertEquals((long) num, size);
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
    assertEquals((long) num, size);
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
    assertEquals((long) num, size);
    verifyIndexMemory();
    loadIndexMemoryNot();
    size = memoryIndex.size();
    assertEquals(0L, size);
    verifyIndexMemoryNot();
  }
}
