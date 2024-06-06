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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.Ignore;
import org.junit.Test;

import com.carrotdata.cache.index.MemoryIndex.MutationResult;
import com.carrotdata.cache.util.UnsafeAccess;

public abstract class TestMemoryIndexMQMultithreadedStress
    extends TestMemoryIndexMultithreadedBase {
  /** Logger */
  private static final Logger LOG =
      LoggerFactory.getLogger(TestMemoryIndexMQMultithreadedStress.class);

  private void setUp() {
    // UnsafeAccess.debug = false;
    UnsafeAccess.mallocStats.clear();
    memoryIndex = new MemoryIndex("default", MemoryIndex.Type.MQ);
    numThreads = 8;
  }

  private int loadIndexBytes() {
    IndexFormat format = memoryIndex.getIndexFormat();
    int entrySize = format.indexEntrySize();
    long buf = UnsafeAccess.mallocZeroed(entrySize);
    byte[][] keys = keysTL.get();
    byte[][] values = valuesTL.get();
    short[] sids = sidsTL.get();
    int[] offsets = offsetsTL.get();
    int[] lengths = lengthsTL.get();
    int total = 0;
    long start = System.currentTimeMillis();
    for (int i = 0; i < numRecords; i++) {
      format.writeIndex(0L, buf, keys[i], 0, keys[i].length, values[i], 0, values[i].length,
        sids[i], offsets[i], lengths[i], 0);
      MutationResult result = memoryIndex.insert(keys[i], 0, keySize, buf, entrySize);
      if (result == MutationResult.INSERTED) {
        total++;
      } else {
        /* DEBUG */ LOG.error("Failed insert, loaded=" + i);
        memoryIndex.dump();
        System.exit(-1);
      }
    }
    long end = System.currentTimeMillis();
    LOG.info(Thread.currentThread().getName() + " loaded " + total + " RPS="
        + ((long) numRecords) * 1000 / (end - start));
    UnsafeAccess.free(buf);
    return total;
  }

  private void verifyIndexBytes(int loaded) {

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
      int result = (int) memoryIndex.find(keys[i], 0, keySize, false, buf, entrySize);
      if (result < 0) {
        failed++;
        continue;
      }
      assertEquals(entrySize, result);
      short sid = (short) format.getSegmentId(buf);
      int offset = (int) format.getOffset(buf);
      int size = (int) format.getKeyValueSize(buf);
      if (sids[i] != sid || offsets[i] != offset || lengths[i] != size) {
        failed++;
      }
    }
    UnsafeAccess.free(buf);
    long end = System.currentTimeMillis();
    if (failed == 0) {
      LOG.info(Thread.currentThread().getName() + " verified " + loaded + " RPS="
          + ((long) numRecords) * 1000 / (end - start) + " failed=" + failed);
    } else {
      LOG.error(Thread.currentThread().getName() + " verified " + loaded + " RPS="
          + ((long) numRecords) * 1000 / (end - start) + " failed=" + failed);
    }

  }

  private int loadIndexMemory() {
    IndexFormat format = memoryIndex.getIndexFormat();
    int entrySize = format.indexEntrySize();
    long buf = UnsafeAccess.mallocZeroed(entrySize);
    long[] mKeys = mKeysTL.get();
    long[] mValues = mValuesTL.get();
    short[] sids = sidsTL.get();
    int[] offsets = offsetsTL.get();
    int[] lengths = lengthsTL.get();
    int total = 0;
    long start = System.currentTimeMillis();
    for (int i = 0; i < numRecords; i++) {
      format.writeIndex(0L, buf, mKeys[i], keySize, mValues[i], valueSize, sids[i], offsets[i],
        lengths[i], 0);
      MutationResult result = memoryIndex.insert(mKeys[i], keySize, buf, entrySize);
      if (result == MutationResult.INSERTED) {
        total++;
      } else {
        /* DEBUG */ LOG.error("Failed insert, loaded=" + i);
        memoryIndex.dump();
        System.exit(-1);
      }
    }
    long end = System.currentTimeMillis();
    LOG.info(Thread.currentThread().getName() + " loaded " + total + " RPS="
        + ((long) numRecords) * 1000 / (end - start));
    UnsafeAccess.free(buf);
    return total;
  }

  private void verifyIndexMemory(int loaded) {
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
      int result = (int) memoryIndex.find(mKeys[i], keySize, false, buf, entrySize);
      if (result < 0) {
        failed++;
        continue;
      }
      assertEquals(entrySize, result);
      short sid = (short) format.getSegmentId(buf);
      int offset = (int) format.getOffset(buf);
      int size = (int) format.getKeyValueSize(buf);
      if (sids[i] != sid || offsets[i] != offset || lengths[i] != size) {
        failed++;
      }
    }
    long end = System.currentTimeMillis();
    if (failed == 0) {
      LOG.info(Thread.currentThread().getName() + " verified " + loaded + " RPS="
          + ((long) numRecords) * 1000 / (end - start) + " failed=" + failed);
    } else {
      LOG.error(Thread.currentThread().getName() + " verified " + loaded + " RPS="
          + ((long) numRecords) * 1000 / (end - start) + " failed=" + failed);
    }
    UnsafeAccess.free(buf);

  }

  @Test
  public void stress() {

    int current_slot_size = 0;
    int max = 100;
    for (int i = 0; i < max; i++) {
      current_slot_size = getCurrentSlotSize(i, max);
      LOG.info("STRESS RUN={} of {} slot size={}", i, max, current_slot_size);
      MemoryIndex.MAX_INDEX_ENTRIES_PER_BLOCK = current_slot_size;
      setUp();
      testLoadReadWithRehashBytesMT();
      tearDown();
      setUp();
      testLoadReadWithRehashMemoryMT();
      tearDown();
      setUp();
      testLoadReadDeleteWithRehashBytesMT();
      tearDown();
      setUp();
      testLoadReadDeleteWithRehashMemoryMT();
      tearDown();
      setUp();
      testLoadReadDeleteWithRehashMemoryMT();
      tearDown();

    }
  }

  private int getCurrentSlotSize(int i, int max) {
    int start_slot_size = 100;
    int max_slot_size = 350;
    return start_slot_size + i * (max_slot_size - start_slot_size) / max;
  }

  @Ignore
  @Test
  public void testLoadReadWithRehashBytesMT() {
    /* DEBUG */ LOG.info("testLoadReadWithRehashBytesMT");
    Runnable r = () -> testLoadReadWithRehashBytes();
    Thread[] workers = startAll(r);
    joinAll(workers);
  }

  private void testLoadReadWithRehashBytes() {
    LOG.info("Test load and read with rehash bytes");
    loadReadBytes(10000000);
    clearData();
  }

  @Ignore
  @Test
  public void testLoadReadWithRehashMemoryMT() {
    /* DEBUG */ LOG.info("testLoadReadWithRehashMemoryMT");

    Runnable r = () -> testLoadReadWithRehashMemory();
    Thread[] workers = startAll(r);
    joinAll(workers);
  }

  private void testLoadReadWithRehashMemory() {
    LOG.info("Test load and read with rehash memory");
    loadReadMemory(10000000);
    clearData();
  }

  @Ignore
  @Test
  public void testLoadReadDeleteWithRehashBytesMT() {
    /* DEBUG */ LOG.info("testLoadReadDeleteWithRehashBytesMT");

    Runnable r = () -> testLoadReadDeleteWithRehashBytes();
    Thread[] workers = startAll(r);
    joinAll(workers);
  }

  private void testLoadReadDeleteWithRehashBytes() {
    LOG.info("Test load and read-delete with rehash bytes");
    loadReadBytes(10000000);
    deleteIndexBytes();
    verifyIndexBytesNot();
    clearData();
  }

  @Ignore
  @Test
  public void testLoadReadDeleteWithRehashMemoryMT() {
    /* DEBUG */ LOG.info("testLoadReadDeleteWithRehashMemoryMT");

    Runnable r = () -> testLoadReadDeleteWithRehashMemory();
    Thread[] workers = startAll(r);
    joinAll(workers);
  }

  private void testLoadReadDeleteWithRehashMemory() {
    LOG.info("Test load and read with rehash memory");
    loadReadMemory(10000000);
    deleteIndexMemory();
    verifyIndexMemoryNot();
    clearData();
  }

  private void loadReadBytes(int num) {
    prepareData(num);
    int loaded = loadIndexBytes();
    verifyIndexBytes(loaded);
  }

  private void loadReadMemory(int num) {
    prepareData(num);
    int loaded = loadIndexMemory();
    verifyIndexMemory(loaded);
  }

}
