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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.carrotdata.cache.index.MemoryIndex.MutationResult;
import com.carrotdata.cache.util.UnsafeAccess;

public abstract class TestMemoryIndexFormatBase extends TestMemoryIndexBase {
  /** Logger */
  @SuppressWarnings("unused")
  private static final Logger LOG = LoggerFactory.getLogger(TestMemoryIndexFormatBase.class);

  @Before
  public void setUp() {
    memoryIndex = getMemoryIndex();
  }

  /**
   * Subclasses must provide memory index implementation for testing
   * @return
   */
  protected abstract MemoryIndex getMemoryIndex();

  protected int loadIndexBytes() {
    int loaded = 0;
    for (int i = 0; i < numRecords; i++) {
      MutationResult res = memoryIndex.insert(keys[i], 0, keySize, values[i], 0, values[i].length,
        sids[i], offsets[i], expires[i]);
      if (res == MutationResult.INSERTED) {
        loaded++;
      }
    }
    return loaded;
  }

  protected int updateIndexBytes() {
    int updated = 0;
    long seed = System.currentTimeMillis();
    Random r = new Random();
    r.setSeed(seed);
    LOG.info("update seed=" + seed);
    long t1 = System.currentTimeMillis();
    for (int i = 0; i < numRecords; i++) {
      short expSid = sids[i];
      int expOffset = offsets[i];
      sids[i] = (short) r.nextInt(32000);
      offsets[i] = nextOffset(r, 100000000);
      MutationResult res =
          memoryIndex.compareAndUpdate(keys[i], 0, keySize, expSid, expOffset, sids[i], offsets[i]);
      if (res == MutationResult.UPDATED) {
        updated++;
      }

    }
    long t2 = System.currentTimeMillis();
    LOG.info("Time to update {} indexes={}ms", numRecords, t2 - t1);
    return updated;
  }

  protected int loadIndexBytesWithEviction(int evictionStartFrom) {
    int loaded = 0;
    for (int i = 0; i < numRecords; i++) {
      MutationResult res = memoryIndex.insert(keys[i], 0, keySize, values[i], 0, values[i].length,
        sids[i], offsets[i], expires[i]);
      if (res == MutationResult.INSERTED && !memoryIndex.isEvictionEnabled()) {
        loaded++;
      } else if (res == MutationResult.UPDATED && memoryIndex.isEvictionEnabled()) {
        loaded--;
      }
      if (i == evictionStartFrom - 1) {
        memoryIndex.setEvictionEnabled(true);
      }
    }
    return loaded;
  }

  protected void verifyIndexBytes(int expected) {
    verifyIndexBytes(false, expected);
  }

  protected void verifyIndexBytes(boolean hit, int expected) {

    IndexFormat format = memoryIndex.getIndexFormat();
    int entrySize = format.indexEntrySize();
    long buf = UnsafeAccess.mallocZeroed(entrySize);
    int verified = 0;
    for (int i = 0; i < numRecords; i++) {
      int result = (int) memoryIndex.find(keys[i], 0, keySize, hit, buf, entrySize);
      if (result > 0) {
        assertEquals(entrySize, result);
      } else {
        continue;
      }

      short sid = (short) format.getSegmentId(buf);
      int offset = (int) format.getOffset(buf);
      int size = (int) format.getKeyValueSize(buf);
      if (sids[i] != sid) {
        continue;
      }
      if (offsets[i] != offset) {
        continue;
      }
      if (sizes[i] != size) {
        continue;
      }
      long expire = memoryIndex.getExpire(keys[i], 0, keySize);
      if (!sameExpire(expires[i], expire)) {
        continue;
      }
      verified++;
    }
    UnsafeAccess.free(buf);
    assertEquals(expected, verified);
  }

  protected void verifyUpdatedIndexBytes(int expected) {

    IndexFormat format = memoryIndex.getIndexFormat();
    int entrySize = format.indexEntrySize();
    long buf = UnsafeAccess.mallocZeroed(entrySize);
    int verified = 0;
    for (int i = 0; i < numRecords; i++) {
      int result = (int) memoryIndex.find(keys[i], 0, keySize, false, buf, entrySize);
      if (result > 0) {
        assertEquals(entrySize, result);
      } else {
        continue;
      }
      short sid = (short) format.getSegmentId(buf);
      int offset = (int) format.getOffset(buf);
      int size = (int) format.getKeyValueSize(buf);
      if (sids[i] != sid) {
        continue;
      }
      if (offsets[i] != offset) {
        continue;
      }
      if (sizes[i] != size) {
        continue;
      }
      long expire = memoryIndex.getExpire(keys[i], 0, keySize);
      if (!sameExpire(expires[i], expire)) {
        continue;
      }
      verified++;
    }
    UnsafeAccess.free(buf);
    assertEquals(expected, verified);
  }

  protected void verifyGetSetExpireBytes(int expected) {

    IndexFormat format = memoryIndex.getIndexFormat();
    if (!format.isExpirationSupported()) {
      return;
    }
    int verified = 0;
    for (int i = 0; i < numRecords; i++) {

      long expire = memoryIndex.getExpire(keys[i], 0, keySize);
      if (!sameExpire(expires[i], expire)) {
        continue;
      }
      // Set expire + 100000
      long newExpire = expire + 100000;
      long result = memoryIndex.getAndSetExpire(keys[i], 0, keySize, newExpire);
      if (!sameExpire(expires[i], result)) {
        continue;
      }
      expire = memoryIndex.getExpire(keys[i], 0, keySize);
      if (!sameExpire(newExpire, expire)) {
        continue;
      }
      verified++;
    }
    LOG.info("verified=" + verified);
    assertEquals(expected, verified);
  }

  protected void verifyGetSetExpireMemory(int expected) {

    IndexFormat format = memoryIndex.getIndexFormat();
    if (!format.isExpirationSupported()) {
      return;
    }
    int verified = 0;
    for (int i = 0; i < numRecords; i++) {
      long expire = memoryIndex.getExpire(mKeys[i], keySize);
      if (!sameExpire(expires[i], expire)) {
        continue;
      }
      // Set expire + 100000
      long newExpire = expire + 100000;
      long result = memoryIndex.getAndSetExpire(mKeys[i], keySize, newExpire);
      if (!sameExpire(expires[i], result)) {
        continue;
      }
      expire = memoryIndex.getExpire(mKeys[i], keySize);
      if (!sameExpire(newExpire, expire)) {
        continue;
      }
      verified++;
    }
    LOG.info("verified=" + verified);
    assertEquals(expected, verified);
  }

  protected int loadIndexMemory() {
    int loaded = 0;
    for (int i = 0; i < numRecords; i++) {
      MutationResult res = memoryIndex.insert(mKeys[i], keySize, mValues[i], valueSize, sids[i],
        offsets[i], expires[i]);
      if (res == MutationResult.INSERTED) {
        loaded++;
      }
    }
    return loaded;
  }

  protected int updateIndexMemory() {
    int updated = 0;
    long seed = System.currentTimeMillis();
    Random r = new Random();
    r.setSeed(seed);
    LOG.info("update seed=" + seed);

    long t1 = System.currentTimeMillis();
    for (int i = 0; i < numRecords; i++) {
      short expSid = sids[i];
      int expOffset = offsets[i];
      sids[i] = (short) r.nextInt(32000);
      offsets[i] = nextOffset(r, 100000000);
      MutationResult res =
          memoryIndex.compareAndUpdate(mKeys[i], keySize, expSid, expOffset, sids[i], offsets[i]);
      if (res == MutationResult.UPDATED) {
        updated++;
      }
    }
    long t2 = System.currentTimeMillis();
    LOG.info("Time to update {} indexes={}ms", numRecords, t2 - t1);
    return updated;
  }

  protected int loadIndexMemoryWithEviction(int evictionStartFrom) {
    int loaded = 0;
    for (int i = 0; i < numRecords; i++) {
      MutationResult res = memoryIndex.insert(mKeys[i], keySize, mValues[i], valueSize, sids[i],
        offsets[i], expires[i]);
      if (res == MutationResult.INSERTED && !memoryIndex.isEvictionEnabled()) {
        loaded++;
      } else if (res == MutationResult.UPDATED && memoryIndex.isEvictionEnabled()) {
        loaded--;
      }
      if (i == evictionStartFrom - 1) {
        /* DEBUG */ LOG.info("Eviction enabled");
        memoryIndex.setEvictionEnabled(true);
      }
    }
    return loaded;
  }

  protected void verifyIndexMemory(int expected) {
    verifyIndexMemory(false, expected);
  }

  protected void verifyIndexMemory(boolean hit, int expected) {
    IndexFormat format = memoryIndex.getIndexFormat();
    int entrySize = format.indexEntrySize();
    long buf = UnsafeAccess.mallocZeroed(entrySize);
    int verified = 0;
    for (int i = 0; i < numRecords; i++) {
      int result = (int) memoryIndex.find(mKeys[i], keySize, hit, buf, entrySize);
      if (result > 0) {
        assertEquals(entrySize, result);
      } else {
        continue;
      }
      short sid = (short) format.getSegmentId(buf);
      int offset = (int) format.getOffset(buf);
      int size = (int) format.getKeyValueSize(buf);
      if (sids[i] != sid) {
        continue;
      }
      if (offsets[i] != offset) {
        continue;
      }
      if (sizes[i] != size) {
        continue;
      }
      long expire = memoryIndex.getExpire(mKeys[i], keySize);
      if (!sameExpire(expires[i], expire)) {
        continue;
      }
      verified++;
    }
    UnsafeAccess.free(buf);
    assertEquals(expected, verified);
  }

  protected void verifyUpdatedIndexMemory(int expected) {
    IndexFormat format = memoryIndex.getIndexFormat();
    int entrySize = format.indexEntrySize();
    long buf = UnsafeAccess.mallocZeroed(entrySize);
    int verified = 0;
    for (int i = 0; i < numRecords; i++) {
      int result = (int) memoryIndex.find(mKeys[i], keySize, false, buf, entrySize);
      if (result > 0) {
        assertEquals(entrySize, result);
      } else {
        continue;
      }
      short sid = (short) format.getSegmentId(buf);
      int offset = (int) format.getOffset(buf);
      int size = (int) format.getKeyValueSize(buf);
      if (sids[i] != sid) {
        continue;
      }
      if (offsets[i] != offset) {
        continue;
      }
      if (sizes[i] != size) {
        continue;
      }
      long expire = memoryIndex.getExpire(mKeys[i], keySize);
      if (!sameExpire(expires[i], expire)) {
        continue;
      }
      verified++;
    }
    UnsafeAccess.free(buf);
    assertEquals(expected, verified);
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
  public void testLoadUpdateReadNoRehashBytes() {
    LOG.info("Test load update and read no rehash bytes");
    loadUpdateReadBytes(100000);
  }

  @Test
  public void testLoadUpdateReadNoRehashMemory() {
    LOG.info("Test load update and read no rehash memory");
    loadUpdateReadMemory(100000);
  }

  @Test
  public void testLoadReadNoRehashBytesWithHit() {
    LOG.info("Test load and read no rehash bytes - with hit");
    loadReadBytesWithHit(100000);
  }

  @Test
  public void testLoadReadNoRehashMemoryWithHit() {
    LOG.info("Test load and read no rehash memory - with hit");
    loadReadMemoryWithHit(100000);
  }

  @Test
  public void testLoadReadDeleteNoRehashBytes() {
    LOG.info("Test load and read-delete no rehash bytes");
    int loaded = loadReadBytes(100000);
    deleteIndexBytes(loaded);
    verifyIndexBytesNot();
  }

  @Test
  public void testLoadReadDeleteNoRehashMemory() {
    LOG.info("Test load and read-delete no rehash memory");
    int loaded = loadReadMemory(100000);
    deleteIndexMemory(loaded);
    verifyIndexMemoryNot();
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
  public void testLoadUpdateReadWithRehashBytes() {
    LOG.info("Test load update and read with rehash bytes");
    loadUpdateReadBytes(1000000);
  }

  @Test
  public void testLoadUpdateReadWithRehashMemory() {
    LOG.info("Test load update and read with rehash memory");
    loadUpdateReadMemory(1000000);
  }

  @Test
  public void testLoadReadWithRehashBytesWithHit() {
    LOG.info("Test load and read with rehash bytes - with hit");
    loadReadBytesWithHit(1000000);
  }

  @Test
  public void testLoadReadWithRehashMemoryWithHit() {
    LOG.info("Test load and read with rehash memory - with hit");
    loadReadMemoryWithHit(1000000);
  }

  @Test
  public void testLoadReadDeleteWithRehashBytes() {
    LOG.info("Test load and read-delete with rehash bytes");
    int loaded = loadReadBytes(1000000);
    deleteIndexBytes(loaded);
    verifyIndexBytesNot();
  }

  @Test
  public void testLoadReadDeleteWithRehashMemory() {
    LOG.info("Test load and read with rehash memory");
    int loaded = loadReadMemory(1000000);
    deleteIndexMemory(loaded);
    verifyIndexMemoryNot();
  }

  @Test
  public void testGetSetExpireBytes() {
    LOG.info("Test get set expire bytes");
    loadVerifyGetSetExpireBytes(100000);
  }

  @Test
  public void testGetSetExpireMemory() {
    LOG.info("Test get set expire memory");
    loadVerifyGetSetExpireMemory(100000);
  }

  @Ignore
  @Test
  public void testEvictionBytes() {
    // We do not support pure eviction anymore - only expired items
    LOG.info("Test eviction bytes");
    int toLoad = 200000;

    prepareData(toLoad);
    int loaded = loadIndexBytesWithEviction(toLoad / 2);

    long size = memoryIndex.size();
    assertEquals(loaded, size);

    IndexFormat format = memoryIndex.getIndexFormat();
    int entrySize = format.indexEntrySize();
    long buf = UnsafeAccess.mallocZeroed(entrySize);

    int evicted1 = 0;
    int evicted2 = 0;
    for (int i = 0; i < toLoad / 2; i++) {
      int result = (int) memoryIndex.find(keys[i], 0, keySize, false, buf, entrySize);
      if (result == -1) {
        evicted1++;
      } else {
        int sid = format.getSegmentId(buf);
        long off = format.getOffset(buf);
        int kvSize = format.getKeyValueSize(buf);
        long expire = memoryIndex.getExpire(keys[i], 0, keys[i].length);
        if (sids[i] != sid || offsets[i] != off || sizes[i] != kvSize
            || !sameExpire(expires[i], expire)) {
          evicted1++;
        }
      }
    }

    for (int i = toLoad / 2; i < toLoad; i++) {
      int result = (int) memoryIndex.find(keys[i], 0, keySize, false, buf, entrySize);
      if (result == -1) {
        evicted2++;
      } else {
        int sid = format.getSegmentId(buf);
        long off = format.getOffset(buf);
        int kvSize = format.getKeyValueSize(buf);
        long expire = memoryIndex.getExpire(keys[i], 0, keySize);
        if (sids[i] != sid || offsets[i] != off || sizes[i] != kvSize
            || !sameExpire(expires[i], expire)) {
          evicted1++;
        }
      }
    }
    LOG.info("evicted1=" + evicted1 + " evicted2=" + evicted2);
    assertEquals(toLoad - loaded, evicted1 + evicted2);
    UnsafeAccess.free(buf);
  }

  @Ignore
  @Test
  public void testEvictionMemory() {
    // We do not support pure eviction anymore - only expired items
    LOG.info("Test eviction memory");
    int toLoad = 200000;
    prepareData(toLoad);
    long loaded = loadIndexMemoryWithEviction(toLoad / 2);

    long size = memoryIndex.size();
    assertEquals(loaded, size);

    IndexFormat format = memoryIndex.getIndexFormat();
    int entrySize = format.indexEntrySize();
    long buf = UnsafeAccess.mallocZeroed(entrySize);

    int evicted1 = 0;
    int evicted2 = 0;
    for (int i = 0; i < toLoad / 2; i++) {
      int result = (int) memoryIndex.find(mKeys[i], keySize, false, buf, entrySize);
      if (result == -1) {
        evicted1++;
      } else {
        int sid = format.getSegmentId(buf);
        long off = format.getOffset(buf);
        int kvSize = format.getKeyValueSize(buf);
        long expire = memoryIndex.getExpire(mKeys[i], keySize);
        if (sids[i] != sid || offsets[i] != off || sizes[i] != kvSize
            || !sameExpire(expires[i], expire)) {
          evicted1++;
        }
      }
    }

    for (int i = toLoad / 2; i < toLoad; i++) {
      int result = (int) memoryIndex.find(mKeys[i], keySize, false, buf, entrySize);
      if (result == -1) {
        evicted2++;
      } else {
        int sid = format.getSegmentId(buf);
        long off = format.getOffset(buf);
        int kvSize = format.getKeyValueSize(buf);
        long expire = memoryIndex.getExpire(mKeys[i], keySize);
        if (sids[i] != sid || offsets[i] != off || sizes[i] != kvSize
            || !sameExpire(expires[i], expire)) {
          evicted1++;
        }
      }
    }
    LOG.info("evicted1=" + evicted1 + " evicted2=" + evicted2);
    assertEquals((int) (toLoad - loaded), evicted1 + evicted2);
    UnsafeAccess.free(buf);
  }

  @Test
  public void testLoadSave() throws IOException {
    LOG.info("Test load save");
    prepareData(100000);
    long loaded = loadIndexMemory();
    long size = memoryIndex.size();
    assertEquals(loaded, size);
    verifyIndexMemory((int) loaded);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);

    memoryIndex.save(dos);

    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    DataInputStream dis = new DataInputStream(bais);

    memoryIndex.dispose();

    memoryIndex = new MemoryIndex();
    memoryIndex.load(dis);

    verifyIndexMemory((int) loaded);

  }

  protected int loadReadBytes(int num) {
    prepareData(num);
    LOG.info("prepare done");
    int loaded = loadIndexBytes();
    LOG.info("load done: " + loaded);
    long size = memoryIndex.size();
    assertEquals(loaded, (int) size);
    verifyIndexBytes(loaded);
    return loaded;
  }

  protected int loadUpdateReadBytes(int num) {
    prepareData(num);
    LOG.info("prepare done");
    int loaded = loadIndexBytes();
    LOG.info("load done: " + loaded);
    long size = memoryIndex.size();
    assertEquals(loaded, (int) size);
    int updated = updateIndexBytes();
    LOG.info("update done: " + updated);
    // updated == num always but can be larger than loaded due to hash collisions
    // therefore we verify against loaded
    verifyUpdatedIndexBytes(loaded);
    return loaded;
  }

  protected int loadVerifyGetSetExpireBytes(int num) {
    prepareData(num);
    LOG.info("prepare done");
    int loaded = loadIndexBytes();
    LOG.info("load done: " + loaded);
    long size = memoryIndex.size();
    assertEquals(loaded, (int) size);
    verifyGetSetExpireBytes(loaded);
    return loaded;
  }

  protected int loadReadMemory(int num) {
    prepareData(num);
    int loaded = loadIndexMemory();
    long size = memoryIndex.size();
    assertEquals(loaded, (int) size);
    verifyIndexMemory(loaded);
    return loaded;
  }

  protected int loadUpdateReadMemory(int num) {
    prepareData(num);
    int loaded = loadIndexMemory();
    LOG.info("load done: " + loaded);

    long size = memoryIndex.size();
    assertEquals(loaded, (int) size);
    int updated = updateIndexMemory();
    // We will always have updated == num
    LOG.info("update done: " + updated);
    verifyUpdatedIndexMemory(loaded);
    return loaded;
  }

  protected int loadVerifyGetSetExpireMemory(int num) {
    prepareData(num);
    int loaded = loadIndexMemory();
    long size = memoryIndex.size();
    assertEquals(loaded, (int) size);
    verifyGetSetExpireMemory(loaded);
    return loaded;
  }

  protected void loadReadBytesWithHit(int num) {
    int loaded = loadReadBytes(num);
    verifyIndexBytes(true, loaded);
    verifyIndexBytes(loaded);
    long size = memoryIndex.size();
    assertEquals(loaded, (int) size);
  }

  protected void loadReadMemoryWithHit(int num) {
    int loaded = loadReadMemory(num);
    verifyIndexMemory(true, loaded);
    verifyIndexMemory(loaded);
    long size = memoryIndex.size();
    assertEquals(loaded, (int) size);
  }
}
