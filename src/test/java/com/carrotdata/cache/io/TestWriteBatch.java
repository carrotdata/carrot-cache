/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.carrotdata.cache.io;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.carrotdata.cache.util.CacheConfig;
import com.carrotdata.cache.util.TestUtils;
import com.carrotdata.cache.util.UnsafeAccess;
import com.carrotdata.cache.util.Utils;

public class TestWriteBatch {

  static class BytesTuple {
    byte[] key;
    byte[] value;

    BytesTuple(byte[] key, byte[] value) {
      this.key = key;
      this.value = value;
    }
  }

  static class MemoryTuple {
    long key;
    int keySize;
    long value;
    int valueSize;

    MemoryTuple(long key, int keySize, long value, int valueSize) {
      this.key = key;
      this.keySize = keySize;
      this.value = value;
      this.valueSize = valueSize;
    }
  }

  WriteBatches wb;
  int batchSize = 8192;
  int keyMax = 50;
  int valueMax = 250;

  @Before
  public void setUp() {
    CompressedBlockBatchDataWriter writer = new CompressedBlockBatchDataWriter();
    CacheConfig config = CacheConfig.getInstance();
    config.setCacheCompressionEnabled("default", true);
    config.setCacheCompressionBlockSize("default", batchSize);
    writer.init("default");
    wb = new WriteBatches(writer);
  }

  @After
  public void tearDown() {
    wb.dispose();
  }

  @Test
  public void testGetWriteBatch() {
    int rank = 10;
    long id = Thread.currentThread().getId();
    int key = -(rank << 16 | (int) id + 2);
    WriteBatch b = wb.getWriteBatch(key);

    assertTrue(id == b.getThreadId());
    assertTrue(rank == b.getRank());
    assertTrue(batchSize == b.batchSize());
    assertEquals(1, wb.size());
  }

  @Test
  public void testWriteReadBytes() {
    int rank = 10;
    long id = Thread.currentThread().getId();
    int key = -(rank << 16 | (int) id + 2);
    WriteBatch b = wb.getWriteBatch(key);
    for (int i = 0; i < 100; i++) {
      List<BytesTuple> loaded = loadBytes(b);
      verifyBytes(b, loaded);
      b.reset();
    }
  }

  @Test
  public void testWriteReadMemory() {
    int rank = 10;
    long id = Thread.currentThread().getId();
    int key = -(rank << 16 | (int) id + 2);
    WriteBatch b = wb.getWriteBatch(key);
    for (int i = 0; i < 100; i++) {
      List<MemoryTuple> loaded = loadMemory(b);
      verifyMemory(b, loaded);
      loaded.stream().forEach(x -> {
        UnsafeAccess.free(x.key);
        UnsafeAccess.free(x.value);
      });
      b.reset();
    }
  }

  @Test
  public void testWriteOverrideReadBytes() {
    int rank = 10;
    long id = Thread.currentThread().getId();
    int key = -(rank << 16 | (int) id + 2);
    WriteBatch b = wb.getWriteBatch(key);
    for (int i = 0; i < 100; i++) {
      List<BytesTuple> loaded = loadOverrideBytes(b);
      verifyBytes(b, loaded);
      b.reset();
    }
  }

  private List<BytesTuple> loadBytes(WriteBatch b) {
    List<BytesTuple> loaded = new ArrayList<BytesTuple>();
    Random r = new Random();
    while (b.acceptsWrite(0)) {
      int keySize = keyMax / 2 + r.nextInt(keyMax / 2);
      int valueSize = valueMax / 2 + r.nextInt(valueMax / 2);
      byte[] key = TestUtils.randomBytes(keySize, r);
      byte[] value = TestUtils.randomBytes(valueSize, r);
      b.addOrUpdate(key, 0, keySize, value, 0, valueSize);
      loaded.add(new BytesTuple(key, value));
    }
    assertTrue(b.position() >= b.batchSize());
    return loaded;
  }

  private List<BytesTuple> loadOverrideBytes(WriteBatch b) {
    List<BytesTuple> loaded = new ArrayList<BytesTuple>();
    Random r = new Random();

    while (b.acceptsWrite(0)) {
      int keySize = keyMax / 2 + r.nextInt(keyMax / 2);
      int valueSize = valueMax / 2 + r.nextInt(valueMax / 2);
      byte[] key = TestUtils.randomBytes(keySize, r);
      byte[] value = TestUtils.randomBytes(valueSize, r);
      b.addOrUpdate(key, 0, keySize, value, 0, valueSize);
      loaded.add(new BytesTuple(key, value));
      if (b.acceptsWrite(0)) {
        // Probability 25 % - override
        if (r.nextInt(4) == 0) {
          int index = r.nextInt(loaded.size());
          BytesTuple bt = loaded.get(index);
          valueSize = valueMax / 2 + r.nextInt(valueMax / 2);

          value = TestUtils.randomBytes(valueSize, r);
          b.addOrUpdate(bt.key, 0, bt.key.length, value, 0, valueSize);
          bt.value = value;
        }
      }
    }
    assertTrue(b.position() >= b.batchSize());
    return loaded;
  }

  private void verifyBytes(WriteBatch b, List<BytesTuple> list) {

    for (BytesTuple bt : list) {
      byte[] key = bt.key;
      byte[] value = bt.value;
      int keySize = key.length;
      int valueSize = value.length;
      int expSize = Utils.kvSize(keySize, valueSize);

      byte[] buffer = new byte[expSize];
      int size = b.get(key, 0, keySize, buffer, 0);
      assertEquals(expSize, size);

      int off = 0;
      int kSize = Utils.readUVInt(buffer, off);
      int kSizeSize = Utils.sizeUVInt(kSize);
      assertTrue(keySize == kSize);
      off += kSizeSize;
      int vSize = Utils.readUVInt(buffer, off);
      int vSizeSize = Utils.sizeUVInt(vSize);
      assertTrue(valueSize == vSize);
      off += vSizeSize;
      assertTrue(Utils.equals(key, 0, keySize, buffer, off, kSize));
      off += kSize;
      assertTrue(Utils.equals(value, 0, valueSize, buffer, off, vSize));

      long bufptr = UnsafeAccess.malloc(expSize);
      size = b.get(key, 0, keySize, bufptr, expSize);
      assertEquals(expSize, size);

      off = 0;
      kSize = Utils.readUVInt(bufptr + off);
      kSizeSize = Utils.sizeUVInt(kSize);
      assertTrue(keySize == kSize);
      off += kSizeSize;
      vSize = Utils.readUVInt(bufptr + off);
      vSizeSize = Utils.sizeUVInt(vSize);
      assertTrue(valueSize == vSize);
      off += vSizeSize;
      assertTrue(Utils.equals(key, 0, keySize, bufptr + off, kSize));
      off += kSize;
      assertTrue(Utils.equals(value, 0, valueSize, bufptr + off, vSize));

      UnsafeAccess.free(bufptr);
    }
  }

  private List<MemoryTuple> loadMemory(WriteBatch b) {
    List<MemoryTuple> loaded = new ArrayList<MemoryTuple>();
    Random r = new Random();
    while (b.acceptsWrite(0)) {
      int keySize = keyMax / 2 + r.nextInt(keyMax / 2);
      int valueSize = valueMax / 2 + r.nextInt(valueMax / 2);
      long key = TestUtils.randomMemory(keySize, r);
      long value = TestUtils.randomMemory(valueSize, r);
      b.addOrUpdate(key, keySize, value, valueSize);
      loaded.add(new MemoryTuple(key, keySize, value, valueSize));
    }
    assertTrue(b.position() >= b.batchSize());
    return loaded;
  }

  private void verifyMemory(WriteBatch b, List<MemoryTuple> loaded) {

    for (MemoryTuple mt : loaded) {
      int keySize = mt.keySize;
      int valueSize = mt.valueSize;
      long key = mt.key;
      long value = mt.value;
      int expSize = Utils.kvSize(keySize, valueSize);

      byte[] buffer = new byte[expSize];
      int size = b.get(key, keySize, buffer, 0);
      assertEquals(expSize, size);

      int off = 0;
      int kSize = Utils.readUVInt(buffer, off);
      int kSizeSize = Utils.sizeUVInt(kSize);
      assertTrue(keySize == kSize);
      off += kSizeSize;
      int vSize = Utils.readUVInt(buffer, off);
      int vSizeSize = Utils.sizeUVInt(vSize);
      assertTrue(valueSize == vSize);
      off += vSizeSize;
      assertTrue(Utils.equals(buffer, off, kSize, key, keySize));
      off += kSize;
      assertTrue(Utils.equals(buffer, off, vSize, value, valueSize));

      long bufptr = UnsafeAccess.malloc(expSize);
      size = b.get(key, keySize, bufptr, expSize);
      assertEquals(expSize, size);

      off = 0;
      kSize = Utils.readUVInt(bufptr + off);
      kSizeSize = Utils.sizeUVInt(kSize);
      assertTrue(keySize == kSize);
      off += kSizeSize;
      vSize = Utils.readUVInt(bufptr + off);
      vSizeSize = Utils.sizeUVInt(vSize);
      assertTrue(valueSize == vSize);
      off += vSizeSize;
      assertTrue(Utils.equals(key, keySize, bufptr + off, kSize));
      off += kSize;
      assertTrue(Utils.equals(value, valueSize, bufptr + off, vSize));

      UnsafeAccess.free(bufptr);
    }
  }
}
