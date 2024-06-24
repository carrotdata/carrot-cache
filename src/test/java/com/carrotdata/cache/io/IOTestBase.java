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
package com.carrotdata.cache.io;

import static com.carrotdata.cache.io.BlockReaderWriterSupport.META_SIZE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import org.junit.After;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotdata.cache.Cache;
import com.carrotdata.cache.index.IndexFormat;
import com.carrotdata.cache.index.MemoryIndex;
import com.carrotdata.cache.util.TestUtils;
import com.carrotdata.cache.util.UnsafeAccess;
import com.carrotdata.cache.util.Utils;

public abstract class IOTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(IOTestBase.class);

  protected static String cacheName = "default";

  int blockSize = 4096;
  protected int numRecords = 10;
  int maxKeySize = 32;
  int maxValueSize = 10000;
  public Random r;

  byte[][] keys;
  byte[][] values;
  long[] mKeys;
  long[] mValues;
  long[] expires;

  int segmentSize;
  long cacheSize;

  Segment segment;
  MemoryIndex index;
  IOEngine engine;

  @BeforeClass
  public static void enableMallocDebug() {
    // UnsafeAccess.setMallocDebugEnabled(true);
    // UnsafeAccess.setMallocDebugStackTraceEnabled(true);
    // UnsafeAccess.setStackTraceRecordingFilter(x -> x == 4194304);
    // UnsafeAccess.setStackTraceRecordingLimit(200);
  }

  @After
  public void tearDown() throws IOException {
    if (this.index != null) {
      this.index.dispose();
    }
    if (mKeys != null) {
      Arrays.stream(mKeys).forEach(x -> UnsafeAccess.free(x));
    }
    if (mValues != null) {
      Arrays.stream(mValues).forEach(x -> UnsafeAccess.free(x));
    }
  }

  protected void prepareRandomData(int numRecords) {
    this.numRecords = numRecords;
    keys = new byte[numRecords][];
    values = new byte[numRecords][];
    mKeys = new long[numRecords];
    mValues = new long[numRecords];
    expires = new long[numRecords];

    Random r = new Random();
    long seed = System.currentTimeMillis();
    r.setSeed(seed);
    LOG.info("seed=" + seed);

    for (int i = 0; i < numRecords; i++) {
      int keySize = nextKeySize();
      int valueSize = nextValueSize();
      keys[i] = TestUtils.randomBytes(keySize, r);
      values[i] = TestUtils.randomBytes(valueSize, r);
      mKeys[i] = TestUtils.randomMemory(keySize, r);
      mValues[i] = TestUtils.randomMemory(valueSize, r);
      expires[i] = getExpire(i); // To make sure that we have distinct expiration values
    }
  }

  protected List<String> loadGithubData() throws URISyntaxException, IOException {

    File dir = new File("./src/test/resources/github");
    File[] list = dir.listFiles();
    ArrayList<String> dataList = new ArrayList<String>();
    for (File ff : list) {
      String s = Files.readString(Paths.get(ff.toURI()));
      dataList.add(s);
    }
    return dataList;
  }

  protected void prepareGithubData(int numRecords) throws URISyntaxException, IOException {
    this.numRecords = numRecords;
    keys = new byte[numRecords][];
    values = new byte[numRecords][];
    mKeys = new long[numRecords];
    mValues = new long[numRecords];
    expires = new long[numRecords];
    String key = "testkey";
    List<String> dataList = loadGithubData();
    String[] sdata = new String[dataList.size()];
    dataList.toArray(sdata);
    Random r = new Random();
    for (int i = 0; i < numRecords; i++) {
      keys[i] = (key + i).getBytes();
      int off = i % sdata.length;
      int howMany = 1 + r.nextInt(10);
      values[i] = combineBytes(sdata, off, howMany);// sdata[ i % sdata.length].getBytes();
      mKeys[i] = TestUtils.copyToMemory(keys[i]);
      mValues[i] = TestUtils.copyToMemory(values[i]);
      expires[i] = getExpire(i); // To make sure that we have distinct expiration values
    }
  }

  private byte[] combineBytes(String[] sarr, int off, int howMany) {
    howMany = Math.min(howMany, sarr.length - off);
    int size = 0;
    for (int i = off; i < off + howMany; i++) {
      size += sarr[i].length();
    }
    byte[] result = new byte[size];
    int offset = 0;
    for (int i = off; i < off + howMany; i++) {
      byte[] arr = sarr[i].getBytes();
      System.arraycopy(arr, 0, result, offset, arr.length);
    }
    return result;
  }

  protected long getExpire(int n) {
    return System.currentTimeMillis() + n * 100000L;
  }

  protected int loadBytes() {
    int count = 0;
    int sid = this.segment.getId();
    long start = System.currentTimeMillis();
    IndexFormat format = this.index != null ? this.index.getIndexFormat() : null;
    int indexSize = this.index != null ? format.indexEntrySize() : 0;
    long indexBuf = this.index != null ? UnsafeAccess.malloc(indexSize) : 0L;

    while (count < this.numRecords) {
      long expire = expires[count];
      byte[] key = keys[count];
      byte[] value = values[count];
      int size = Utils.kvSize(key.length, value.length);
      long offset = segment.append(key, 0, key.length, value, 0, value.length, expire);
      if (offset < 0) {
        break;
      }

      if (this.index != null) {
        format.writeIndex(0L, indexBuf, key, 0, key.length, value, 0, value.length, sid,
          (int) offset, size, expire);
        index.insert(key, 0, key.length, indexBuf, indexSize);
      }
      count++;
    }
    if (indexBuf > 0) {
      UnsafeAccess.free(indexBuf);
    }
    LOG.info("time=" + (System.currentTimeMillis() - start));
    return count;
  }

  protected int loadBytesEngine(IOEngine engine) throws IOException {
    int count = 0;
    while (count < this.numRecords) {
      long expire = expires[count];
      byte[] key = keys[count];
      byte[] value = values[count];
      boolean result = engine.put(key, value, expire);
      if (!result) {
        break;
      }
      count++;
    }
    return count;
  }

  protected int deleteBytesEngine(IOEngine engine, int num) throws IOException {
    int count = 0;
    while (count < num) {
      byte[] key = keys[count];
      boolean result = engine.delete(key, 0, key.length);
      assertTrue(result);
      count++;
    }
    return count;
  }

  protected int loadBytesCache(Cache cache) throws IOException {
    int count = 0;
    while (count < this.numRecords) {
      long expire = expires[count];
      byte[] key = keys[count];
      byte[] value = values[count];
      boolean result = cache.put(key, value, expire);
      if (!result) {
        break;
      }
      count++;
    }
    return count;
  }

  protected int deleteBytesCache(Cache cache, int num) throws IOException {
    int count = 0;
    while (count < num) {
      byte[] key = keys[count];
      boolean result = cache.delete(key, 0, key.length);
      assertTrue(result);
      count++;
    }
    return count;
  }

  protected int loadMemoryEngine(IOEngine engine) throws IOException {
    int count = 0;
    while (count < this.numRecords) {
      long expire = expires[count];
      long keyPtr = mKeys[count];
      int keySize = keys[count].length;
      long valuePtr = mValues[count];
      int valueSize = values[count].length;
      boolean result = engine.put(keyPtr, keySize, valuePtr, valueSize, expire);
      if (!result) {
        break;
      }
      count++;
    }
    return count;
  }

  protected int deleteMemoryEngine(IOEngine engine, int num) throws IOException {
    int count = 0;
    while (count < num) {
      int keySize = keys[count].length;
      long keyPtr = mKeys[count];
      boolean result = engine.delete(keyPtr, keySize);
      assertTrue(result);
      count++;
    }
    return count;
  }

  protected int loadMemoryCache(Cache cache) throws IOException {
    int count = 0;
    while (count < this.numRecords) {
      long expire = expires[count];
      long keyPtr = mKeys[count];
      int keySize = keys[count].length;
      long valuePtr = mValues[count];
      int valueSize = values[count].length;
      boolean result = cache.put(keyPtr, keySize, valuePtr, valueSize, expire);
      if (!result) {
        break;
      }
      count++;
    }
    return count;
  }

  protected int deleteMemoryCache(Cache cache, int num) throws IOException {
    int count = 0;
    while (count < num) {
      int keySize = keys[count].length;
      long keyPtr = mKeys[count];
      boolean result = cache.delete(keyPtr, keySize);
      assertTrue(result);
      count++;
    }
    return count;
  }

  protected int loadMemory() {
    int count = 0;
    int sid = this.segment.getId();

    IndexFormat format = this.index != null ? this.index.getIndexFormat() : null;
    int indexSize = this.index != null ? format.indexEntrySize() : 0;
    long indexBuf = this.index != null ? UnsafeAccess.malloc(indexSize) : 0L;

    while (count < this.numRecords) {
      long expire = expires[count];
      byte[] key = keys[count];
      byte[] value = values[count];
      int keySize = key.length;
      int valueSize = value.length;
      int size = Utils.kvSize(key.length, value.length);
      long keyPtr = mKeys[count];
      long valuePtr = mValues[count];
      long offset = segment.append(keyPtr, keySize, valuePtr, valueSize, expire);

      if (offset < 0) {
        break;
      }

      if (this.index != null) {
        format.writeIndex(0L, indexBuf, keyPtr, keySize, valuePtr, valueSize, sid, (int) offset,
          size, expire);
        index.insert(keyPtr, keySize, indexBuf, indexSize);
      }
      count++;
    }
    if (indexBuf > 0) {
      UnsafeAccess.free(indexBuf);
    }
    return count;
  }

  protected void verifyBytes(int num) {
    long ptr = segment.getAddress();

    for (int i = 0; i < num; i++) {
      byte[] key = keys[i];
      byte[] value = values[i];
      int kSize = Utils.readUVInt(ptr);
      int kSizeSize = Utils.sizeUVInt(kSize);
      int vSize = Utils.readUVInt(ptr + kSizeSize);
      int vSizeSize = Utils.sizeUVInt(vSize);
      assertEquals(key.length, kSize);
      assertEquals(value.length, vSize);
      assertTrue(Utils.compareTo(key, 0, key.length, ptr + kSizeSize + vSizeSize, kSize) == 0);
      assertTrue(
        Utils.compareTo(value, 0, value.length, ptr + kSizeSize + vSizeSize + kSize, vSize) == 0);
      ptr += kSize + vSize + kSizeSize + vSizeSize;
    }
  }

  protected void verifyBytesEngine(IOEngine engine, int num) throws IOException {
    int bufferSize = safeBufferSize();// .kvSize(maxKeySize, maxValueSize);
    byte[] buffer = new byte[bufferSize];
    for (int i = 0; i < num; i++) {
      // *DEBUG*/ LOG.info(i);
      byte[] key = keys[i];
      byte[] value = values[i];
      long expSize = Utils.kvSize(key.length, value.length);
      long size = engine.get(key, 0, key.length, true, buffer, 0);
      assertEquals(expSize, size);
      int kSize = Utils.readUVInt(buffer, 0);
      assertEquals(key.length, kSize);
      int kSizeSize = Utils.sizeUVInt(kSize);
      int vSize = Utils.readUVInt(buffer, kSizeSize);
      assertEquals(value.length, vSize);
      int vSizeSize = Utils.sizeUVInt(vSize);
      int off = kSizeSize + vSizeSize;
      assertTrue(Utils.compareTo(buffer, off, kSize, key, 0, key.length) == 0);
      off += kSize;
      assertTrue(Utils.compareTo(buffer, off, vSize, value, 0, value.length) == 0);
    }
  }

  protected void verifyBytesEngineByteBuffer(IOEngine engine, int num) throws IOException {
    int bufferSize = safeBufferSize();
    ByteBuffer buffer = ByteBuffer.allocate(bufferSize);

    for (int i = 0; i < num; i++) {
      byte[] key = keys[i];
      byte[] value = values[i];
      long expSize = Utils.kvSize(key.length, value.length);
      long size = engine.get(key, 0, key.length, true, buffer);
      assertEquals(expSize, size);
      int kSize = Utils.readUVInt(buffer);
      assertEquals(key.length, kSize);
      int kSizeSize = Utils.sizeUVInt(kSize);
      int off = kSizeSize;
      buffer.position(off);
      int vSize = Utils.readUVInt(buffer);
      assertEquals(value.length, vSize);
      int vSizeSize = Utils.sizeUVInt(vSize);
      off += vSizeSize;
      buffer.position(off);

      assertTrue(Utils.compareTo(buffer, kSize, key, 0, key.length) == 0);
      off += kSize;
      buffer.position(off);
      assertTrue(Utils.compareTo(buffer, vSize, value, 0, value.length) == 0);
      buffer.clear();
    }
  }

  protected void verifyBytesEngineWithDeletes(IOEngine engine, int num, int deleted)
      throws IOException {
    int bufferSize = safeBufferSize();// .kvSize(maxKeySize, maxValueSize);
    byte[] buffer = new byte[bufferSize];
    for (int i = 0; i < num; i++) {
      byte[] key = keys[i];
      byte[] value = values[i];
      long expSize = Utils.kvSize(key.length, value.length);
      long size = engine.get(key, 0, key.length, true, buffer, 0);
      if (i < deleted) {
        assertTrue(size < 0);
        continue;
      }
      assertEquals(expSize, size);
      int kSize = Utils.readUVInt(buffer, 0);
      assertEquals(key.length, kSize);
      int kSizeSize = Utils.sizeUVInt(kSize);
      int vSize = Utils.readUVInt(buffer, kSizeSize);
      assertEquals(value.length, vSize);
      int vSizeSize = Utils.sizeUVInt(vSize);
      int off = kSizeSize + vSizeSize;
      assertTrue(Utils.compareTo(buffer, off, kSize, key, 0, key.length) == 0);
      off += kSize;
      assertTrue(Utils.compareTo(buffer, off, vSize, value, 0, value.length) == 0);
    }
  }

  protected void verifyBytesCache(Cache cache, int num) throws IOException {
    int bufferSize = safeBufferSize();// .kvSize(maxKeySize, maxValueSize);
    byte[] buffer = new byte[bufferSize];
    int failed = 0;
    /* DEBUG */ LOG.info("Verify =" + num);
    for (int i = 0; i < num; i++) {
      ///*DEBUG*/ LOG.info("i={}", i);
      byte[] key = keys[i];
      byte[] value = values[i];
      long expSize = Utils.kvSize(key.length, value.length);
      long size = cache.getKeyValue(key, 0, key.length, false, buffer, 0);
      assertEquals(expSize, size);
      int kSize = Utils.readUVInt(buffer, 0);
      assertEquals(key.length, kSize);
      int kSizeSize = Utils.sizeUVInt(kSize);
      int vSize = Utils.readUVInt(buffer, kSizeSize);
      assertEquals(value.length, vSize);
      int vSizeSize = Utils.sizeUVInt(vSize);
      int off = kSizeSize + vSizeSize;
      assertTrue(Utils.compareTo(buffer, off, kSize, key, 0, key.length) == 0);
      off += kSize;
      assertTrue(Utils.compareTo(buffer, off, vSize, value, 0, value.length) == 0);
    }
    LOG.info("verification failed=" + failed);
  }

  protected void verifyBytesCacheByteBuffer(Cache cache, int num) throws IOException {
    int bufferSize = safeBufferSize();
    ByteBuffer buffer = ByteBuffer.allocate(bufferSize);

    for (int i = 0; i < num; i++) {
      byte[] key = keys[i];
      byte[] value = values[i];
      long expSize = Utils.kvSize(key.length, value.length);
      long size = cache.getKeyValue(key, 0, key.length, true, buffer);
      assertEquals(expSize, size);
      int kSize = Utils.readUVInt(buffer);
      assertEquals(key.length, kSize);
      int kSizeSize = Utils.sizeUVInt(kSize);
      int off = kSizeSize;
      buffer.position(off);
      int vSize = Utils.readUVInt(buffer);
      assertEquals(value.length, vSize);
      int vSizeSize = Utils.sizeUVInt(vSize);
      off += vSizeSize;
      buffer.position(off);

      assertTrue(Utils.compareTo(buffer, kSize, key, 0, key.length) == 0);
      off += kSize;
      buffer.position(off);
      assertTrue(Utils.compareTo(buffer, vSize, value, 0, value.length) == 0);
      buffer.clear();
    }
  }

  protected void verifyBytesCacheNot(Cache cache, int num) throws IOException {
    int bufferSize = safeBufferSize();
    byte[] buffer = new byte[bufferSize];
    for (int i = 0; i < num; i++) {
      byte[] key = keys[i];
      long size = cache.getKeyValue(key, 0, key.length, true, buffer, 0);
      assertEquals(-1L, size);
    }
  }

  protected void verifyBytesCacheWithDeletes(Cache cache, int num, int deleted) throws IOException {
    int bufferSize = safeBufferSize();
    byte[] buffer = new byte[bufferSize];
    for (int i = 0; i < num; i++) {
      byte[] key = keys[i];
      byte[] value = values[i];
      long expSize = Utils.kvSize(key.length, value.length);
      long size = cache.getKeyValue(key, 0, key.length, true, buffer, 0);
      if (i < deleted) {
        assertTrue(size < 0);
        continue;
      }
      assertEquals(expSize, size);
      int kSize = Utils.readUVInt(buffer, 0);
      assertEquals(key.length, kSize);
      int kSizeSize = Utils.sizeUVInt(kSize);
      int vSize = Utils.readUVInt(buffer, kSizeSize);
      assertEquals(value.length, vSize);
      int vSizeSize = Utils.sizeUVInt(vSize);
      int off = kSizeSize + vSizeSize;
      assertTrue(Utils.compareTo(buffer, off, kSize, key, 0, key.length) == 0);
      off += kSize;
      assertTrue(Utils.compareTo(buffer, off, vSize, value, 0, value.length) == 0);
    }
  }

  protected void verifyScanner(SegmentScanner scanner, int num) throws IOException {
    int n = 0;
    while (scanner.hasNext()) {
      byte[] key = keys[n];
      byte[] value = values[n];
      long keyPtr = scanner.keyAddress();
      int keySize = scanner.keyLength();
      long valuePtr = scanner.valueAddress();
      int valueSize = scanner.valueLength();
      assertEquals(key.length, keySize);
      assertEquals(value.length, valueSize);
      assertTrue(Utils.compareTo(key, 0, key.length, keyPtr, keySize) == 0);
      assertTrue(Utils.compareTo(value, 0, value.length, valuePtr, valueSize) == 0);
      n++;
      scanner.next();
    }
    assertEquals(num, n);
    scanner.close();

  }

  protected void verifyScannerFile(SegmentScanner scanner, int num) throws IOException {
    int n = 0;

    byte[] keyBuffer = new byte[Utils.kvSize(maxKeySize, maxValueSize)];
    byte[] valueBuffer = new byte[Utils.kvSize(maxKeySize, maxValueSize)];
    while (scanner.hasNext()) {
      byte[] key = keys[n];
      byte[] value = values[n];
      int keySize = scanner.keyLength();
      int valueSize = scanner.valueLength();
      assertEquals(key.length, keySize);
      assertEquals(value.length, valueSize);

      int size = scanner.getKey(keyBuffer, 0);
      assertEquals(size, keySize);

      size = scanner.getValue(valueBuffer, 0);
      assertEquals(size, valueSize);

      assertTrue(Utils.compareTo(key, 0, key.length, keyBuffer, 0, keySize) == 0);
      assertTrue(Utils.compareTo(value, 0, value.length, valueBuffer, 0, valueSize) == 0);
      n++;
      scanner.next();
    }
    assertEquals(num, n);
    scanner.close();
  }

  protected void verifyMemory(int num) {
    long ptr = segment.getAddress();

    for (int i = 0; i < num; i++) {
      byte[] key = keys[i];
      byte[] value = values[i];
      int kSize = Utils.readUVInt(ptr);
      int kSizeSize = Utils.sizeUVInt(kSize);
      int vSize = Utils.readUVInt(ptr + kSizeSize);
      int vSizeSize = Utils.sizeUVInt(vSize);
      long mKey = mKeys[i];
      long mValue = mValues[i];
      assertEquals(key.length, kSize);
      assertEquals(value.length, vSize);
      assertTrue(Utils.compareTo(mKey, kSize, ptr + kSizeSize + vSizeSize, kSize) == 0);
      assertTrue(Utils.compareTo(mValue, vSize, ptr + kSizeSize + vSizeSize + kSize, vSize) == 0);
      ptr += kSize + vSize + kSizeSize + vSizeSize;
    }
  }

  protected void verifyMemoryEngine(IOEngine engine, int num) throws IOException {
    int bufferSize = safeBufferSize();
    ByteBuffer buffer = ByteBuffer.allocate(bufferSize);

    for (int i = 0; i < num; i++) {
      int keySize = keys[i].length;
      int valueSize = values[i].length;
      long keyPtr = mKeys[i];
      long valuePtr = mValues[i];

      long expSize = Utils.kvSize(keySize, valueSize);
      long size = engine.get(keyPtr, keySize, false, buffer);
      assertEquals(expSize, size);
      int kSize = Utils.readUVInt(buffer);
      assertEquals(keySize, kSize);
      int kSizeSize = Utils.sizeUVInt(kSize);
      int off = kSizeSize;
      buffer.position(off);
      int vSize = Utils.readUVInt(buffer);
      assertEquals(valueSize, vSize);
      int vSizeSize = Utils.sizeUVInt(vSize);
      off += vSizeSize;
      buffer.position(off);
      assertTrue(Utils.compareTo(buffer, kSize, keyPtr, keySize) == 0);
      off += kSize;
      buffer.position(off);
      assertTrue(Utils.compareTo(buffer, vSize, valuePtr, valueSize) == 0);
      buffer.clear();
    }
  }

  protected void verifyMemoryEngineWithDeletes(IOEngine engine, int num, int deleted)
      throws IOException {
    int bufferSize = safeBufferSize();
    ByteBuffer buffer = ByteBuffer.allocate(bufferSize);

    for (int i = 0; i < num; i++) {
      int keySize = keys[i].length;
      int valueSize = values[i].length;
      long keyPtr = mKeys[i];
      long valuePtr = mValues[i];

      long expSize = Utils.kvSize(keySize, valueSize);
      long size = engine.get(keyPtr, keySize, false, buffer);
      if (i < deleted) {
        assertTrue(size < 0);
        continue;
      }
      assertEquals(expSize, size);
      int kSize = Utils.readUVInt(buffer);
      assertEquals(keySize, kSize);
      int kSizeSize = Utils.sizeUVInt(kSize);
      int off = kSizeSize;
      buffer.position(off);
      int vSize = Utils.readUVInt(buffer);
      assertEquals(valueSize, vSize);
      int vSizeSize = Utils.sizeUVInt(vSize);
      off += vSizeSize;
      buffer.position(off);
      assertTrue(Utils.compareTo(buffer, kSize, keyPtr, keySize) == 0);
      off += kSize;
      buffer.position(off);
      assertTrue(Utils.compareTo(buffer, vSize, valuePtr, valueSize) == 0);
      buffer.clear();
    }
  }

  protected void verifyMemoryCache(Cache cache, int num) throws IOException {
    int bufferSize = safeBufferSize();// .kvSize(maxKeySize, maxValueSize);
    ByteBuffer buffer = ByteBuffer.allocate(bufferSize);

    for (int i = 0; i < num; i++) {
      int keySize = keys[i].length;
      int valueSize = values[i].length;
      long keyPtr = mKeys[i];
      long valuePtr = mValues[i];

      long expSize = Utils.kvSize(keySize, valueSize);
      long size = cache.getKeyValue(keyPtr, keySize, false, buffer);
      assertEquals(expSize, size);
      int kSize = Utils.readUVInt(buffer);
      assertEquals(keySize, kSize);
      int kSizeSize = Utils.sizeUVInt(kSize);
      int off = kSizeSize;
      buffer.position(off);
      int vSize = Utils.readUVInt(buffer);
      assertEquals(valueSize, vSize);
      int vSizeSize = Utils.sizeUVInt(vSize);
      off += vSizeSize;
      buffer.position(off);
      assertTrue(Utils.compareTo(buffer, kSize, keyPtr, keySize) == 0);
      off += kSize;
      buffer.position(off);
      assertTrue(Utils.compareTo(buffer, vSize, valuePtr, valueSize) == 0);
      buffer.clear();
    }
  }

  protected void verifyMemoryCacheNot(Cache cache, int num) throws IOException {
    int bufferSize = safeBufferSize();// .kvSize(maxKeySize, maxValueSize);
    ByteBuffer buffer = ByteBuffer.allocate(bufferSize);

    for (int i = 0; i < num; i++) {
      int keySize = keys[i].length;
      long keyPtr = mKeys[i];
      long size = cache.getKeyValue(keyPtr, keySize, true, buffer);
      assertEquals(-1L, size);
      buffer.clear();
    }
  }

  protected void verifyMemoryCacheWithDeletes(Cache cache, int num, int deleted)
      throws IOException {
    int bufferSize = safeBufferSize();// .kvSize(maxKeySize, maxValueSize);
    ByteBuffer buffer = ByteBuffer.allocate(bufferSize);

    for (int i = 0; i < num; i++) {
      int keySize = keys[i].length;
      int valueSize = values[i].length;
      long keyPtr = mKeys[i];
      long valuePtr = mValues[i];

      long expSize = Utils.kvSize(keySize, valueSize);
      long size = cache.getKeyValue(keyPtr, keySize, true, buffer);
      if (i < deleted) {
        assertTrue(size < 0);
        continue;
      }
      assertEquals(expSize, size);
      int kSize = Utils.readUVInt(buffer);
      assertEquals(keySize, kSize);
      int kSizeSize = Utils.sizeUVInt(kSize);
      int off = kSizeSize;
      buffer.position(off);
      int vSize = Utils.readUVInt(buffer);
      assertEquals(valueSize, vSize);
      int vSizeSize = Utils.sizeUVInt(vSize);
      off += vSizeSize;
      buffer.position(off);
      assertTrue(Utils.compareTo(buffer, kSize, keyPtr, keySize) == 0);
      off += kSize;
      buffer.position(off);
      assertTrue(Utils.compareTo(buffer, vSize, valuePtr, valueSize) == 0);
      buffer.clear();
    }
  }

  private int safeBufferSize() {
    int bufSize = Utils.kvSize(maxKeySize, maxValueSize);
    return (bufSize / blockSize + 1) * blockSize;
  }

  protected void verifyBytesWithReader(int num, DataReader reader, IOEngine engine)
      throws IOException {
    IndexFormat format = index.getIndexFormat();
    int indexSize = format.indexEntrySize();
    long indexBuf = UnsafeAccess.malloc(indexSize);
    int bufSize = safeBufferSize();
    byte[] buf = new byte[bufSize];
    int sid = segment.getId();
    for (int i = 0; i < num; i++) {
      byte[] key = keys[i];
      byte[] value = values[i];
      long result = index.find(key, 0, key.length, false, indexBuf, indexSize);
      assertEquals(indexSize, (int) result);

      int offset = (int) format.getOffset(indexBuf);
      int size = format.getKeyValueSize(indexBuf);
      int expSize = Utils.kvSize(key.length, value.length);
      assertEquals(expSize, size);

      int read = reader.read(engine, key, 0, key.length, sid, offset, size, buf, 0);
      assertEquals(expSize, read);

      int keySize = Utils.readUVInt(buf, 0);
      int kSizeSize = Utils.sizeUVInt(keySize);
      int valueSize = Utils.readUVInt(buf, kSizeSize);
      int vSizeSize = Utils.sizeUVInt(valueSize);
      assertEquals(key.length, keySize);
      assertEquals(value.length, valueSize);
      assertTrue(Utils.compareTo(key, 0, key.length, buf, kSizeSize + vSizeSize, keySize) == 0);
      assertTrue(Utils.compareTo(value, 0, value.length, buf, kSizeSize + vSizeSize + keySize,
        valueSize) == 0);
    }

    UnsafeAccess.free(indexBuf);
  }

  protected void verifyBytesWithReaderByteBuffer(int num, DataReader reader, IOEngine engine)
      throws IOException {
    IndexFormat format = index.getIndexFormat();
    int indexSize = format.indexEntrySize();
    long indexBuf = UnsafeAccess.malloc(indexSize);
    int bufSize = safeBufferSize();
    ByteBuffer buf = ByteBuffer.allocate(bufSize);
    int sid = segment.getId();
    for (int i = 0; i < num; i++) {
      byte[] key = keys[i];
      byte[] value = values[i];
      long result = index.find(key, 0, key.length, false, indexBuf, indexSize);
      assertEquals(indexSize, (int) result);

      int offset = (int) format.getOffset(indexBuf);

      int size = format.getKeyValueSize(indexBuf);
      int expSize = Utils.kvSize(key.length, value.length);
      assertEquals(expSize, size);
      int read = reader.read(engine, key, 0, key.length, sid, offset, size, buf);
      assertEquals(expSize, read);

      int keySize = Utils.readUVInt(buf);
      int kSizeSize = Utils.sizeUVInt(keySize);
      int off = kSizeSize;
      buf.position(off);
      int valueSize = Utils.readUVInt(buf);
      int vSizeSize = Utils.sizeUVInt(valueSize);
      off += vSizeSize;
      buf.position(off);
      assertEquals(key.length, keySize);
      assertEquals(value.length, valueSize);
      assertTrue(Utils.compareTo(buf, keySize, key, 0, key.length) == 0);
      off += keySize;
      buf.position(off);
      assertTrue(Utils.compareTo(buf, valueSize, value, 0, value.length) == 0);
      buf.clear();
    }
    UnsafeAccess.free(indexBuf);
  }

  protected void verifyMemoryWithReader(int num, DataReader reader, IOEngine engine)
      throws IOException {

    IndexFormat format = index.getIndexFormat();
    int indexSize = format.indexEntrySize();
    long indexBuf = UnsafeAccess.malloc(indexSize);
    int bufSize = safeBufferSize();// .kvSize(maxKeySize, maxValueSize);
    byte[] buf = new byte[bufSize];

    int sid = segment.getId();
    for (int i = 0; i < num; i++) {

      int kSize = keys[i].length;
      int vSize = values[i].length;
      long keyPtr = mKeys[i];
      long valuePtr = mValues[i];
      long result = index.find(keyPtr, kSize, false, indexBuf, indexSize);
      assertEquals(indexSize, (int) result);

      int offset = (int) format.getOffset(indexBuf);
      int size = format.getKeyValueSize(indexBuf);
      int expSize = Utils.kvSize(kSize, vSize);
      assertEquals(expSize, size);

      int read = reader.read(engine, keyPtr, kSize, sid, offset, size, buf, 0);
      assertEquals(expSize, read);

      int keySize = Utils.readUVInt(buf, 0);
      int kSizeSize = Utils.sizeUVInt(keySize);
      int valueSize = Utils.readUVInt(buf, kSizeSize);
      int vSizeSize = Utils.sizeUVInt(valueSize);
      assertEquals(kSize, keySize);
      assertEquals(vSize, valueSize);
      assertTrue(Utils.compareTo(buf, kSizeSize + vSizeSize, keySize, keyPtr, kSize) == 0);
      assertTrue(
        Utils.compareTo(buf, kSizeSize + vSizeSize + keySize, valueSize, valuePtr, vSize) == 0);
    }
    UnsafeAccess.free(indexBuf);
  }

  protected void verifyMemoryWithReaderByteBuffer(int num, DataReader reader, IOEngine engine)
      throws IOException {

    IndexFormat format = index.getIndexFormat();
    int indexSize = format.indexEntrySize();
    long indexBuf = UnsafeAccess.malloc(indexSize);
    int bufSize = safeBufferSize();
    ByteBuffer buf = ByteBuffer.allocate(bufSize);

    int sid = segment.getId();
    for (int i = 0; i < num; i++) {

      int kSize = keys[i].length;
      int vSize = values[i].length;
      long keyPtr = mKeys[i];
      long valuePtr = mValues[i];
      long result = index.find(keyPtr, kSize, false, indexBuf, indexSize);
      assertEquals(indexSize, (int) result);

      int offset = (int) format.getOffset(indexBuf);
      int size = format.getKeyValueSize(indexBuf);
      int expSize = Utils.kvSize(kSize, vSize);
      assertEquals(expSize, size);

      int read = reader.read(engine, keyPtr, kSize, sid, offset, size, buf);
      assertEquals(expSize, read);

      int keySize = Utils.readUVInt(buf);
      int kSizeSize = Utils.sizeUVInt(keySize);
      int off = kSizeSize;
      buf.position(off);
      int valueSize = Utils.readUVInt(buf);
      int vSizeSize = Utils.sizeUVInt(valueSize);
      off += vSizeSize;
      buf.position(off);
      assertEquals(kSize, keySize);
      assertEquals(vSize, valueSize);
      assertTrue(Utils.compareTo(buf, keySize, keyPtr, kSize) == 0);
      off += keySize;
      buf.position(off);
      assertTrue(Utils.compareTo(buf, valueSize, valuePtr, vSize) == 0);
      buf.clear();
    }
    UnsafeAccess.free(indexBuf);
  }

  protected void verifyBytesBlock(int num, int blockSize) {
    long ptr = segment.getAddress();
    for (int i = 0; i < num; i++) {
      int blockDataSize = UnsafeAccess.toInt(ptr);
      long $ptr = ptr + META_SIZE;
      int count = 0;
      while ($ptr < ptr + blockDataSize + META_SIZE) {
        byte[] key = keys[i + count];
        byte[] value = values[i + count];
        int kSize = Utils.readUVInt($ptr);
        int kSizeSize = Utils.sizeUVInt(kSize);
        int vSize = Utils.readUVInt($ptr + kSizeSize);
        int vSizeSize = Utils.sizeUVInt(vSize);
        assertEquals(key.length, kSize);
        assertEquals(value.length, vSize);
        assertTrue(Utils.compareTo(key, 0, key.length, $ptr + kSizeSize + vSizeSize, kSize) == 0);
        assertTrue(Utils.compareTo(value, 0, value.length, $ptr + kSizeSize + vSizeSize + kSize,
          vSize) == 0);
        $ptr += kSize + vSize + kSizeSize + vSizeSize;
        count++;
      }
      i += count - 1;
      ptr += ((blockDataSize + META_SIZE - 1) / blockSize + 1) * blockSize;
    }
  }

  protected void verifyMemoryBlock(int num, int blockSize) {
    long ptr = segment.getAddress();
    for (int i = 0; i < num; i++) {
      int blockDataSize = UnsafeAccess.toInt(ptr);
      long $ptr = ptr + META_SIZE;
      int count = 0;

      while ($ptr < ptr + blockDataSize + META_SIZE) {
        byte[] key = keys[i + count];
        byte[] value = values[i + count];
        long mKey = mKeys[i + count];
        long mValue = mValues[i + count];
        int kSize = Utils.readUVInt($ptr);
        int kSizeSize = Utils.sizeUVInt(kSize);
        int vSize = Utils.readUVInt($ptr + kSizeSize);
        int vSizeSize = Utils.sizeUVInt(vSize);
        assertEquals(key.length, kSize);
        assertEquals(value.length, vSize);
        assertTrue(Utils.compareTo(mKey, key.length, $ptr + kSizeSize + vSizeSize, kSize) == 0);
        assertTrue(
          Utils.compareTo(mValue, value.length, $ptr + kSizeSize + vSizeSize + kSize, vSize) == 0);
        $ptr += kSize + vSize + kSizeSize + vSizeSize;
        count++;
      }
      i += count - 1;
      ptr += ((blockDataSize + META_SIZE - 1) / blockSize + 1) * blockSize;
    }
  }

  protected int nextKeySize() {
    int size = this.maxKeySize / 2 + r.nextInt(this.maxKeySize / 2);
    return size;
  }

  protected int nextValueSize() {
    int size = 1 + r.nextInt(this.maxValueSize - 1);
    return size;
  }

}
