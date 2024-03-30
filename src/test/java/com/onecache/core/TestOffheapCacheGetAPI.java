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
package com.onecache.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Random;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.onecache.core.controllers.MinAliveRecyclingSelector;
import com.onecache.core.expire.ExpireSupportUnixTime;
import com.onecache.core.index.CompactBaseWithExpireIndexFormat;
import com.onecache.core.io.BaseDataWriter;
import com.onecache.core.io.BaseFileDataReader;
import com.onecache.core.io.BaseMemoryDataReader;
import com.onecache.core.util.TestUtils;
import com.onecache.core.util.UnsafeAccess;
import com.onecache.core.util.Utils;

public class TestOffheapCacheGetAPI {
  
  int blockSize = 4096;
  int numRecords = 10;
  int maxKeySize = 32;
  int maxValueSize = 5000;
  public Random r;
  
  byte[][] keys;
  byte[][] values;
  long[] mKeys;
  long[] mValues;
  long[] expires;
  
  boolean offheap = true;
  Cache cache;
  int segmentSize = 8 * 1024 * 1024;
  long maxCacheSize = 100L * segmentSize;
  int scavengerInterval = 10000; // seconds - disable for test
  long expireTime;
  double scavDumpBelowRatio = 0.5;
  double minActiveRatio = 0.90;
  
  boolean tlsEnabled = false;
  int tlsInitialBufferSize = 32;
  
  @Before
  public void setUp() throws IOException {
    this.offheap = true;
    this.numRecords = 100000;
    this.r = new Random();
    long seed = System.currentTimeMillis();
    r.setSeed(seed);
    System.out.printf("r.seed=%d\n", seed);

  }
  
  @After
  public void tearDown() throws IOException {
    cache.dispose();
    Arrays.stream(mKeys).forEach(x -> UnsafeAccess.free(x));
    Arrays.stream(mValues).forEach(x -> UnsafeAccess.free(x));
    TestUtils.deleteCacheFiles(this.cache);
  }
  
  protected Cache createCache() throws IOException {
    String cacheName = "cache";
    // Data directory
    Path rootDirPath = Files.createTempDirectory(null);
    File  dir = rootDirPath.toFile();
    String rootDir = dir.getAbsolutePath();
    
    Builder builder = new Builder(cacheName);
    
    builder
      .withCacheDataSegmentSize(segmentSize)
      .withCacheMaximumSize(maxCacheSize)
      .withScavengerRunInterval(scavengerInterval)
      .withScavengerDumpEntryBelowMin(scavDumpBelowRatio)
      .withRecyclingSelector(MinAliveRecyclingSelector.class.getName())
      .withDataWriter(BaseDataWriter.class.getName())
      .withMemoryDataReader(BaseMemoryDataReader.class.getName())
      .withFileDataReader(BaseFileDataReader.class.getName())
      .withMainQueueIndexFormat(CompactBaseWithExpireIndexFormat.class.getName())
      .withCacheRootDir(rootDir)
      .withMinimumActiveDatasetRatio(minActiveRatio)
      .withEvictionDisabledMode(true)
      .withTLSSupported(tlsEnabled)
      .withCacheTLSInitialBufferSize(tlsInitialBufferSize)
      .withStartIndexNumberOfSlotsPower(16)
      .withExpireSupport(ExpireSupportUnixTime.class.getName());
    
    if (offheap) {
      return builder.buildMemoryCache();
    } else {
      return builder.buildDiskCache();
    }
  }
  
  protected void prepareData(int numRecords) throws IOException {
    this.numRecords = numRecords;
    keys = new byte[numRecords][];
    values = new byte[numRecords][];
    mKeys = new long[numRecords];
    mValues = new long[numRecords];
    expires = new long[numRecords];
    
    Random r = new Random();
    long seed = System.currentTimeMillis();
    r.setSeed(seed);
    System.out.println("seed="+ seed);
    
    for (int i = 0; i < numRecords; i++) {
      int keySize = nextKeySize();
      int valueSize = nextValueSize();
      keys[i] = TestUtils.randomBytes(keySize, r);
      values[i] = TestUtils.randomBytes(valueSize, r);
      mKeys[i] = TestUtils.randomMemory(keySize, r);
      mValues[i] = TestUtils.randomMemory(valueSize, r);
      expires[i] = getExpire(i); // To make sure that we have distinct expiration values
    }  
    //System.out.println("Press any key ...");
    //System.in.read();
  }
  
  protected int loadBytes() throws IOException {
    long t1 = System.currentTimeMillis();
    int count = 0;
    while(count < this.numRecords) {
      long expire = expires[count];

      byte[] key = keys[count];
      byte[] value = values[count];      
      boolean result = cache.put(key, value, expire);
      if (!result) {
        break;
      }
      count++;
    }  
    long t2 = System.currentTimeMillis();
    System.out.printf("Time to load (bytes) %d keys is %dms\n", this.numRecords, (t2-t1));
    return count;
  }
  
  protected int loadMemory() throws IOException {
    int count = 0;
    long t1 = System.currentTimeMillis();
    while(count < this.numRecords) {
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
    long t2 = System.currentTimeMillis();
    System.out.printf("Time to load (memory) %d keys is %dms\n", this.numRecords, (t2-t1));

    return count;
  }
  
  protected void verifyMemoryCacheBuffer(int num) throws IOException {
    int bufferSize = safeBufferSize();
    ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
    long getTime = 0;
    int failed = 0;
    for (int i = 0; i < num; i++) {
      int keySize = keys[i].length;
      int valueSize = values[i].length;
      long keyPtr = mKeys[i];
      long valuePtr = mValues[i];
      long expSize = valueSize;
      long t1 = System.nanoTime();
      long size = cache.get(keyPtr, keySize, true, buffer);
      getTime += System.nanoTime() - t1;
      if (size < 0) {
        failed++; continue;
      }
      assertEquals(expSize, size);
      assertTrue( Utils.compareTo(buffer, valueSize, valuePtr, valueSize) == 0);
      buffer.clear();
    }
    System.out.printf("Time to get (memory, cache buffer, hit) %d keys is %dms failed=%s\n", num, getTime / 1_000_000, failed);

  }
  
  protected void verifyBytesCache(int num) throws IOException {
    int bufferSize = safeBufferSize();
    byte[] buffer = new byte[bufferSize];
    long getTime = 0;
    int failed = 0;
    for (int i = 0; i < num; i++) {
      byte[] key = keys[i];
      byte[] value = values[i];
      long expSize = value.length;
      long t1 = System.nanoTime();
      long size = cache.get(key, 0, key.length, false, buffer, 0);
      getTime += System.nanoTime() - t1;
      if (size < 0) {
        failed++; continue;
      }
      assertEquals(expSize, size);
      assertTrue( Utils.compareTo(buffer, 0, value.length, value, 0, value.length) == 0);
    }
    System.out.printf("Time to get (bytes, cache, no hit) %d keys is %dms failed=%d\n", num, getTime / 1_000_000, failed);

  }
  
  protected void verifyBytesCacheAllocated(int num) throws IOException {
    long getTime = 0;
    int failed = 0;
    for (int i = 0; i < num; i++) {
      byte[] key = keys[i];
      byte[] value = values[i];
      long t1 = System.nanoTime();
      byte[] read = cache.get(key, 0, key.length, false);
      getTime += System.nanoTime() - t1;
      if (read == null) {
        failed++;
      }
      assertTrue(read != null);
      assertTrue(Utils.compareTo(read, 0, read.length, value, 0, value.length) == 0);
    }
    System.out.printf("Time to get (bytes, no cache, no hit) %d keys is %dms failed=%d\n", num, getTime / 1_000_000, failed);

  }
  
  protected void verifyKeyValueBytesCache(int num) throws IOException {
    int bufferSize = safeBufferSize();
    byte[] buffer = new byte[bufferSize];
    long getTime = 0;
    for (int i = 0; i < num; i++) {
      byte[] key = keys[i];
      byte[] value = values[i];
      int keySize = key.length;
      int valueSize = value.length;
      long expSize = Utils.kvSize(keySize, valueSize);
      long t1 = System.nanoTime();
      long size = cache.getKeyValue(key, 0, key.length, false, buffer, 0);
      getTime += System.nanoTime() - t1;
      assertEquals(expSize, size);
      int kSize = Utils.getKeySize(buffer, 0);
      int kSizeSize = Utils.sizeUVInt(kSize);
      int off = kSizeSize;
      assertEquals(keySize, kSize);
      int vSize = Utils.getValueSize(buffer, 0);
      int vSizeSize = Utils.sizeUVInt(vSize);
      off += vSizeSize;
      assertEquals(valueSize, vSize);
      assertTrue( Utils.compareTo(buffer, off, kSize, key, 0, key.length) == 0);
      off += kSize;
      assertTrue( Utils.compareTo(buffer, off, vSize, value, 0, value.length) == 0);
    }
    System.out.printf("Time to get-kv (bytes, cache, no hit) %d keys is %dms\n", num, getTime / 1_000_000);

  }
  
  protected void verifyKeyValueBytesCacheAllocated(int num) throws IOException {
    long getTime = 0;
    for (int i = 0; i < num; i++) {
      byte[] key = keys[i];
      byte[] value = values[i];
      long expSize = Utils.kvSize(key.length, value.length);
      long t1 = System.nanoTime();
      byte[] buffer = cache.getKeyValue(key, 0, key.length, false);
      getTime += System.nanoTime() - t1;
      assertEquals(expSize, buffer.length);
      int kSize = Utils.getKeySize(buffer, 0);
      int kSizeSize = Utils.sizeUVInt(kSize);
      assertEquals(key.length, kSize);
      int off = kSizeSize;
      int vSize = Utils.getValueSize(buffer, 0);
      int vSizeSize = Utils.sizeUVInt(vSize);
      off += vSizeSize;
      assertEquals(value.length, vSize);
      assertTrue( Utils.compareTo(buffer, off, kSize, key, 0, key.length) == 0);
      off += kSize;
      assertTrue( Utils.compareTo(buffer, off, vSize, value, 0, value.length) == 0);

    }
    System.out.printf("Time to get-kv (bytes, no cache, no hit) %d keys is %dms\n", num, getTime / 1_000_000);

  }
  
  protected void verifyBytesCacheBuffer(int num) throws IOException {
    int bufferSize = safeBufferSize();
    ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
    long getTime = 0;
    for (int i = 0; i < num; i++) {
      byte[] key = keys[i];
      byte[] value = values[i];
      int keySize = keys[i].length;
      int valueSize = values[i].length;
      long expSize = valueSize;
      long t1 = System.nanoTime();
      long size = cache.get(key, 0, keySize, false, buffer);
      getTime += System.nanoTime() - t1;
      assertEquals(expSize, size);
      assertTrue(Utils.compareTo(buffer, valueSize, value, 0, valueSize) == 0);
      buffer.clear();
    }    
    System.out.printf("Time to get (bytes, cache buffer, no hit) %d keys is %dms\n", num, getTime / 1_000_000);

  }
  
  protected void verifyMemoryCache(int num) throws IOException {
    int bufferSize = safeBufferSize();
    byte[] buffer = new byte[bufferSize];
    long getTime = 0;
    for (int i = 0; i < num; i++) {
      byte[] key = keys[i];
      byte[] value = values[i];
      int expSize = value.length;
      int keySize = key.length;
      long keyPtr = mKeys[i];
      long valuePtr = mValues[i];
      long t1 = System.nanoTime();
      long size = cache.get(keyPtr, keySize, false, buffer, 0);
      getTime += System.nanoTime() - t1;
      assertEquals(expSize, size);
      assertTrue( Utils.compareTo(buffer, 0, value.length, valuePtr, expSize) == 0);
    }
    System.out.printf("Time to get (memory, cache, no hit) %d keys is %dms\n", num, getTime / 1_000_000);
  }
  
  protected void verifyKeyValueMemoryCache(int num) throws IOException {
    int bufferSize = safeBufferSize();
    byte[] buffer = new byte[bufferSize];
    long getTime = 0;
    for (int i = 0; i < num; i++) {
     
      long keyPtr = mKeys[i];
      long valuePtr = mValues[i];
      int keySize = keys[i].length;
      int valueSize = values[i].length;
      long expSize = Utils.kvSize(keySize, valueSize);
      long t1 = System.nanoTime();
      long size = cache.getKeyValue(keyPtr, keySize, false, buffer, 0);
      getTime += System.nanoTime() - t1;
      assertEquals(expSize, size);
      int kSize = Utils.getKeySize(buffer, 0);
      int kSizeSize = Utils.sizeUVInt(kSize);
      int off = kSizeSize;
      assertEquals(keySize, kSize);
      int vSize = Utils.getValueSize(buffer, 0);
      int vSizeSize = Utils.sizeUVInt(vSize);
      off += vSizeSize;
      assertEquals(valueSize, vSize);
      assertTrue(Utils.compareTo(buffer, off, kSize, keyPtr, keySize) == 0);
      off += kSize;
      assertTrue(Utils.compareTo(buffer, off, vSize, valuePtr, valueSize) == 0);
    }
    System.out.printf("Time to get-kv (memory, cache, no hit) %d keys is %dms\n", num, getTime / 1_000_000);

  }
  
  
  protected void verifyMemoryCacheAllocated(int num) throws IOException {
    long getTime = 0;
    for (int i = 0; i < num; i++) {
      byte[] key = keys[i];
      byte[] value = values[i];
      int expSize = value.length;
      int keySize = key.length;
      long keyPtr = mKeys[i];
      long valuePtr = mValues[i];
      long t1 = System.nanoTime();
      byte[] buffer = cache.get(keyPtr, keySize, false);
      getTime += System.nanoTime() - t1;
      assertEquals(expSize, buffer.length);
      assertTrue(Utils.compareTo(buffer, 0, value.length, valuePtr, expSize) == 0);
    }
    System.out.printf("Time to get (memory, no cache, no hit) %d keys is %dms\n", num, getTime / 1_000_000);

  }
  
  protected void verifyKeyValueMemoryCacheAllocated(int num) throws IOException {
    long getTime =0;
    for (int i = 0; i < num; i++) {
      byte[] key = keys[i];
      byte[] value = values[i];
      long keyPtr = mKeys[i];
      long valuePtr = mValues[i];
      int keySize = key.length;
      int valueSize = value.length;
      long t1 = System.nanoTime();
      byte[] buffer = cache.getKeyValue(keyPtr, keySize, false);
      getTime += System.nanoTime() - t1;
      assertTrue(buffer != null);
      int kSize = Utils.getKeySize(buffer, 0);
      int kSizeSize = Utils.sizeUVInt(kSize);
      assertEquals(keySize, kSize);
      int off = kSizeSize;
      
      int vSize = Utils.getValueSize(buffer, 0);
      int vSizeSize = Utils.sizeUVInt(vSize);
      off += vSizeSize;
      assertEquals(valueSize, vSize);
      assertTrue( Utils.compareTo(buffer, off, kSize, keyPtr, keySize) == 0);
      off += kSize;
      assertTrue( Utils.compareTo(buffer, off, vSize, valuePtr, valueSize) == 0);

    }
    System.out.printf("Time to get-kv (memory, no cache, no hit) %d keys is %dms\n", num, getTime / 1_000_000);

  }
  
  protected long getExpire(int n) {
    return System.currentTimeMillis() + 1000000L;
  }
  
  protected int nextKeySize() {
    int size = this.maxKeySize / 2 + r.nextInt(this.maxKeySize / 2);
    return size;
  }

  protected int nextValueSize() {
    int size = 1 + r.nextInt(this.maxValueSize - 1);
    return size;
  }
  
  private int safeBufferSize() {
    int bufSize = Utils.kvSize(maxKeySize, maxValueSize);
    return (bufSize / blockSize + 1) * blockSize;
  }
  
  @Test
  public void testGetAPIBytes() throws IOException {
    cache = createCache();
    prepareData(numRecords);
    int loaded = loadBytes();
    assertEquals(this.numRecords, loaded);
    verifyBytesCache(loaded);
    verifyKeyValueBytesCache(loaded);
    verifyBytesCacheAllocated(loaded);
    verifyKeyValueBytesCacheAllocated(loaded);
    verifyBytesCacheBuffer(loaded);
  
  }
  
  @Test
  public void testGetAPIMemory() throws IOException{
    cache = createCache();
    prepareData(numRecords);
    int loaded = loadMemory();
    assertEquals(this.numRecords, loaded);
    verifyMemoryCache(loaded);
    verifyKeyValueMemoryCache(loaded);
    verifyMemoryCacheAllocated(loaded);
    verifyKeyValueMemoryCacheAllocated(loaded);
    verifyMemoryCacheBuffer(loaded);

  }
  
  @Test
  public void testGetAPIBytesTLS() throws IOException {
    this.tlsEnabled = true;
    testGetAPIBytes();  
  } 
  
  @Test
  public void testGetAPIMemoryTLS() throws IOException{
    this.tlsEnabled = true;
    testGetAPIMemory();
  }
}
