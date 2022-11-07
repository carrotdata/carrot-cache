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

import com.carrot.cache.controllers.MinAliveRecyclingSelector;
import com.carrot.cache.util.TestUtils;
import com.carrot.cache.util.UnsafeAccess;
import com.carrot.cache.util.Utils;

public class TestOffheapCacheGetRangeAPI {
  
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
  int segmentSize = 4 * 1024 * 1024;
  long maxCacheSize = 100L * segmentSize;
  int scavengerInterval = 10000; // seconds - disable for test
  long expireTime;
  double scavDumpBelowRatio = 0.5;
  double minActiveRatio = 0.90;
  
  @Before
  public void setUp() throws IOException {
    this.offheap = true;
    cache = createCache();
    this.numRecords = 100000;
    this.r = new Random();
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
      .withScavengerDumpEntryBelowStart(scavDumpBelowRatio)
      .withRecyclingSelector(MinAliveRecyclingSelector.class.getName())
      .withCacheRootDir(rootDir)
      .withEvictionDisabledMode(true)
      .withMinimumActiveDatasetRatio(minActiveRatio);
    if (offheap) {
      return builder.buildMemoryCache();
    } else {
      return builder.buildDiskCache();
    }
  }
  
  protected void prepareData(int numRecords) {
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
    System.out.printf("prepare finished in %dms\n", System.currentTimeMillis() - seed );
    
  }
  
  protected int loadBytes() throws IOException {
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
    System.out.printf("loaded=%d\n", count);
    return count;
  }
  
  protected int loadMemory() throws IOException {
    int count = 0;
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
    System.out.printf("loaded=%d\n", count);

    return count;
  }
  
  protected void verifyMemoryCache(int num) throws IOException {
    int bufferSize = safeBufferSize();
    ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
    
    for (int i = 0; i < num; i++) {
      int keySize = keys[i].length;
      int valueSize = values[i].length;
      long keyPtr = mKeys[i];
      long valuePtr = mValues[i];
      int rangeStart = r.nextInt(valueSize / 2);
      int rangeSize = r.nextInt(valueSize - rangeStart) + 1;
      long size = cache.getRange(keyPtr, keySize, rangeStart, rangeSize, false, buffer);
      assertEquals(rangeSize, size);
      assertTrue(Utils.compareTo(buffer, rangeSize, valuePtr + rangeStart, rangeSize) == 0);
      buffer.clear();
    }    
  }
  
  protected void verifyBytesCache(int num) throws IOException {
    int bufferSize = safeBufferSize();
    byte[] buffer = new byte[bufferSize];
    for (int i = 0; i < num; i++) {
      byte[] key = keys[i];
      byte[] value = values[i];
      int valueSize = value.length;
      int rangeStart = r.nextInt(valueSize / 2);
      int rangeSize = r.nextInt(valueSize - rangeStart);
      long size = cache.getRange(key, 0, key.length, rangeStart, rangeSize, false, buffer, 0);
      assertEquals(rangeSize, size);
      assertTrue(Utils.compareTo(buffer, 0, rangeSize, value, rangeStart, rangeSize) == 0);
    }
  }
  
  protected void verifyBytesCacheBuffer(int num) throws IOException {
    int bufferSize = safeBufferSize();
    ByteBuffer buffer = ByteBuffer.allocate(bufferSize);
    
    for (int i = 0; i < num; i++) {
      byte[] key = keys[i];
      byte[] value = values[i];
      int keySize = keys[i].length;
      int valueSize = values[i].length;
      int rangeStart = r.nextInt(valueSize / 2);
      int rangeSize = r.nextInt(valueSize - rangeStart);
      long size = cache.getRange(key, 0, keySize, rangeStart, rangeSize, false, buffer);
      assertEquals(rangeSize, size);
      assertTrue(Utils.compareTo(buffer, rangeSize, value, rangeStart, rangeSize) == 0);
      buffer.clear();
    }    
  }
  
  protected void verifyMemoryCacheBytes(int num) throws IOException {
    int bufferSize = safeBufferSize();
    byte[] buffer = new byte[bufferSize];
    for (int i = 0; i < num; i++) {
      byte[] key = keys[i];
      byte[] value = values[i];
      int valueSize = value.length;
      int keySize = key.length;
      long keyPtr = mKeys[i];
      long valuePtr = mValues[i];
      int rangeStart = r.nextInt(valueSize / 2);
      int rangeSize = r.nextInt(valueSize - rangeStart);
      long size = cache.getRange(keyPtr, keySize, rangeStart, rangeSize, false, buffer, 0);
      assertEquals(rangeSize, size);
      assertTrue(Utils.compareTo(buffer, 0, rangeSize, valuePtr + rangeStart, rangeSize) == 0);
    }
  }
  
  protected long getExpire(int n) {
    return System.currentTimeMillis() + (n + 1) * 100000L;
  }
  
  protected int nextKeySize() {
    int size = this.maxKeySize / 2 + r.nextInt(this.maxKeySize / 2);
    return size;
  }

  protected int nextValueSize() {
    int size = 100 + r.nextInt(this.maxValueSize - 100);
    return size;
  }
  
  private int safeBufferSize() {
    int bufSize = Utils.kvSize(maxKeySize, maxValueSize);
    return (bufSize / blockSize + 1) * blockSize;
  }
  
  @Test
  public void testGetRangeAPIBytes() throws IOException {
    prepareData(numRecords);
    int loaded = loadBytes();
    assertEquals(this.numRecords, loaded);
    verifyBytesCache(loaded);
    verifyBytesCacheBuffer(loaded);
  }
  
  @Test
  public void testGetRangeAPIMemory() throws IOException{
    prepareData(numRecords);
    int loaded = loadMemory();
    assertEquals(this.numRecords, loaded);
    verifyMemoryCache(loaded);
    verifyMemoryCacheBytes(loaded);
  }
}
