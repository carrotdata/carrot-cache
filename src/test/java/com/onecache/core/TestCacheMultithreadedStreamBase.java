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
import java.util.Random;

import org.junit.After;
import org.junit.Test;

import com.onecache.core.util.TestUtils;
import com.onecache.core.Builder;
import com.onecache.core.Cache;
import com.onecache.core.Scavenger;
import com.onecache.core.controllers.LRCRecyclingSelector;
import com.onecache.core.eviction.LRUEvictionPolicy;
import com.onecache.core.util.Percentile;
import com.onecache.core.util.UnsafeAccess;
import com.onecache.core.util.Utils;

public abstract class TestCacheMultithreadedStreamBase {

  protected Cache cache;
  
  protected boolean offheap = true;
  
  protected boolean evictionDisabled = false;
  
  protected long segmentSize = 4 * 1024 * 1024;
  
  protected long maxCacheSize = 1000L * segmentSize;
  
  int scavengerInterval = 2; // seconds
    
  double scavDumpBelowRatio = 0.5;
  
  double minActiveRatio = 0.90;
  
  protected int maxKeySize = 32;
  
  protected int maxValueSize = 5000;
  
  protected int numRecords = 10;
  
  protected int numThreads = 1;
  
  protected int blockSize = 4096;
    
  private static ThreadLocal<Percentile> perc = new ThreadLocal<Percentile>();
   
  @After  
  public void tearDown() throws IOException {
    // UnsafeAccess.mallocStats.printStats(false);
    this.cache.dispose();
    TestUtils.deleteCacheFiles(cache);
  }
  
  protected  Cache createCache() throws IOException{
    String cacheName = "cache";
    // Data directory
    Path path = Files.createTempDirectory(null);
    File  dir = path.toFile();
    String rootDir = dir.getAbsolutePath();
    
    Builder builder = new Builder(cacheName);
    
    builder
      .withCacheDataSegmentSize(segmentSize)
      .withCacheMaximumSize(maxCacheSize)
      .withScavengerRunInterval(scavengerInterval)
      .withScavengerDumpEntryBelowMin(scavDumpBelowRatio)
      .withCacheEvictionPolicy(LRUEvictionPolicy.class.getName())
      .withRecyclingSelector(LRCRecyclingSelector.class.getName())
      //.withDataWriter(BlockDataWriter.class.getName())
      //.withMemoryDataReader(BlockMemoryDataReader.class.getName())
      //.withFileDataReader(BlockFileDataReader.class.getName())
      //.withMainQueueIndexFormat(CompactWithExpireIndexFormat.class.getName())
      .withCacheRootDir(rootDir)
      .withMinimumActiveDatasetRatio(minActiveRatio)
      .withEvictionDisabledMode(evictionDisabled);
    
    if (offheap) {
      return builder.buildMemoryCache();
    } else {
      return builder.buildDiskCache();
    }
  }
  
  protected int safeBufferSize() {
    int bufSize = Utils.kvSize(maxKeySize, maxValueSize);
    return (bufSize / blockSize + 1) * blockSize;
  }
  
  protected void joinAll(Thread[] workers) {
    for (Thread t : workers) {
      try {
        t.join();
      } catch(Exception e) {
      }
    }
  }
  
  protected Thread[] startAll(Runnable r) {
    Thread[] workers = new Thread[numThreads];
    for (int i = 0; i < workers.length; i++) {
      workers[i] = new Thread(r);
      workers[i].start();
    }
    return workers;
  }
  
  protected long getExpireStream(long startTime, int n) {
    return startTime + 1000000L;
  }
  
  protected long getExpire(int n) {
    return System.currentTimeMillis() + 1000000L;
  }
  
  protected int nextKeySize(Random r) {
    int size = maxKeySize / 2 + r.nextInt(maxKeySize / 2);
    return size;
  }

  protected int nextValueSize(Random r) {
    int size = 1 + r.nextInt(maxValueSize - 1);
    return size;
  }
  
  
  protected int loadBytesStreamWithoutExpire(int total) throws IOException {
    long startTime = System.currentTimeMillis();
    int loaded = 0;
    for(int i = 0; i < total; i++) {
      if (loadBytesStream(startTime, i, total)) {
        loaded++;
      }
      if (loaded > 0 && loaded % 500000 == 0) {
        System.out.printf("loaded=%d\n", loaded);
      }
    }
    return loaded;
  }
  
  protected int loadBytesStreamWithExpire(int total) throws IOException {
    int loaded = 0;
    for(int i = 0; i < total; i++) {
      if (loadBytesStream(i, total)) {
        loaded++;
      }
      if (loaded > 0 && loaded % 500000 == 0) {
        System.out.printf("loaded=%d\n", loaded);
      }
    }
    return loaded;
  }
  
  protected int loadMemoryStreamWithoutExpire(int total) throws IOException {
    long startTime = System.currentTimeMillis();
    int loaded = 0;
    for(int i = 0; i < total; i++) {
      if (loadMemoryStream(startTime, i, total)) {
        loaded++;
      }
      if (loaded > 0 && loaded % 100000 == 0) {
        System.out.printf("loaded=%d\n", loaded);
      }
    }
    return loaded;
  }
  
  protected int loadMemoryStreamWithExpire(int total) throws IOException {
    int loaded = 0;
    for(int i = 0; i < total; i++) {
      if (loadMemoryStream(i, total)) {
        loaded++;
      }
      if (loaded > 0 && loaded % 100000 == 0) {
        System.out.printf("loaded=%d\n", loaded);
      }
    }
    return loaded;
  }
  
  protected final boolean loadBytesStream(int n, int max) throws IOException {
    long id = Thread.currentThread().getId();
    Random r = new Random(n + id * max);
    int keySize = nextKeySize(r);
    int valueSize = nextValueSize(r);
    byte[] key = TestUtils.randomBytes(keySize, r);
    // To improve performance
    byte[] value = new byte[valueSize];
    long expire = getExpire(n);
    boolean result = this.cache.put(key, value, expire);
    return result;
  }
  
  protected final boolean loadBytesStream(long startTime, int n, int max) throws IOException {
    long id = Thread.currentThread().getId();
    Random r = new Random(n + id * max);
    int keySize = nextKeySize(r);
    int valueSize = nextValueSize(r);
    byte[] key = TestUtils.randomBytes(keySize, r);
    // To improve performance
    byte[] value = new byte[valueSize];
    long expire = getExpireStream(startTime, n);
    
    long start = System.nanoTime();
    boolean result = this.cache.put(key, value, expire);
    long end = System.nanoTime();
    Percentile p = perc.get();
    p.add(end - start);
    
    return result;
  }

  protected final boolean deleteBytesStream(int n, int max) throws IOException {
    long id = Thread.currentThread().getId();
    Random r = new Random(n + id * max);
    int keySize = nextKeySize(r);
    @SuppressWarnings("unused")
    int valueSize = nextValueSize(r);
    byte[] key = TestUtils.randomBytes(keySize, r);
    boolean result = this.cache.delete(key, 0, key.length);
    return result;
  }

  protected final boolean verifyBytesStream(int n, int max) throws IOException {
    int kvSize = safeBufferSize();
    byte[] buffer = new byte[kvSize];
    return verifyBytesStream(n, max, buffer);
  }

  protected final boolean verifyBytesStream(int n, int max, byte[] buffer) throws IOException {
    
    long id = Thread.currentThread().getId();
    Random r = new Random(n + id * max);
    
    int keySize = nextKeySize(r);
    int valueSize = nextValueSize(r);
    byte[] key = TestUtils.randomBytes(keySize, r);

    long expSize = Utils.kvSize(keySize, valueSize);
    long size = this.cache.getKeyValue(key, 0, key.length, false, buffer, 0);
    if (size < 0) {
      return false;
    }
    try {
      assertEquals(expSize, size);
      int kSize = Utils.readUVInt(buffer, 0);
      assertEquals(key.length, kSize);
      int kSizeSize = Utils.sizeUVInt(kSize);
      int vSize = Utils.readUVInt(buffer, kSizeSize);
      assertEquals(valueSize, vSize);
      int vSizeSize = Utils.sizeUVInt(vSize);
      int off = kSizeSize + vSizeSize;
      assertTrue(Utils.compareTo(buffer, off, kSize, key, 0, keySize) == 0);
      
    } catch(AssertionError e) {
      return false;
    } 
    return true;
  }

  protected final int countAliveBytesBetween(int start, int end, int max) throws IOException {
    int total = 0;
    byte[] buffer = new byte[safeBufferSize()];
    for (int i = start; i < end; i++) {
      if (verifyBytesStream(i, max, buffer)) {
        total ++;
        if (total > 0 && total % 100000 == 0) {
          System.out.printf("verified=%d\n", total);
        }
      }
    }
    return total;
  }
  
  protected final int countAliveMemoryBetween(int start, int end, int max) throws IOException {
    int total = 0;
    ByteBuffer buf = ByteBuffer.allocate(safeBufferSize());
    for (int i = start; i < end; i++) {
      if (verifyMemoryStream(i, max, buf)) {
        total ++;
        if (total > 0 && total % 100000 == 0) {
          System.out.printf("verified=%d\n", total);
        }
      }
    }
    return total;
  }
  
  protected final boolean loadMemoryStream(long startTime, int n, int max) throws IOException {
    long id = Thread.currentThread().getId();
    Random r = new Random(n + id * max);
    
    int keySize = nextKeySize(r);
    int valueSize = nextValueSize(r);
    long keyPtr = TestUtils.randomMemory(keySize, r);
    long valuePtr = UnsafeAccess.malloc(valueSize);
    long expire = getExpireStream(startTime, n);
    Percentile p = perc.get();
    long start = System.nanoTime();
    boolean result = this.cache.put(keyPtr, keySize, valuePtr, valueSize, expire);
    long stop = System.nanoTime();
    p.add(stop - start);
    UnsafeAccess.free(keyPtr);
    UnsafeAccess.free(valuePtr);
    return result;
  }

  protected final boolean loadMemoryStream(int n, int max) throws IOException {
    long id = Thread.currentThread().getId();
    Random r = new Random(n + id * max);
    
    int keySize = nextKeySize(r);
    int valueSize = nextValueSize(r);
    long keyPtr = TestUtils.randomMemory(keySize, r);
    long valuePtr = UnsafeAccess.malloc(valueSize);
    long expire = getExpire(n);
    boolean result = this.cache.put(keyPtr, keySize, valuePtr, valueSize, expire);
    UnsafeAccess.free(keyPtr);
    UnsafeAccess.free(valuePtr);
    return result;
  }
  
  protected final boolean deleteMemoryStream(int n, int max) throws IOException {
    long id = Thread.currentThread().getId();
    Random r = new Random(n + id * max);
    int keySize = nextKeySize(r);
    @SuppressWarnings("unused")
    int valueSize = nextValueSize(r);
    long keyPtr = TestUtils.randomMemory(keySize, r);
    boolean result = this.cache.delete(keyPtr, keySize);
    UnsafeAccess.free(keyPtr);
    return result;
  }

  protected final boolean verifyMemoryStream(int n, int max, ByteBuffer buffer) throws IOException {

    long id = Thread.currentThread().getId();
    Random r = new Random(n + id * max);
    
    int keySize = nextKeySize(r);
    int valueSize = nextValueSize(r);
    long keyPtr = TestUtils.randomMemory(keySize, r);

    long expSize = Utils.kvSize(keySize, valueSize);
    long size = this.cache.getKeyValue(keyPtr, keySize, buffer);
    if (size < 0) {
      UnsafeAccess.free(keyPtr);
      return false;
    }
    try {
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
    } catch (AssertionError e) {
      return false;
    } finally {
      UnsafeAccess.free(keyPtr);
      buffer.clear();
    }
    return true;
  }
  
  @Test
  public void testContinuosLoadBytesRun() throws IOException {
    long start = System.currentTimeMillis();
    // Create cache after setting is configuration parameters
    this.cache = createCache();
    Runnable r = () -> {
      try {
        testContinuosLoadBytes();
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
    }};
    
    Thread[] all = startAll(r);
    joinAll(all);
    long stop = System.currentTimeMillis();
    Scavenger.printStats();
    System.out.printf("Time=%dms\n", stop - start);
  }
  
  @Test
  public void testContinuosLoadMemoryRun() throws IOException {
    long start = System.currentTimeMillis();
    // Create cache after setting is configuration parameters
    this.cache = createCache();
    Runnable r = () -> {
      try {
        testContinuosLoadMemory();
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
    }};
    
    Thread[] all = startAll(r);
    joinAll(all);
    long stop = System.currentTimeMillis();
    Scavenger.printStats();
    System.out.printf("Time=%dms\n", stop - start);
  }
  
  private void testContinuosLoadBytes() throws IOException {
    /*DEBUG*/ System.out.println(Thread.currentThread().getName() + 
      ": testContinuosLoadBytes");
    
    Percentile p = new Percentile(1000, numRecords);
    perc.set(p);
    
    int loaded = loadBytesStreamWithoutExpire(this.numRecords);
    /*DEBUG*/ System.out.println(Thread.currentThread().getName() + ": loaded=" + loaded);
    long cacheSize = this.cache.size();
    
    int alive = countAliveBytesBetween((int)(this.numRecords - cacheSize / this.numThreads), this.numRecords, this.numRecords);
    /*DEBUG*/ System.out.printf("%s : cache size=%d alive=%d\n", 
      Thread.currentThread().getName(), cacheSize, alive);
    System.out.printf("%s : min=%dns max=%dns p50=%dns p90=%dns p99=%dns p999=%dns\n",
      Thread.currentThread().getName(),
      p.min(), p.max(), p.value(0.5), p.value(0.9), p.value(0.99), p.value(0.999));
  }
  
  private void testContinuosLoadMemory() throws IOException {
    /*DEBUG*/ System.out.println(Thread.currentThread().getName() + 
      ": testContinuosLoadMemory");
    
    Percentile p = new Percentile(1000, numRecords);
    perc.set(p);
    
    int loaded = loadMemoryStreamWithoutExpire(this.numRecords);
    /*DEBUG*/ System.out.println(Thread.currentThread().getName() + ": loaded=" + loaded);
    long cacheSize = this.cache.size();
    
    int alive = countAliveMemoryBetween((int)(this.numRecords - cacheSize / this.numThreads), this.numRecords, this.numRecords);
    /*DEBUG*/ System.out.printf("%s : cache size=%d alive=%d\n", 
      Thread.currentThread().getName(), cacheSize, alive);
    System.out.printf("%s : min=%dns max=%dns p50=%dns p90=%dns p99=%dns p999=%dns\n",
      Thread.currentThread().getName(),
      p.min(), p.max(), p.value(0.5), p.value(0.9), p.value(0.99), p.value(0.999));
  }
  
}
