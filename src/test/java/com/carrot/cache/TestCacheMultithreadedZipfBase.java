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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;

import org.apache.commons.math3.distribution.ZipfDistribution;
import org.junit.After;

import com.carrot.cache.controllers.AdmissionController;
import com.carrot.cache.controllers.BaseAdmissionController;
import com.carrot.cache.controllers.LRCRecyclingSelector;
import com.carrot.cache.controllers.RecyclingSelector;
import com.carrot.cache.eviction.EvictionPolicy;
import com.carrot.cache.eviction.LRUEvictionPolicy;
import com.carrot.cache.util.Percentile;
import com.carrot.cache.util.TestUtils;
import com.carrot.cache.util.UnsafeAccess;
import com.carrot.cache.util.Utils;

public abstract class TestCacheMultithreadedZipfBase {

  protected Cache cache;
  
  protected boolean offheap = true;
  
  protected boolean evictionDisabled = false;
  
  protected long segmentSize = 4 * 1024 * 1024;
  
  protected long maxCacheSize = 1000L * segmentSize;
  
  int scavengerInterval = 2; // seconds
    
  double scavDumpBelowRatio = 1.0;
  
  double minActiveRatio = 0.90;
  
  protected int maxKeySize = 32;
  
  protected int maxValueSize = 5000;
  
  protected int numRecords = 10;
  
  protected int numIterations = 0;
  
  protected int numThreads = 1;
  
  protected int blockSize = 4096;
  
  protected Class<? extends EvictionPolicy> epClz = LRUEvictionPolicy.class;
  
  protected Class<? extends RecyclingSelector> rsClz = LRCRecyclingSelector.class;
  
  protected Class<? extends AdmissionController> acClz = BaseAdmissionController.class;
    
  protected double zipfAlpha = 0.9;
  
  protected long testStartTime = System.currentTimeMillis();
  
  protected Path dataDirPath;
  
  protected Path snapshotDirPath;
  
  protected double aqStartRatio = 0.3;
  
  @After  
  public void tearDown() throws IOException {
    // UnsafeAccess.mallocStats.printStats(false);
    this.cache.printStats();
    this.cache.dispose();
    // Delete temp data
    TestUtils.deleteDir(dataDirPath);
    TestUtils.deleteDir(snapshotDirPath);
    Scavenger.clear();
  }
  
  protected  Cache createCache() throws IOException{
    String cacheName = "cache";
    // Data directory
    dataDirPath = Files.createTempDirectory(null);
    String dataDir = dataDirPath.toFile().getAbsolutePath();
    
    snapshotDirPath = Files.createTempDirectory(null);
    String snapshotDir = snapshotDirPath.toFile().getAbsolutePath();
    
    Cache.Builder builder = new Cache.Builder(cacheName);
    
    builder
      .withCacheDataSegmentSize(segmentSize)
      .withCacheMaximumSize(maxCacheSize)
      .withScavengerRunInterval(scavengerInterval)
      .withScavengerDumpEntryBelowStart(scavDumpBelowRatio)
      .withCacheEvictionPolicy(epClz.getName())
      .withRecyclingSelector(rsClz.getName())
      .withSnapshotDir(snapshotDir)
      .withDataDir(dataDir)
      .withMinimumActiveDatasetRatio(minActiveRatio)
      .withEvictionDisabledMode(evictionDisabled)
      .withAdmissionQueueStartSizeRatio(aqStartRatio)
      .withAdmissionController(acClz.getName());
    builder = withAddedConfigurations(builder);
    if (offheap) {
      return builder.buildMemoryCache();
    } else {
      return builder.buildDiskCache();
    }
  }
  
  /**
   * Subclasses may override
   * @param b builder instance
   * @return builder instance
   */
  protected Cache.Builder withAddedConfigurations(Cache.Builder b) {
    return b;
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
  
  protected void runBytesStreamZipf(int total) throws IOException {
    int loaded = 0;
    int hits = 0;
    int kvSize = safeBufferSize();
    byte[] buffer = new byte[kvSize];
    ZipfDistribution dist = new ZipfDistribution(this.numRecords, this.zipfAlpha);
    Percentile perc = new Percentile(10000, total);
    for(int i = 0; i < total; i++) {
      int n  = dist.sample();
      long start = System.nanoTime();
      if (verifyBytesStream(n, buffer)) {
        hits++;
      } else if(loadBytesStream(n)) {
        loaded++;
      }
      long end = System.nanoTime();
      perc.add(end - start);
      if (i > 0 && i % 500000 == 0) {
        System.out.printf("%s - i=%d hit rate=%f\n", Thread.currentThread().getName(), i, 
          (float) cache.getOverallHitRate());
      }
    }
    System.out.printf("%s - hit=%f loaded=%d\n", Thread.currentThread().getName(), 
      (float) hits / total, loaded);
    System.out.printf("Thread=%s latency: min=%dns max=%dns p50=%dns p90=%dns p99=%dns p999=%dns p9999=%dns\n",
      Thread.currentThread().getName(), perc.min(), perc.max(), perc.value(0.5),
      perc.value(0.9), perc.value(0.99), perc.value(0.999), perc.value(0.9999));
  }
  
  
  protected void runMemoryStreamZipf(int total) throws IOException {
    int loaded = 0;
    int hits = 0;
    int kvSize = safeBufferSize();
    ByteBuffer buffer = ByteBuffer.allocate(kvSize);
    
    ZipfDistribution dist = new ZipfDistribution(this.numRecords, this.zipfAlpha);
    Percentile perc = new Percentile(10000, this.numRecords);

    for(int i = 0; i < total; i++) {
      int n  = dist.sample();
      long start = System.nanoTime();
      if (verifyMemoryStream(n, buffer)) {
        hits++;
      } else if (loadMemoryStream(n)) {
        loaded++;
      }
      long end = System.nanoTime();
      perc.add(end - start);
      
      if (i > 0 && i % 500000 == 0) {
        System.out.printf("%s - i=%d hit rate=%f\n", Thread.currentThread().getName(), i, 
          (float) cache.getOverallHitRate());
      }
    }
    System.out.printf("%s - hit=%f loaded=%d\n", Thread.currentThread().getName(), 
      (float) hits / total, loaded);
    System.out.printf("Thread=%s latency: min=%dns max=%dns p50=%dns p90=%dns p99=%dns p999=%dns p9999=%dns\n",
      Thread.currentThread().getName(), perc.min(), perc.max(), perc.value(0.5),
      perc.value(0.9), perc.value(0.99), perc.value(0.999), perc.value(0.9999));
  }
  
  
  protected final boolean loadBytesStream(int n) throws IOException {
    Random r = new Random(testStartTime + n);
    int keySize = nextKeySize(r);
    int valueSize = nextValueSize(r);
    byte[] key = TestUtils.randomBytes(keySize, r);
    // To improve performance
    byte[] value = new byte[valueSize];
    long expire = getExpire(n);
    boolean result = this.cache.put(key, value, expire);
    return result;
  }
  
  protected final boolean verifyBytesStream(int n, byte[] buffer) throws IOException {
    
    Random r = new Random(testStartTime + n);
    
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


  protected final boolean loadMemoryStream(int n) throws IOException {
    Random r = new Random(testStartTime + n);
    
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
  
  protected final boolean verifyMemoryStream(int n, ByteBuffer buffer) throws IOException {

    Random r = new Random(testStartTime + n);
    
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
  
  protected void testContinuosLoadBytesRun() throws IOException {
    long start = System.currentTimeMillis();
    // Create cache after setting is configuration parameters
    this.cache = createCache();
    this.numIterations = this.numIterations > 0? this.numIterations: this.numRecords;
    Runnable r = () -> {
      try {
        runBytesStreamZipf(this.numIterations);
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
    }};
    
    Thread[] all = startAll(r);
    joinAll(all);
    long stop = System.currentTimeMillis();
    Scavenger.printStats();
    System.out.printf("Thread=%s time=%dms\n", Thread.currentThread().getName(), stop - start);
  }
  
  protected void testContinuosLoadMemoryRun() throws IOException {
    long start = System.currentTimeMillis();
    // Create cache after setting is configuration parameters
    this.cache = createCache();
    this.numIterations = this.numIterations > 0? this.numIterations: this.numRecords;

    Runnable r = () -> {
      try {
        runMemoryStreamZipf(this.numIterations);
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
  
}
