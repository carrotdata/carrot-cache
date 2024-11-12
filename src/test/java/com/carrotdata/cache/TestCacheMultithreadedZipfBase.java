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

package com.carrotdata.cache;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;

import org.apache.commons.math3.distribution.ZipfDistribution;
import org.junit.After;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotdata.cache.util.TestUtils;
import com.carrotdata.cache.controllers.AdmissionController;
import com.carrotdata.cache.controllers.BaseAdmissionController;
import com.carrotdata.cache.controllers.BasePromotionController;
import com.carrotdata.cache.controllers.LRCRecyclingSelector;
import com.carrotdata.cache.controllers.PromotionController;
import com.carrotdata.cache.controllers.RecyclingSelector;
import com.carrotdata.cache.eviction.EvictionPolicy;
import com.carrotdata.cache.eviction.LRUEvictionPolicy;
import com.carrotdata.cache.util.Percentile;
import com.carrotdata.cache.util.UnsafeAccess;
import com.carrotdata.cache.util.Utils;

public abstract class TestCacheMultithreadedZipfBase {
  private static final Logger LOG = LoggerFactory.getLogger(TestCacheMultithreadedZipfBase.class);

  protected Cache cache;

  protected boolean memory = true;

  protected boolean evictionDisabled = false;

  protected long segmentSize = 4 * 1024 * 1024;

  protected long maxCacheSize = 1000L * segmentSize;

  int scavengerInterval = 2; // seconds

  int scavNumberThreads = 1;

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

  protected Class<? extends PromotionController> pcController = BasePromotionController.class;

  protected double zipfAlpha = 0.9;

  protected long testStartTime = System.currentTimeMillis();

  protected double aqStartRatio = 0.3;

  protected double pqStartRatio = 0.2;

  protected String parentCacheName = "default";

  protected String victimCacheName = "victim";

  protected int slruInsertionPoint = 7;

  protected long spinOnWaitTime;

  protected boolean hybridCacheInverseMode = false;

  @After
  public void tearDown() throws IOException {
    this.cache.printStats();
    Scavenger.printStats();
    this.cache.dispose();
    // Delete temp data
    TestUtils.deleteCacheFiles(cache);

    Scavenger.clear();
  }

  protected Cache createCache() throws IOException {
    LOG.info("Before CC");
    String cacheName = parentCacheName;
    // Data directory
    Path rootDirPath = Files.createTempDirectory(null);
    String rootDir = rootDirPath.toFile().getAbsolutePath();
    Builder builder = new Builder(cacheName);

    builder.withCacheDataSegmentSize(segmentSize)
        .withCacheMaximumSize(maxCacheSize)
        .withScavengerRunInterval(scavengerInterval)
        .withScavengerDumpEntryBelowMin(scavDumpBelowRatio)
        .withCacheEvictionPolicy(epClz.getName())
        .withRecyclingSelector(rsClz.getName())
        .withCacheRootDirs(new String[] {rootDir})
        .withMinimumActiveDatasetRatio(minActiveRatio)
        .withEvictionDisabledMode(evictionDisabled)
        .withAdmissionQueueStartSizeRatio(aqStartRatio)
        .withAdmissionController(acClz.getName())
        .withSLRUInsertionPoint(slruInsertionPoint)
        .withCacheSpinWaitTimeOnHighPressure(spinOnWaitTime)
        .withScavengerNumberOfThreads(scavNumberThreads)
        .withCacheHybridInverseMode(hybridCacheInverseMode)
        .withPromotionController(pcController.getName())
        .withPromotionQueueStartSizeRatio(pqStartRatio);
    builder = withAddedConfigurations(builder);
    Cache c = null;
    if (memory) {
      c = builder.buildMemoryCache();
    } else {
      c = builder.buildDiskCache();
    }
    LOG.info("After CC");
    return c;
  }

  /**
   * Subclasses may override
   * @param b builder instance
   * @return builder instance
   */
  protected Builder withAddedConfigurations(Builder b) {
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
      } catch (Exception e) {
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
    int failed = 0;
    int kvSize = safeBufferSize();
    byte[] buffer = new byte[kvSize];
    ZipfDistribution dist = new ZipfDistribution(this.numRecords, this.zipfAlpha);
    Percentile perc = new Percentile(10000, total);
    for (int i = 0; i < total; i++) {
      int n = dist.sample();
      long start = System.nanoTime();
      if (verifyBytesStream(n, buffer)) {
        hits++;
      } else if (loadBytesStream(n)) {
        loaded++;
      } else {
        failed++;
      }
      long end = System.nanoTime();
      perc.add(end - start);
      if (i > 0 && i % 500000 == 0) {
        LOG.info("{} - i={} hit rate={}", Thread.currentThread().getName(), i,
          (float) cache.getOverallHitRate());
      }
    }
    LOG.info("{} - hit={} loaded={} failed={}", Thread.currentThread().getName(), (float) hits / total,
      loaded, failed);
    LOG.info("Thread={} latency: min={}ns max={}ns p50={}ns p90={}ns p99={}ns p999={}ns p9999={}ns",
      Thread.currentThread().getName(), perc.min(), perc.max(), perc.value(0.5), perc.value(0.9),
      perc.value(0.99), perc.value(0.999), perc.value(0.9999));
  }

  protected void runMemoryStreamZipf(int total) throws IOException {
    int loaded = 0;
    int hits = 0;
    int failed = 0;
    int kvSize = safeBufferSize();
    ByteBuffer buffer = ByteBuffer.allocate(kvSize);

    ZipfDistribution dist = new ZipfDistribution(this.numRecords, this.zipfAlpha);
    Percentile perc = new Percentile(10000, this.numRecords);

    for (int i = 0; i < total; i++) {
      int n = dist.sample();
      long start = System.nanoTime();
      if (verifyMemoryStream(n, buffer)) {
        hits++;
      } else if (loadMemoryStream(n)) {
        loaded++;
      } else {
        failed++;
      }
      long end = System.nanoTime();
      perc.add(end - start);

      if (i > 0 && i % 500000 == 0) {
        LOG.info("{} - i={} hit rate={}", Thread.currentThread().getName(), i,
          (float) cache.getOverallHitRate());
      }
    }
    LOG.info("{} - hit={} loaded={} failed to load={}", Thread.currentThread().getName(), (float) hits / total,
      loaded, failed);
    LOG.info("Thread={} latency: min={}ns max={}ns p50={}ns p90={}ns p99={}ns p999={}ns p9999={}ns",
      Thread.currentThread().getName(), perc.min(), perc.max(), perc.value(0.5), perc.value(0.9),
      perc.value(0.99), perc.value(0.999), perc.value(0.9999));
  }
  
  protected boolean loadBytesStream(int n) throws IOException {

    long id = Thread.currentThread().getId();
    Random r = new Random(testStartTime + n + id * Integer.MAX_VALUE);
    int keySize = nextKeySize(r);
    int valueSize = nextValueSize(r);
    byte[] key = TestUtils.randomBytes(keySize, r);
    // To improve performance
    byte[] value = new byte[valueSize];
    long expire = getExpire(n);
    boolean result = this.cache.put(key, value, expire);
    return result;
  }

  protected boolean verifyBytesStream(int n, byte[] buffer) throws IOException {

    long id = Thread.currentThread().getId();
    Random r = new Random(testStartTime + n + id * Integer.MAX_VALUE);
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

    } catch (AssertionError e) {
      e.printStackTrace();
      return false;
    }
    return true;
  }

  protected boolean loadMemoryStream(int n) throws IOException {

    long id = Thread.currentThread().getId();
    Random r = new Random(testStartTime + n + id * Integer.MAX_VALUE);
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

  protected boolean verifyMemoryStream(int n, ByteBuffer buffer) throws IOException {

    long id = Thread.currentThread().getId();
    Random r = new Random(testStartTime + n + id * Integer.MAX_VALUE);
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

    // Create cache after setting is configuration parameters
    this.cache = createCache();
    this.numIterations = this.numIterations > 0 ? this.numIterations : this.numRecords;
    Runnable r = () -> {
      try {
        runBytesStreamZipf(this.numIterations);
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    };
    long start = System.currentTimeMillis();
    Thread[] all = startAll(r);
    joinAll(all);
    long stop = System.currentTimeMillis();
    Scavenger.printStats();
    LOG.info("Thread={} time={}ms rps={}", Thread.currentThread().getName(), stop - start,
      (this.numThreads * (long) this.numIterations) * 1000 / (stop - start));
  }

  protected void testContinuosLoadMemoryRun() throws IOException {
    // Create cache after setting is configuration parameters
    this.cache = createCache();
    this.numIterations = this.numIterations > 0 ? this.numIterations : this.numRecords;

    Runnable r = () -> {
      try {
        runMemoryStreamZipf(this.numIterations);
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    };
    long start = System.currentTimeMillis();
    Thread[] all = startAll(r);
    joinAll(all);
    long stop = System.currentTimeMillis();
    Scavenger.printStats();
    LOG.info("Thread={} time={}ms rps={}", Thread.currentThread().getName(), stop - start,
      (this.numThreads * (long) this.numIterations) * 1000 / (stop - start));
  }

}
