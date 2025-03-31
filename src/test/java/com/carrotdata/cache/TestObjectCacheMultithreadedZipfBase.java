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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;

import org.apache.commons.math3.distribution.ZipfDistribution;
import org.junit.After;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotdata.cache.controllers.AdmissionController;
import com.carrotdata.cache.controllers.BaseAdmissionController;
import com.carrotdata.cache.controllers.BasePromotionController;
import com.carrotdata.cache.controllers.LRCRecyclingSelector;
import com.carrotdata.cache.controllers.PromotionController;
import com.carrotdata.cache.controllers.RecyclingSelector;
import com.carrotdata.cache.eviction.EvictionPolicy;
import com.carrotdata.cache.eviction.LRUEvictionPolicy;
import com.carrotdata.cache.util.Percentile;
import com.carrotdata.cache.util.TestUtils;
import com.carrotdata.cache.util.Utils;

public abstract class TestObjectCacheMultithreadedZipfBase {
  private static final Logger LOG = LoggerFactory.getLogger(TestObjectCacheMultithreadedZipfBase.class);

  protected ObjectCache cache;

  protected boolean memory = true;

  protected boolean evictionDisabled = false;

  protected long segmentSize = 4 * 1024 * 1024;

  protected long maxCacheSize = 1000L * segmentSize;

  int scavengerInterval = 2; // seconds

  int scavNumberThreads = 1;

  double scavDumpBelowRatio = 1.0;

  double minActiveRatio = 0.90;

  protected int maxKeySize = 32;

  protected int maxValueSize = 10000;

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
  
  protected long maxOnHeapSize = 250_000;

  @After
  public void tearDown() throws IOException {
    this.cache.getNativeCache().printStats();
    Scavenger.printStats();
    this.cache.getNativeCache().dispose();
    // Delete temp data
    TestUtils.deleteCacheFiles(cache.getNativeCache());

    Scavenger.clear();
  }

  protected ObjectCache createCache() throws IOException {
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
        .withPromotionQueueStartSizeRatio(pqStartRatio)
        .withOnHeapMaxCacheSize(maxOnHeapSize);
    builder = withAddedConfigurations(builder);
    ObjectCache c = null;
    if (memory) {
      c = builder.buildObjectMemoryCache();
    } else {
      c = builder.buildObjectDiskCache();
    }
    c.registerClasses(byte[].class, String.class);
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
    //int kvSize = safeBufferSize();
    //byte[] buffer = new byte[kvSize];
    ZipfDistribution dist = new ZipfDistribution(this.numRecords, this.zipfAlpha);
    Percentile perc = new Percentile(10000, total);
    for (int i = 0; i < total; i++) {
      int n = dist.sample();
      long start = System.nanoTime();
      if (verifyBytesStream(n)) {
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
          (float) hits / i);
      }
    }
    LOG.info("{} - hit={} loaded={} failed={}", Thread.currentThread().getName(), (float) hits / total,
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
    String skey = Utils.toHex(key);
    // To improve performance
    byte[] value = new byte[valueSize];
    long expire = getExpire(n);
    boolean result = this.cache.put(skey, value, expire);
    return result;
  }

  protected boolean verifyBytesStream(int n) throws IOException {

    long id = Thread.currentThread().getId();
    Random r = new Random(testStartTime + n + id * Integer.MAX_VALUE);
    int keySize = nextKeySize(r);
    int valueSize = nextValueSize(r);
    byte[] key = TestUtils.randomBytes(keySize, r);
    String skey = Utils.toHex(key);

    byte[] value = (byte[]) this.cache.get(skey);
    if (value == null) {
      return false;
    }
    try {
      assertEquals(valueSize, value.length);
    } catch (AssertionError e) {
      e.printStackTrace();
      return false;
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

}
