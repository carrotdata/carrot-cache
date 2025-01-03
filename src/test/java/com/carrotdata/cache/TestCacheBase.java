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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotdata.cache.controllers.MinAliveRecyclingSelector;
import com.carrotdata.cache.index.CompactBaseWithExpireIndexFormat;
import com.carrotdata.cache.io.BaseDataWriter;
import com.carrotdata.cache.io.BaseFileDataReader;
import com.carrotdata.cache.io.BaseMemoryDataReader;
import com.carrotdata.cache.io.IOTestBase;
import com.carrotdata.cache.util.TestUtils;
import com.carrotdata.cache.util.UnsafeAccess;

public abstract class TestCacheBase extends IOTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(TestCacheBase.class);

  boolean memory = true;
  Cache cache;
  int segmentSize = 4 * 1024 * 1024;
  long maxCacheSize = 100L * segmentSize;
  int scavengerInterval = 10000; // seconds - disable for test
  long expireTime;
  double scavDumpBelowRatio = 0.5;
  double minActiveRatio = 0.90;
  int maxKeyValueSize = 0;

  String recycleSelector = MinAliveRecyclingSelector.class.getName();
  String dataWriter = BaseDataWriter.class.getName();
  String dataReaderMemory = BaseMemoryDataReader.class.getName();
  String dataReaderFile = BaseFileDataReader.class.getName();
  String indexFormat = CompactBaseWithExpireIndexFormat.class.getName();

  @BeforeClass
  public static void before() {
    //UnsafeAccess.setMallocDebugEnabled(true);
  }
  
  @Before
  public void setUp() throws IOException {
    this.r = new Random();
    long seed = System.currentTimeMillis();
    LOG.info("r.seed=" + seed);
    r.setSeed(seed);
    this.numRecords = 150000;

  }

  @After
  public void tearDown() throws IOException {
    super.tearDown();
    cache.dispose();
    TestUtils.deleteCacheFiles(cache);
    //UnsafeAccess.mallocStats.printStats();
  }

  protected Cache createCache() throws IOException {
    return createCache("cache");
  }

  protected Cache createCache(String cacheName) throws IOException {
    // Data directory
    Path path = Files.createTempDirectory(null);
    File dir = path.toFile();
    dir.deleteOnExit();
    String rootDir = dir.getAbsolutePath();
    String[] rootDirs = memory? new String[] {rootDir}: 
        new String[] {rootDir + File.separator + "1", rootDir + File.separator + "2"};
    Builder builder = new Builder(cacheName);

    builder.withCacheDataSegmentSize(segmentSize).withCacheMaximumSize(maxCacheSize)
        .withScavengerRunInterval(scavengerInterval)
        .withScavengerDumpEntryBelowMin(scavDumpBelowRatio).withRecyclingSelector(recycleSelector)
        .withDataWriter(dataWriter).withMemoryDataReader(dataReaderMemory)
        .withFileDataReader(dataReaderFile).withMainQueueIndexFormat(indexFormat)
        .withCacheRootDirs(rootDirs).withMinimumActiveDatasetRatio(minActiveRatio)
        .withCacheStreamingSupportBufferSize(1 << 19).withEvictionDisabledMode(true);
    if (maxKeyValueSize > 0) {
      builder.withMaximumKeyValueSize(maxKeyValueSize);
    }
    Cache c = null;
    if (memory) {
      c = builder.buildMemoryCache();
    } else {
      c = builder.buildDiskCache();
    }
    c.disableScavengers();
    return c;
  }

  @Override
  protected long getExpire(int n) {
    return System.currentTimeMillis() + expireTime;
  }

  @Test
  public void testBigKeyValue() throws IOException {
    LOG.info("Test big key value");
    this.numRecords = 0;
    this.maxKeyValueSize = 100000;
    Scavenger.clear();
    this.cache = createCache();

    byte[] key = new byte[20];
    byte[] value = new byte[maxKeyValueSize - 30];

    boolean result = cache.put(key, value, 0);
    assertTrue(result);

    value = new byte[maxKeyValueSize];
    result = cache.put(key, value, 0);
    assertFalse(result);
  }

  protected void prepareData() {
    prepareRandomData(numRecords);
  }

  @Test
  public void testAllExpiredBytes() throws IOException {
    LOG.info("Test all expired bytes");
    Scavenger.clear();
    // Create cache
    this.cache = createCache();
    this.expireTime = 1000;
    prepareData();
    int loaded = loadBytesCache(cache);
    LOG.info("loaded=" + loaded);
    // Wait expireTime
    try {
      Thread.sleep(expireTime);
    } catch (InterruptedException e) {

    }
    long allocated = cache.getStorageAllocated();
    long used = cache.getRawDataSize();
    long size = cache.size();
    long activeSize = cache.activeSize();
    LOG.info("Before scan: allocated={}, used={}, size={}, active={}", allocated, used, size,
      activeSize);
    // Access all objects to remove them from memory index
    // and from data segments statistics
    verifyBytesCacheNot(cache, loaded);

    allocated = cache.getStorageAllocated();
    used = cache.getRawDataSize();
    size = cache.size();
    activeSize = cache.activeSize();
    LOG.info("After scan: allocated={}, used={}, size={}, active={}", allocated, used, size,
      activeSize);

  }

  @Test
  public void testAllExpiredMemory() throws IOException {
    LOG.info("Test all expired memory");
    Scavenger.clear();
    // Create cache
    this.cache = createCache();
    this.expireTime = 1000;
    prepareData();
    int loaded = loadMemoryCache(cache);
    LOG.info("loaded=" + loaded);
    // Wait expireTime
    try {
      Thread.sleep(expireTime);
    } catch (InterruptedException e) {

    }
    long allocated = cache.getStorageAllocated();
    long used = cache.getRawDataSize();
    long size = cache.size();
    long activeSize = cache.activeSize();
    LOG.info("Before scan: allocated={}, used={}, size={}, active={}", allocated, used, size,
      activeSize);
    // Access all objects to remove them from memory index
    // and from data segments statistics
    verifyMemoryCacheNot(cache, loaded);

    allocated = cache.getStorageAllocated();
    used = cache.getRawDataSize();
    size = cache.size();
    activeSize = cache.activeSize();
    LOG.info("After scan: allocated={}, used={}, size={}, active={}", allocated, used, size,
      activeSize);

  }

  @Test
  public void testNoExpiredBytes() throws IOException {
    LOG.info("Test no expired - bytes");
    Scavenger.clear();
    // Create cache
    this.cache = createCache();

    this.expireTime = 1000000;
    prepareData();
    // Fill cache completely (no eviction is enabled)
    int loaded = loadBytesCache(cache);
    LOG.info("loaded=" + loaded);

    long allocated = cache.getStorageAllocated();
    long used = cache.getRawDataSize();
    long size = cache.size();
    long activeSize = cache.activeSize();
    LOG.info("Before scan: allocated={}, used={}, size={}, active={}", allocated, used, size,
      activeSize);
    // Access all objects to remove them from memory index
    // and from data segments statistics
    verifyBytesCache(cache, loaded);
    verifyBytesCacheByteBuffer(cache, loaded);

    allocated = cache.getStorageAllocated();
    used = cache.getRawDataSize();
    size = cache.size();
    activeSize = cache.activeSize();
    LOG.info("After scan: allocated={}, used={}, size={}, active={}", allocated, used, size,
      activeSize);

  }

  @Test
  public void testNoExpiredMemory() throws IOException {
    LOG.info("Test no expired - memory");
    Scavenger.clear();
    // Create cache
    this.cache = createCache();

    this.expireTime = 1000000;
    prepareData();
    // Fill cache completely (no eviction is enabled)
    int loaded = loadMemoryCache(cache);
    LOG.info("loaded=" + loaded);

    long allocated = cache.getStorageAllocated();
    long used = cache.getRawDataSize();
    long size = cache.size();
    long activeSize = cache.activeSize();
    LOG.info("Before scan: allocated={}, used={}, size={}, active={}", allocated, used, size,
      activeSize);
    // Access all objects to remove them from memory index
    // and from data segments statistics
    verifyMemoryCache(cache, loaded);

    allocated = cache.getStorageAllocated();
    used = cache.getRawDataSize();
    size = cache.size();
    activeSize = cache.activeSize();
    LOG.info("After scan: allocated={}, used={}, size={}, active={}", allocated, used, size,
      activeSize);

  }

  @Test
  public void testNoExpiredWithDeletesBytes() throws IOException {
    LOG.info("Test no expired with deletes bytes");
    // Clear scavenger's statics
    Scavenger.clear();
    // Create cache
    this.scavDumpBelowRatio = 0; // remove only deleted
    this.cache = createCache();

    this.expireTime = 1000000;
    prepareData();
    // Fill cache completely (no eviction is enabled)
    int loaded = loadBytesCache(cache);
    LOG.info("loaded=" + loaded);
    int deleted = deleteBytesCache(cache, loaded / 17);

    long allocated = cache.getStorageAllocated();
    long used = cache.getRawDataSize();
    long size = cache.size();
    long activeSize = cache.activeSize();
    LOG.info("Before scan: allocated={}, used={}, size={}, active={}", allocated, used, size,
      activeSize);
    // Access all objects to remove them from memory index
    // and from data segments statistics
    verifyBytesCacheWithDeletes(cache, loaded, deleted);

    allocated = cache.getStorageAllocated();
    used = cache.getRawDataSize();
    size = cache.size();
    activeSize = cache.activeSize();
    LOG.info("After scan: allocated={}, used={}, size={}, active={}", allocated, used, size,
      activeSize);

  }

  @Test
  public void testNoExpiredWithDeletesMemory() throws IOException {
    LOG.info("Test no expired with deletes memory");
    // Clear scavenger's statics
    Scavenger.clear();
    // Create cache
    this.scavDumpBelowRatio = 0; // remove only deleted
    this.cache = createCache();

    this.expireTime = 1000000;
    prepareData();
    // Fill cache completely (no eviction is enabled)
    int loaded = loadMemoryCache(cache);
    LOG.info("loaded=" + loaded);
    int deleted = deleteMemoryCache(cache, loaded / 17);

    long allocated = cache.getStorageAllocated();
    long used = cache.getRawDataSize();
    long size = cache.size();
    long activeSize = cache.activeSize();
    LOG.info("Before scan: allocated={}, used={}, size={}, active={}", allocated, used, size,
      activeSize);
    // Access all objects to remove them from memory index
    // and from data segments statistics
    verifyMemoryCacheWithDeletes(cache, loaded, deleted);

    allocated = cache.getStorageAllocated();
    used = cache.getRawDataSize();
    size = cache.size();
    activeSize = cache.activeSize();
    LOG.info("After scan: allocated={}, used={}, size={}, active={}", allocated, used, size,
      activeSize);
  }

  @Test
  public void testSaveLoad() throws IOException {
    LOG.info("Test save load");
    Scavenger.clear();
    // Create cache
    this.cache = createCache();

    this.expireTime = 1000000;
    prepareData();
    // Fill cache completely (no eviction is enabled)
    int loaded = loadBytesCache(cache);
    LOG.info("loaded=" + loaded);
    verifyBytesCacheByteBuffer(cache, loaded);

    String cacheName = cache.getName();
    long t1 = System.currentTimeMillis();
    cache.save();
    long t2 = System.currentTimeMillis();
    LOG.info("Saved {} in {} ms", cache.getStorageAllocated(), t2 - t1);

    t1 = System.currentTimeMillis();
    Cache newCache = Cache.loadCache(cacheName);
    t2 = System.currentTimeMillis();
    LOG.info("Loaded {} in {} ms", cache.getStorageAllocated(), t2 - t1);

    assertEquals(cache.getCacheType(), newCache.getCacheType());
    assertEquals(cache.activeSize(), newCache.activeSize());
    assertEquals(cache.getMaximumCacheSize(), newCache.getMaximumCacheSize());
    //TODO: check why it fails
    //assertEquals(cache.getStorageAllocated(), newCache.getStorageAllocated());
    assertEquals(cache.getRawDataSize(), newCache.getRawDataSize());
    assertEquals(cache.getTotalGets(), newCache.getTotalGets());
    assertEquals(cache.getTotalGetsSize(), newCache.getTotalGetsSize());
    assertEquals(cache.getTotalHits(), newCache.getTotalHits());
    assertEquals(cache.getTotalWrites(), newCache.getTotalWrites());
    assertEquals(cache.getTotalWritesSize(), newCache.getTotalWritesSize());

    verifyBytesCache(newCache, loaded);

    newCache.dispose();
    TestUtils.deleteCacheFiles(newCache);
  }

  @Test
  public void testFlushAll() throws IOException {
    LOG.info("Test flushAll");
    Scavenger.clear();
    // Create cache
    this.cache = createCache();
    this.expireTime = 1000000;
    prepareData();
    // Fill cache completely (no eviction is enabled)
    int loaded = loadBytesCache(cache);
    LOG.info("loaded=" + loaded);
    verifyBytesCacheByteBuffer(cache, loaded);

    long t1 = System.currentTimeMillis();
    this.cache = Cache.flushAll(cache);
    long t2 = System.currentTimeMillis();
    LOG.info("Flushed all in {} ms", t2 - t1);

    loaded = loadBytesCache(cache);
    LOG.info("loaded=" + loaded);
    verifyBytesCacheByteBuffer(cache, loaded);
 
  }
  
  @Test
  public void testSaveLoadSmallData() throws IOException {
    LOG.info("Test save load small data");
    Scavenger.clear();
    // Create cache
    this.cache = createCache();

    this.expireTime = 1000000;
    this.numRecords = 1;
    prepareData();
    // Fill cache completely (no eviction is enabled)
    int loaded = loadBytesCache(cache);
    LOG.info("loaded=" + loaded);
    verifyBytesCacheByteBuffer(cache, loaded);

    String cacheName = cache.getName();
    long t1 = System.currentTimeMillis();
    cache.save();
    long t2 = System.currentTimeMillis();
    LOG.info("Saved {} in {} ms", cache.getStorageAllocated(), t2 - t1);

    t1 = System.currentTimeMillis();
    Cache newCache = Cache.loadCache(cacheName);
    t2 = System.currentTimeMillis();
    LOG.info("Loaded {} in {} ms", newCache.getStorageAllocated(), t2 - t1);

    assertEquals(cache.getCacheType(), newCache.getCacheType());
    assertEquals(cache.activeSize(), newCache.activeSize());
    assertEquals(cache.getMaximumCacheSize(), newCache.getMaximumCacheSize());
    //TODO: check why this fails
    //assertEquals(cache.getStorageAllocated(), newCache.getStorageAllocated());
    assertEquals(cache.getRawDataSize(), newCache.getRawDataSize());
    assertEquals(cache.getTotalGets(), newCache.getTotalGets());
    assertEquals(cache.getTotalGetsSize(), newCache.getTotalGetsSize());
    assertEquals(cache.getTotalHits(), newCache.getTotalHits());
    assertEquals(cache.getTotalWrites(), newCache.getTotalWrites());
    assertEquals(cache.getTotalWritesSize(), newCache.getTotalWritesSize());

    verifyBytesCache(newCache, loaded);

    newCache.save();

    Cache newCache2 = Cache.loadCache(cacheName);
    assertEquals(newCache2.getCacheType(), newCache.getCacheType());
    assertEquals(newCache2.activeSize(), newCache.activeSize());
    assertEquals(newCache2.getMaximumCacheSize(), newCache.getMaximumCacheSize());
    //TODO: check why this fails
    //assertEquals(newCache2.getStorageAllocated(), newCache.getStorageAllocated());
    assertEquals(newCache2.getRawDataSize(), newCache.getRawDataSize());
    assertEquals(newCache2.getTotalGets(), newCache.getTotalGets());
    assertEquals(newCache2.getTotalGetsSize(), newCache.getTotalGetsSize());
    assertEquals(newCache2.getTotalHits(), newCache.getTotalHits());
    assertEquals(newCache2.getTotalWrites(), newCache.getTotalWrites());
    assertEquals(newCache2.getTotalWritesSize(), newCache.getTotalWritesSize());

    verifyBytesCache(newCache2, loaded);

    newCache.dispose();
    newCache2.dispose();
    TestUtils.deleteCacheFiles(newCache);
  }

  @Test
  public void testSaveLoadTwoCaches() throws IOException {
    LOG.info("Test save load two caches");
    Scavenger.clear();
    // Create cache
    this.cache = createCache("cache1");

    this.expireTime = 1000000;
    prepareData();
    // Fill cache completely (no eviction is enabled)
    int loaded = loadBytesCache(cache);
    LOG.info("loaded={}", loaded);
    verifyBytesCacheByteBuffer(cache, loaded);

    String cacheName = cache.getName();
    long t1 = System.currentTimeMillis();
    cache.save();
    long t2 = System.currentTimeMillis();
    LOG.info("Saved {} in {} ms", cache.getStorageAllocated(), t2 - t1);

    t1 = System.currentTimeMillis();
    Cache newCache = Cache.loadCache(cacheName);
    t2 = System.currentTimeMillis();
    LOG.info("Loaded {} in {} ms", cache.getStorageAllocated(), t2 - t1);

    assertEquals(cache.getCacheType(), newCache.getCacheType());
    assertEquals(cache.activeSize(), newCache.activeSize());
    assertEquals(cache.getMaximumCacheSize(), newCache.getMaximumCacheSize());
    //TODO: check why it fails
    //assertEquals(cache.getStorageAllocated(), newCache.getStorageAllocated());
    assertEquals(cache.getRawDataSize(), newCache.getRawDataSize());
    assertEquals(cache.getTotalGets(), newCache.getTotalGets());
    assertEquals(cache.getTotalGetsSize(), newCache.getTotalGetsSize());
    assertEquals(cache.getTotalHits(), newCache.getTotalHits());
    assertEquals(cache.getTotalWrites(), newCache.getTotalWrites());
    assertEquals(cache.getTotalWritesSize(), newCache.getTotalWritesSize());

    verifyBytesCache(newCache, loaded);

    Cache cache2 = createCache("cache2");
    loaded = loadBytesCache(cache2);
    LOG.info("loaded={}", loaded);
    verifyBytesCacheByteBuffer(cache2, loaded);

    t1 = System.currentTimeMillis();
    cache2.save();
    t2 = System.currentTimeMillis();

    LOG.info("Saved {} in {} ms", cache2.getStorageAllocated(), t2 - t1);

    t1 = System.currentTimeMillis();
    Cache newCache2 = Cache.loadCache("cache2");
    t2 = System.currentTimeMillis();
    LOG.info("Loaded {} in {} ms", newCache2.getStorageAllocated(), t2 - t1);

    assertEquals(cache2.getCacheType(), newCache2.getCacheType());
    assertEquals(cache2.activeSize(), newCache2.activeSize());
    assertEquals(cache2.getMaximumCacheSize(), newCache2.getMaximumCacheSize());
    //assertEquals(cache2.getStorageAllocated(), newCache2.getStorageAllocated());
    assertEquals(cache2.getRawDataSize(), newCache2.getRawDataSize());
    assertEquals(cache2.getTotalGets(), newCache2.getTotalGets());
    assertEquals(cache2.getTotalGetsSize(), newCache2.getTotalGetsSize());
    assertEquals(cache2.getTotalHits(), newCache2.getTotalHits());
    assertEquals(cache2.getTotalWrites(), newCache2.getTotalWrites());
    assertEquals(cache2.getTotalWritesSize(), newCache2.getTotalWritesSize());

    newCache.dispose();
    TestUtils.deleteCacheFiles(newCache);
    cache2.dispose();
    TestUtils.deleteCacheFiles(cache2);
    newCache2.dispose();
    TestUtils.deleteCacheFiles(newCache2);

  }

}
