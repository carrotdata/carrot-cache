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
package com.carrotdata.cache;

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotdata.cache.controllers.MinAliveRecyclingSelector;
import com.carrotdata.cache.io.IOTestBase;
import com.carrotdata.cache.util.TestUtils;

public class TestMemoryCacheVacuumMode extends IOTestBase {
  protected Logger LOG = LoggerFactory.getLogger(TestMemoryCacheVacuumMode.class);

  boolean memory = true;
  Cache cache;
  int segmentSize = 4 * 1024 * 1024;
  long maxCacheSize = 200L * segmentSize;
  int vacuumCleanerInterval = 5; // seconds 
  long expireTime;
  double minActiveRatio = 0.99;

  String recycleSelector = MinAliveRecyclingSelector.class.getName();


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

    Builder builder = new Builder(cacheName);

    builder.withCacheDataSegmentSize(segmentSize).withCacheMaximumSize(maxCacheSize)
        .withScavengerRunInterval(vacuumCleanerInterval)
        .withCacheRootDir(rootDir)
        .withMinimumActiveDatasetRatio(minActiveRatio)
        .withVacuumCleanerInterval(vacuumCleanerInterval);
    
    Cache c = null;
    if (memory) {
      c = builder.buildMemoryCache();
    } else {
      c = builder.buildDiskCache();
    }
    return c;
  }

  @Override
  protected long getExpire(int n) {
    return System.currentTimeMillis() + expireTime;
  }

  protected void prepareData() {
    prepareRandomData(numRecords);
  }
 
  @Test
  public void testVacuumAfterDeletes() throws IOException, InterruptedException {
    LOG.info("Test vacuum after delete");
    // Clear scavenger's statics
    Scavenger.clear();
    // Create cache
    this.cache = createCache();
    LOG.info("Cache created");

    this.expireTime = 1000000;
    prepareData();
    // Fill cache completely (no eviction is enabled)
    LOG.info("Data prepared");

    int loaded = loadMemoryCache(cache);
    LOG.info("Data loaded=" + loaded);
    long allocated = cache.getStorageAllocated();
    long used = cache.getRawDataSize();
    long size = cache.size();
    long activeSize = cache.activeSize();
    LOG.info("After delete immediately: allocated={}, used={}, size={}, active={}", allocated, used, size,
      activeSize);
    int deleted = deleteMemoryCache(cache, loaded);

    Thread.sleep(vacuumCleanerInterval * 1000 + 1000);
    
    allocated = cache.getStorageAllocated();
    used = cache.getRawDataSize();
    size = cache.size();
    activeSize = cache.activeSize();
    LOG.info("After vacuum scan : allocated={}, used={}, size={}, active={}", allocated, used, size,
      activeSize);
    assertTrue(allocated == this.segmentSize);
  }

}
