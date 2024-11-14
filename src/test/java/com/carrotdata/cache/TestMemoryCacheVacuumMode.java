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
        .withCacheRootDirs(new String[] {rootDir})
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
