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

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.After;
import org.junit.BeforeClass;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotdata.cache.controllers.AQBasedPromotionController;
import com.carrotdata.cache.controllers.AdmissionController;
import com.carrotdata.cache.controllers.BaseAdmissionController;
import com.carrotdata.cache.controllers.LRCRecyclingSelector;
import com.carrotdata.cache.controllers.MinAliveRecyclingSelector;
import com.carrotdata.cache.controllers.RecyclingSelector;
import com.carrotdata.cache.eviction.EvictionPolicy;
import com.carrotdata.cache.eviction.FIFOEvictionPolicy;
import com.carrotdata.cache.eviction.LRUEvictionPolicy;
import com.carrotdata.cache.eviction.SLRUEvictionPolicy;
import com.carrotdata.cache.util.TestUtils;
import com.carrotdata.cache.util.UnsafeAccess;

public class TestCompressedHybridCacheMultithreadedZipf
    extends TestCompressedMemoryCacheMultithreadedZipf {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestCompressedHybridCacheMultithreadedZipf.class);

  int victim_segmentSize = 16 * 1024 * 1024;

  long victim_maxCacheSize = 1000L * victim_segmentSize;

  double victim_minActiveRatio = 0.5;

  int victim_scavengerInterval = 10; // seconds

  double victim_scavDumpBelowRatio = 0.5;

  boolean victim_promoteOnHit = true;

  protected Class<? extends EvictionPolicy> victim_epClz = FIFOEvictionPolicy.class;

  protected Class<? extends RecyclingSelector> victim_rsClz = LRCRecyclingSelector.class;

  protected Class<? extends AdmissionController> victim_acClz = BaseAdmissionController.class;

  
  @BeforeClass
  public static void start() {
    //System.setProperty("MALLOC_DEBUG", "true");
  }
  
  @Override
  public void setUp() throws IOException, URISyntaxException {
    super.setUp();
    // Parent cache
    this.memory = true;
    this.numRecords = 1000000;
    this.numIterations = this.numRecords;
    this.numThreads = 4;
    this.minActiveRatio = 0.9;
    this.maxCacheSize = 20L * this.segmentSize;
    this.scavDumpBelowRatio = 0.1;
    this.dictionaryEnabled = true;
    this.hybridCacheInverseMode = true;
    this.pcController = AQBasedPromotionController.class;

    // victim cache
    // this.victim_segmentSize = 4 * 1024 * 1024;
    // this.victim_maxCacheSize = 1000L * this.victim_segmentSize;
    this.victim_minActiveRatio = 0.5;
    this.victim_scavDumpBelowRatio = 1.0;
    this.victim_scavengerInterval = 10;
    this.victim_promoteOnHit = true;
    this.victim_epClz = SLRUEvictionPolicy.class;
    this.victim_rsClz = MinAliveRecyclingSelector.class;
    this.victim_acClz = BaseAdmissionController.class;

  }

  @After
  public void tearDown() throws IOException {
    Cache victim = cache.getVictimCache();
    LOG.info("main cache: size={} hit rate={} items={}", cache.getStorageAllocated(),
      cache.getHitRate(), cache.size());

    LOG.info("victim cache: size={} hit rate={} items={}", victim.getStorageAllocated(),
      victim.getHitRate(), victim.size());

    super.tearDown();
    // Delete temp data
    TestUtils.deleteCacheFiles(victim);
  }

  protected Cache createCache() throws IOException {
    Cache parent = super.createCache();

    String cacheName = "victim";
    // Data directory
    Path victim_rootDirPath = Files.createTempDirectory(null);
    String rootDir = victim_rootDirPath.toFile().getAbsolutePath();

    Builder builder = new Builder(cacheName);

    builder.withCacheDataSegmentSize(victim_segmentSize).withCacheMaximumSize(victim_maxCacheSize)
        .withScavengerRunInterval(victim_scavengerInterval)
        .withScavengerDumpEntryBelowMax(victim_scavDumpBelowRatio)
        .withCacheEvictionPolicy(victim_epClz.getName())
        .withRecyclingSelector(victim_rsClz.getName()).withCacheRootDir(rootDir)
        .withMinimumActiveDatasetRatio(victim_minActiveRatio)
        .withVictimCachePromoteOnHit(victim_promoteOnHit)
        .withAdmissionController(victim_acClz.getName());

    LOG.info("Victim start");
    Cache victim = builder.buildDiskCache();
    LOG.info("Victim finished");

    parent.setVictimCache(victim);

    return parent;
  }

}
