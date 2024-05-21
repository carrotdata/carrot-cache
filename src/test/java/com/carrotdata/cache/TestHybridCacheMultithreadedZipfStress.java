/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.carrotdata.cache;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotdata.cache.controllers.AQBasedAdmissionController;
import com.carrotdata.cache.controllers.AdmissionController;
import com.carrotdata.cache.controllers.BaseAdmissionController;
import com.carrotdata.cache.controllers.LRCRecyclingSelector;
import com.carrotdata.cache.controllers.MinAliveRecyclingSelector;
import com.carrotdata.cache.controllers.RecyclingSelector;
import com.carrotdata.cache.eviction.EvictionPolicy;
import com.carrotdata.cache.eviction.FIFOEvictionPolicy;
import com.carrotdata.cache.eviction.SLRUEvictionPolicy;
import com.carrotdata.cache.util.Epoch;
import com.carrotdata.cache.util.TestUtils;

public class TestHybridCacheMultithreadedZipfStress extends TestCacheMultithreadedZipfBase {
  private static final Logger LOG = LoggerFactory.getLogger(TestHybridCacheMultithreadedZipfStress.class);

  int victim_segmentSize = 16 * 1024 * 1024;

  long victim_maxCacheSize = 100L * victim_segmentSize;

  double victim_minActiveRatio = 0.5;

  int victim_scavengerInterval = 10; // seconds

  double victim_scavDumpBelowRatio = 0.5;
  
  double victim_aqStartRatio = 0.3;

  boolean victim_promoteOnHit = true;

  double victim_promoteThreshold = 0.9;

  boolean hybridCacheInverseMode = false;

  protected Class<? extends EvictionPolicy> victim_epClz = FIFOEvictionPolicy.class;

  protected Class<? extends RecyclingSelector> victim_rsClz = LRCRecyclingSelector.class;

  protected Class<? extends AdmissionController> victim_acClz = BaseAdmissionController.class;

  @Before
  public void setUp() {
    // Parent cache
    this.offheap = true;
    this.numRecords = 10000000;
    this.numIterations = 10 * this.numRecords;
    this.numThreads = 4;
    this.minActiveRatio = 0.9;
    this.segmentSize = 4 * 1024 * 1024;
    this.maxCacheSize = 1000L * this.segmentSize; // 16 GB in RAM
    this.epClz = SLRUEvictionPolicy.class;
    this.rsClz = MinAliveRecyclingSelector.class;
    //this.acClz = AQBasedAdmissionController.class;
    this.aqStartRatio = 1.0;
    this.scavengerInterval = 4; // scavenger interval in sec
    this.hybridCacheInverseMode = false;
    this.scavDumpBelowRatio = 0.2;

    // victim cache
    this.victim_segmentSize = 64 * 1024 * 1024;
    this.victim_maxCacheSize = 200L * this.victim_segmentSize; // 160GB
    this.victim_minActiveRatio = 0.0;
    this.victim_scavDumpBelowRatio = 0.2;
    this.victim_scavengerInterval = 1000;
    this.victim_promoteOnHit = false;
    this.victim_promoteThreshold = 0.9;
    this.victim_epClz = SLRUEvictionPolicy.class;
    this.victim_rsClz = LRCRecyclingSelector.class;
    this.zipfAlpha = 0.9;

    Epoch.reset();
    
  }

  @After
  public void tearDown() throws IOException {
    Cache victim = cache.getVictimCache();
    LOG.info("main cache: size={} hit rate={} items={}", cache.getStorageAllocated(),
      cache.getHitRate(), cache.size());

    if (victim != null) {
      LOG.info("victim cache: size={} hit rate={} items={}", victim.getStorageAllocated(),
        victim.getHitRate(), victim.size());
    }
    super.tearDown();
    // Delete temp data
    if (victim != null) {
      TestUtils.deleteCacheFiles(victim);
    }
  }

  @Override
  protected Builder withAddedConfigurations(Builder b) {
    b.withCacheHybridInverseMode(hybridCacheInverseMode)
    .withCacheSpinWaitTimeOnHighPressure(20000)
    .withSLRUInsertionPoint(6);
    return b;
  }

  protected Cache createCache() throws IOException {
    Cache parent = super.createCache();

    String cacheName = this.victimCacheName;
    // Data directory
    Path victim_rootDirPath = Files.createTempDirectory(null);
    String rootDir = victim_rootDirPath.toFile().getAbsolutePath();

    Builder builder = new Builder(cacheName);

    builder.withCacheDataSegmentSize(victim_segmentSize).withCacheMaximumSize(victim_maxCacheSize)
        .withScavengerRunInterval(victim_scavengerInterval)
        .withScavengerDumpEntryBelowMin(victim_scavDumpBelowRatio)
        .withCacheEvictionPolicy(victim_epClz.getName())
        .withRecyclingSelector(victim_rsClz.getName())
        .withCacheRootDir(rootDir)
        .withMinimumActiveDatasetRatio(victim_minActiveRatio)
        .withVictimCachePromoteOnHit(victim_promoteOnHit)
        .withVictimCachePromotionThreshold(victim_promoteThreshold)
        .withAdmissionController(victim_acClz.getName())
        .withCacheSpinWaitTimeOnHighPressure(10000)
        .withScavengerNumberOfThreads(1)
        .withAdmissionQueueStartSizeRatio(victim_aqStartRatio);
    Cache victim = builder.buildDiskCache();
    parent.setVictimCache(victim);
    parent.registerJMXMetricsSink();
    
    return parent;
  }

  @Test
  public void testLRUEvictionAndMinAliveSelectorBytesAPI() throws IOException {
    LOG.info("Bytes API: eviction=SLRU, selector=MinAlive");
    this.parentCacheName = "RAM-AC-OFF";
    this.victimCacheName = "DISK-AC-OFF";
    super.testContinuosLoadBytesRun();
  }


  @Test
  public void testLRUEvictionAndMinAliveSelectorWithAQBytesAPI() throws IOException {
    LOG.info("Bytes API: eviction=SLRU, selector=MinAlive - AQ");
    this.parentCacheName = "RAM-AC-ON";
    this.victimCacheName = "DISK-AC-ON";
    this.victim_acClz = AQBasedAdmissionController.class;
    this.victim_aqStartRatio = 1.0;
    super.testContinuosLoadBytesRun();
  }
}
