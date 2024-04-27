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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.onecache.core.controllers.AdmissionController;
import com.onecache.core.controllers.BaseAdmissionController;
import com.onecache.core.controllers.LRCRecyclingSelector;
import com.onecache.core.controllers.MinAliveRecyclingSelector;
import com.onecache.core.controllers.RecyclingSelector;
import com.onecache.core.eviction.EvictionPolicy;
import com.onecache.core.eviction.FIFOEvictionPolicy;
import com.onecache.core.eviction.LRUEvictionPolicy;
import com.onecache.core.util.TestUtils;

public class TestHybridCacheMultithreadedZipf extends TestOffheapCacheMultithreadedZipf {
  private static final Logger LOG = LoggerFactory.getLogger(TestHybridCacheMultithreadedZipf.class);

  int victim_segmentSize = 16 * 1024 * 1024;
  
  long victim_maxCacheSize = 1000L * victim_segmentSize;
  
  double victim_minActiveRatio = 0.5;
  
  int victim_scavengerInterval = 10; // seconds
  
  double victim_scavDumpBelowRatio = 0.5;
  
  boolean victim_promoteOnHit = true;
    
  protected Class<? extends EvictionPolicy> victim_epClz = FIFOEvictionPolicy.class;
  
  protected Class<? extends RecyclingSelector> victim_rsClz = LRCRecyclingSelector.class;
  
  protected Class<? extends AdmissionController> victim_acClz = BaseAdmissionController.class;
  
  @Override
  public void setUp() {
    // Parent cache
    this.offheap = true;
    this.numRecords = 2000000;
    this.numIterations = 2 * this.numRecords;
    this.numThreads = 4;
    this.minActiveRatio = 0.9;
    this.maxCacheSize = 10L * this.segmentSize;
    // victim cache
    //this.victim_segmentSize = 4 * 1024 * 1024;
    //this.victim_maxCacheSize = 1000L * this.victim_segmentSize;
    this.victim_minActiveRatio = 0.5;
    this.victim_scavDumpBelowRatio = 0.5;
    this.victim_scavengerInterval = 10;
    this.victim_promoteOnHit = false;
    this.victim_epClz = LRUEvictionPolicy.class;
    this.victim_rsClz = MinAliveRecyclingSelector.class;
    this.victim_acClz = BaseAdmissionController.class;
    
  }
  
  @Test
  public void testLRUEvictionAndMinAliveSelectorMemoryAPI() throws IOException {
    LOG.info("Memory API: eviction=LRU, selector=MinAlive");

    this.evictionDisabled = false;
    this.scavengerInterval = 2; // scavenger interval in sec
    this.epClz = LRUEvictionPolicy.class;
    this.rsClz = MinAliveRecyclingSelector.class;
    //this.scavDumpBelowRatio = 1.0;
    super.testContinuosLoadMemoryRun();
  }
  
  @After  
  public void tearDown() throws IOException {
    Cache victim = cache.getVictimCache();
    LOG.info("main cache: size={} hit rate={} items={}", 
      cache.getStorageAllocated(), cache.getHitRate(), cache.size());
    
    LOG.info("victim cache: size={} hit rate={} items={}", 
      victim.getStorageAllocated(), victim.getHitRate(), victim.size());
    
    super.tearDown();
    // Delete temp data
    TestUtils.deleteCacheFiles(victim);
  }
  
  protected  Cache createCache() throws IOException{
    Cache parent = super.createCache();
        
    String cacheName = "victim";
    // Data directory
    Path victim_rootDirPath = Files.createTempDirectory(null);
    String rootDir = victim_rootDirPath.toFile().getAbsolutePath();
    
    Builder builder = new Builder(cacheName);
    
    builder
      .withCacheDataSegmentSize(victim_segmentSize)
      .withCacheMaximumSize(victim_maxCacheSize)
      .withScavengerRunInterval(victim_scavengerInterval)
      .withScavengerDumpEntryBelowMin(victim_scavDumpBelowRatio)
      .withCacheEvictionPolicy(victim_epClz.getName())
      .withRecyclingSelector(victim_rsClz.getName())
      .withCacheRootDir(rootDir)
      .withMinimumActiveDatasetRatio(victim_minActiveRatio)
      .withVictimCachePromoteOnHit(victim_promoteOnHit)
      .withAdmissionController(victim_acClz.getName());
    
    Cache victim = builder.buildDiskCache();
    parent.setVictimCache(victim);

    return parent;
  }

}
