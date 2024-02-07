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

import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.After;
import org.junit.Test;

import com.carrot.cache.controllers.AdmissionController;
import com.carrot.cache.controllers.BaseAdmissionController;
import com.carrot.cache.controllers.LRCRecyclingSelector;
import com.carrot.cache.controllers.MinAliveRecyclingSelector;
import com.carrot.cache.controllers.RecyclingSelector;
import com.carrot.cache.eviction.EvictionPolicy;
import com.carrot.cache.eviction.FIFOEvictionPolicy;
import com.carrot.cache.eviction.LRUEvictionPolicy;
import com.carrot.cache.util.TestUtils;

public class TestHybridCompressedCacheMultithreadedZipf extends TestOffheapCompressedCacheMultithreadedZipf {
  
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
  public void setUp() throws IOException, URISyntaxException {
    super.setUp();
    // Parent cache
    this.offheap = true;
    this.numRecords = 2000000;
    this.numIterations = 2 * this.numRecords;
    this.numThreads = 1;
    this.minActiveRatio = 0.9;
    this.maxCacheSize = 10L * this.segmentSize;
   
    // victim cache
    this.victim_segmentSize = 4 * 1024 * 1024;
    this.victim_maxCacheSize = 1000L * this.victim_segmentSize;
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
    System.out.println("Memory API: eviction=LRU, selector=MinAlive");

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
    System.out.printf("main cache: size=%d hit rate=%f items=%d\n", 
      cache.getStorageAllocated(), cache.getHitRate(), cache.size());
    
    System.out.printf("victim cache: size=%d hit rate=%f items=%d\n", 
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
      .withScavengerDumpEntryBelowStart(victim_scavDumpBelowRatio)
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
