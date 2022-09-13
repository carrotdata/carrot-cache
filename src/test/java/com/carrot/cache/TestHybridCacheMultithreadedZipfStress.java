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

import com.carrot.cache.controllers.BaseAdmissionController;
import com.carrot.cache.controllers.LRCRecyclingSelector;
import com.carrot.cache.eviction.FIFOEvictionPolicy;

public class TestHybridCacheMultithreadedZipfStress extends TestHybridCacheMultithreadedZipf {
  

  @Override
  public void setUp() {
    // Parent cache
    this.offheap = true;
    this.numRecords = 10000000;
    this.numIterations = 2 * this.numRecords;
    this.numThreads = 4;
    this.minActiveRatio = 0.9;
    this.maxCacheSize = 1000L * this.segmentSize;
    // victim cache
    this.victim_segmentSize = 16 * 1024 * 1024;
    this.victim_maxCacheSize = 2000L * this.victim_segmentSize;
    this.victim_minActiveRatio = 0.5;
    this.victim_scavDumpBelowRatio = 0.5;
    this.victim_scavengerInterval = 10;
    this.victim_epClz = FIFOEvictionPolicy.class;
    this.victim_rsClz = LRCRecyclingSelector.class;
    this.victim_acClz = BaseAdmissionController.class;
    
  }
  

}
