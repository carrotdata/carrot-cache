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
package com.carrotdata.cache;

import java.io.IOException;

import org.junit.Test;

import com.carrotdata.cache.controllers.AQBasedAdmissionController;
import com.carrotdata.cache.eviction.SLRUEvictionPolicy;

public class TestOffheapCacheMultithreadedZipfStress extends TestOffheapCacheMultithreadedZipf {
  
  @Override
  public void setUp() {
    this.offheap = true;
    this.numRecords = 10000000;
    this.numThreads = 4;
    this.segmentSize = 13 * 1024 * 1024;
    this.maxCacheSize = 1000 * this.segmentSize;
    this.scavNumberThreads = 1;
    this.scavengerInterval = 100000;
    this.numIterations = 10 * this.numRecords;
    this.acClz = AQBasedAdmissionController.class;
    this.aqStartRatio = 0.6;
    this.epClz = SLRUEvictionPolicy.class;
    this.slruInsertionPoint = 6;

  }
  
//  @Test
//  public void testSLRUEvictionAndPopularitySelectorBytesAPI() throws IOException {
//    LOG.info("Bytes API: eviction=SLRU, selector=Popularity");
//    this.evictionDisabled = false;
//    this.scavDumpBelowRatio = 0.5;
//    this.rsClz = PopularityBasedRecyclingSelector.class;
//
//    super.testContinuosLoadBytesRun();
//  }
  
  @Test
  public void testSLRUEvictionAndMinAliveSelectorBytesAPI() throws IOException {
    this.scavDumpBelowRatio = 0.5;

    super.testSLRUEvictionAndMinAliveSelectorBytesAPI();
  }
  
  @Test
  public void testNoEvictionBytesAPI() throws IOException {
    super.testNoEvictionBytesAPI();
  }
  
  @Test
  public void testLRUEvictionAndMRCSelectorBytesAPI() throws IOException {
    this.scavDumpBelowRatio = 0.5;
    this.spinOnWaitTime = 10000;
    super.testLRUEvictionAndMRCSelectorBytesAPI();
  }
}
