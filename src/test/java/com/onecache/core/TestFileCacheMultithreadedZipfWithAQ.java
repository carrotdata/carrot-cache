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

import org.junit.Before;
import org.junit.Test;

import com.onecache.core.Builder;
import com.onecache.core.controllers.AQBasedAdmissionController;
import com.onecache.core.controllers.MinAliveRecyclingSelector;
import com.onecache.core.eviction.LRUEvictionPolicy;

public class TestFileCacheMultithreadedZipfWithAQ extends TestCacheMultithreadedZipfBase {
  
  protected double startSizeRatio = 0.5;
  
  @Before
  public void setUp() {
    this.offheap = false;
    this.numRecords = 1000000;
    this.numIterations = 10 * this.numRecords;
    this.numThreads = 4;
    this.segmentSize = 16 * 1024 * 1024;
    this.maxCacheSize = 50 * this.segmentSize;
  }
  
  @Override
  protected Builder withAddedConfigurations(Builder b) {
     b = b.withAdmissionQueueStartSizeRatio(startSizeRatio);
     return b;
  }
  
  //@Ignore
  @Test
  public void testLRUEvictionAndMinAliveSelectorBytesAPIWithAQ() throws IOException {
    System.out.println("Bytes API: eviction=LRU, selector=MinAlive - AQ");
    this.evictionDisabled = false;
    this.scavengerInterval = 2; // scavenger interval in sec
    this.epClz = LRUEvictionPolicy.class;
    this.rsClz = MinAliveRecyclingSelector.class;
    this.acClz = AQBasedAdmissionController.class;
    super.testContinuosLoadBytesRun();
  }
  
  //@Ignore
  @Test
  public void testLRUEvictionAndMinAliveSelectorBytesAPI() throws IOException {
    System.out.println("Bytes API: eviction=LRU, selector=MinAlive");
    this.evictionDisabled = false;
    this.scavengerInterval = 2; // scavenger interval in sec
    this.epClz = LRUEvictionPolicy.class;
    this.rsClz = MinAliveRecyclingSelector.class;
    super.testContinuosLoadBytesRun();
  }
}
