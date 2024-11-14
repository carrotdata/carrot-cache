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

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotdata.cache.controllers.AQBasedAdmissionController;
import com.carrotdata.cache.controllers.LRCRecyclingSelector;
import com.carrotdata.cache.controllers.MinAliveRecyclingSelector;
import com.carrotdata.cache.eviction.LRUEvictionPolicy;
import com.carrotdata.cache.eviction.SLRUEvictionPolicy;
import com.carrotdata.cache.io.FileIOEngine;
import com.carrotdata.cache.io.IOEngine;

public class TestFileCacheMultithreadedZipfWithAQ extends TestCacheMultithreadedZipfBase {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestFileCacheMultithreadedZipfWithAQ.class);

  protected double startSizeRatio = 0.2;

  @Before
  public void setUp() {
    this.memory = false;
    this.numRecords = 1000000;
    this.numIterations = 2 * this.numRecords;
    this.numThreads = 2;
    this.segmentSize = 4 * 1024 * 1024;
    this.scavNumberThreads = 1;
    this.scavDumpBelowRatio = 1.0;

    this.maxCacheSize = 125 * this.numThreads * this.segmentSize;
  }

  @Override
  protected Builder withAddedConfigurations(Builder b) {
    b = b.withAdmissionQueueStartSizeRatio(startSizeRatio);
    return b;
  }

  @Test
  public void testSLRUEvictionAndLRCSelectorMemoryAPIWithAQ() throws IOException {
    LOG.info("Bytes API: eviction=SLRU, selector=LRC - AQ");
    this.evictionDisabled = false;
    this.scavengerInterval = 2; // scavenger interval in sec
    this.epClz = SLRUEvictionPolicy.class;
    this.rsClz = LRCRecyclingSelector.class;
    this.acClz = AQBasedAdmissionController.class;
    super.testContinuosLoadMemoryRun();
  }

  @Test
  public void testSLRUEvictionAndLRCSelectorMemoryAPI() throws IOException {
    LOG.info("Bytes API: eviction=SLRU, selector=LRC");
    this.evictionDisabled = false;
    this.scavengerInterval = 2; // scavenger interval in sec
    this.epClz = SLRUEvictionPolicy.class;
    this.rsClz = LRCRecyclingSelector.class;
    super.testContinuosLoadMemoryRun();
  }
}
