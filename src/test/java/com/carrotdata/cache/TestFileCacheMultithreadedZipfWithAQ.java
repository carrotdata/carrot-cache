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
    this.offheap = false;
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
