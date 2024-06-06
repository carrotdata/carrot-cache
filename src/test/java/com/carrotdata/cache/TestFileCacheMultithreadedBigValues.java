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
import java.util.Random;

import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotdata.cache.controllers.AQBasedAdmissionController;
import com.carrotdata.cache.controllers.MinAliveRecyclingSelector;
import com.carrotdata.cache.eviction.LRUEvictionPolicy;

public class TestFileCacheMultithreadedBigValues extends TestCacheMultithreadedZipfBase {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestFileCacheMultithreadedBigValues.class);

  protected double startSizeRatio = 0.3;

  @Before
  public void setUp() {
    this.offheap = false;
    this.numRecords = 10000;
    this.numIterations = 10 * this.numRecords;
    this.numThreads = 1;
    this.segmentSize = 64 * 1024 * 1024;
    this.maxCacheSize = 100 * this.segmentSize;
    this.maxValueSize = 1024 * 1024;
  }

  @Override
  protected int nextValueSize(Random r) {
    return this.maxValueSize;
  }

  @Override
  protected Builder withAddedConfigurations(Builder b) {
    b = b.withAdmissionQueueStartSizeRatio(startSizeRatio);
    return b;
  }

  @Test
  public void testLRUEvictionAndMinAliveSelectorBytesAPIWithAQ() throws IOException {
    LOG.info("Bytes API: eviction=LRU, selector=MinAlive - AQ");
    this.evictionDisabled = false;
    this.scavengerInterval = 2; // scavenger interval in sec
    this.epClz = LRUEvictionPolicy.class;
    this.rsClz = MinAliveRecyclingSelector.class;
    this.acClz = AQBasedAdmissionController.class;
    super.testContinuosLoadBytesRun();
  }

  @Test
  public void testLRUEvictionAndMinAliveSelectorBytesAPI() throws IOException {
    LOG.info("Bytes API: eviction=LRU, selector=MinAlive");
    this.evictionDisabled = false;
    this.scavengerInterval = 2; // scavenger interval in sec
    this.epClz = LRUEvictionPolicy.class;
    this.rsClz = MinAliveRecyclingSelector.class;
    super.testContinuosLoadBytesRun();
  }
}
