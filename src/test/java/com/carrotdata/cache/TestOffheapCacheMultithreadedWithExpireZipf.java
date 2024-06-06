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

import com.carrotdata.cache.controllers.AQBasedExpirationAwareAdmissionController;
import com.carrotdata.cache.controllers.LRCRecyclingSelector;
import com.carrotdata.cache.controllers.MinAliveRecyclingSelector;
import com.carrotdata.cache.eviction.LRUEvictionPolicy;
import com.carrotdata.cache.eviction.SLRUEvictionPolicy;
import com.carrotdata.cache.index.CompactBaseWithExpireIndexFormat;

public class TestOffheapCacheMultithreadedWithExpireZipf extends TestCacheMultithreadedZipfBase {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestOffheapCacheMultithreadedWithExpireZipf.class);

  protected int binStartValue = 1;

  protected double binMultiplier = 2.0;

  protected int numberOfBins = 10; // number of ranks

  protected int[] expArray = new int[] { 2, 2, 2, 2, 500, 500, 500, 1000, 1000, 1000, 1000, 1000 };

  Random r;

  @Before
  public void setUp() {
    this.offheap = true;
    this.segmentSize = 4 * 1024 * 1024;
    this.maxCacheSize = 100 * this.segmentSize;
    this.binStartValue = 1;
    this.binMultiplier = 2.0;
    this.numberOfBins = 10;
    this.numThreads = 1;
    this.numRecords = 1000000;
    this.numIterations = 10 * this.numRecords;
    this.scavDumpBelowRatio = 0.2;
    this.minActiveRatio = 0.0;
    this.acClz = AQBasedExpirationAwareAdmissionController.class;
    r = new Random();
  }

  @Override
  protected Builder withAddedConfigurations(Builder b) {
    b = b.withExpireStartBinValue(binStartValue).withExpireBinMultiplier(binMultiplier)
        .withNumberOfPopularityRanks(numberOfBins)
        .withMainQueueIndexFormat(CompactBaseWithExpireIndexFormat.class.getName());
    return b;
  }

  protected long getExpire(int n) {
    int index = r.nextInt(expArray.length);
    return System.currentTimeMillis() + 1000L * expArray[index];
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

  @Test
  public void testSLRUEvictionAndMinAliveSelectorBytesAPI() throws IOException {
    LOG.info("Bytes API: eviction=SLRU, selector=MinAlive");
    this.evictionDisabled = false;
    this.scavengerInterval = 2; // scavenger interval in sec
    this.epClz = SLRUEvictionPolicy.class;
    this.rsClz = MinAliveRecyclingSelector.class;
    super.testContinuosLoadBytesRun();
  }

  @Test
  public void testLRUEvictionAndLRCSelectorBytesAPI() throws IOException {
    LOG.info("Bytes API: eviction=LRU, selector=LRC");
    this.evictionDisabled = false;
    this.scavengerInterval = 2; // scavenger interval in sec
    this.epClz = LRUEvictionPolicy.class;
    this.rsClz = LRCRecyclingSelector.class;
    super.testContinuosLoadBytesRun();
  }

  @Test
  public void testSLRUEvictionAndLRCSelectorBytesAPI() throws IOException {
    LOG.info("Bytes API: eviction=SLRU, selector=LRC");
    this.evictionDisabled = false;
    this.scavengerInterval = 2; // scavenger interval in sec
    this.epClz = SLRUEvictionPolicy.class;
    this.rsClz = LRCRecyclingSelector.class;
    super.testContinuosLoadBytesRun();
  }
}
