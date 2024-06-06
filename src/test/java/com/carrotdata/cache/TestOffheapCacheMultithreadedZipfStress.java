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

  // @Test
  // public void testSLRUEvictionAndPopularitySelectorBytesAPI() throws IOException {
  // LOG.info("Bytes API: eviction=SLRU, selector=Popularity");
  // this.evictionDisabled = false;
  // this.scavDumpBelowRatio = 0.5;
  // this.rsClz = PopularityBasedRecyclingSelector.class;
  //
  // super.testContinuosLoadBytesRun();
  // }

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
