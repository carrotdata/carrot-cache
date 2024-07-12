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
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotdata.cache.controllers.LRCRecyclingSelector;
import com.carrotdata.cache.controllers.LeastAccessedRecyclingSelector;
import com.carrotdata.cache.controllers.MRCRecyclingSelector;
import com.carrotdata.cache.controllers.MinAliveRecyclingSelector;
import com.carrotdata.cache.eviction.LRUEvictionPolicy;
import com.carrotdata.cache.eviction.SLRUEvictionPolicy;

public class TestMemoryCacheMultithreadedZipf extends TestCacheMultithreadedZipfBase {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestMemoryCacheMultithreadedZipf.class);

  /**
   * Eviction tested: No eviction, LRU, S-LRU, FIFO; Recycling selectors: LAR (least-accessed),
   * MinAlive, LRC (oldest), MRC (youngest)
   */
  @BeforeClass
  public static void start() {
    //System.setProperty("MALLOC_DEBUG", "true");
  }
  @Before
  public void setUp() {
    this.memory = true;
    this.numRecords = 1000000;
    this.numIterations = 2 * this.numRecords;
    this.numThreads = 4;
    this.maxCacheSize = this.numThreads * 125 * this.segmentSize;
    this.scavNumberThreads = 2;
    this.scavDumpBelowRatio = 0.2;
  }

  @Test
  public void testNoEvictionBytesAPI() throws IOException {
    LOG.info("Bytes API: eviction=none, selector=null");
    this.evictionDisabled = true;
    this.scavengerInterval = 100000; // disable scavenger
    super.testContinuosLoadBytesRun();
  }

  @Test
  public void testNoEvictionMemoryAPI() throws IOException {
    LOG.info("Memory API: eviction=none, selector=null");
    this.evictionDisabled = true;
    this.scavengerInterval = 100000; // disable scavenger
    super.testContinuosLoadMemoryRun();
  }

  @Test
  public void testLRUEvictionAndLRCSelectorBytesAPI() throws IOException {
    LOG.info("Bytes API: eviction=LRU, selector=LRC");
    this.evictionDisabled = false;
    this.scavengerInterval = 2; // scavenger interval in sec
    this.epClz = LRUEvictionPolicy.class;
    this.rsClz = LRCRecyclingSelector.class;
    // this.scavDumpBelowRatio = 1.0;
    super.testContinuosLoadBytesRun();
  }

  @Test
  public void testLRUEvictionAndLRCSelectorMemoryAPI() throws IOException {
    LOG.info("Memory API: eviction=LRU, selector=LRC");

    this.evictionDisabled = false;
    this.scavengerInterval = 2; // scavenger interval in sec
    this.epClz = LRUEvictionPolicy.class;
    this.rsClz = LRCRecyclingSelector.class;
    // this.scavDumpBelowRatio = 1.0;
    super.testContinuosLoadMemoryRun();
  }

  @Ignore 
  @Test
  public void testLRUEvictionAndMRCSelectorBytesAPI() throws IOException {
    LOG.info("Bytes API: eviction=LRU, selector=MRC");
    this.evictionDisabled = false;
    this.scavengerInterval = 2; // scavenger interval in sec
    this.epClz = LRUEvictionPolicy.class;
    this.rsClz = MRCRecyclingSelector.class;
    // this.scavDumpBelowRatio = 1.0;
    super.testContinuosLoadBytesRun();
  }

  @Ignore
  @Test
  public void testLRUEvictionAndMRCSelectorMemoryAPI() throws IOException {
    LOG.info("Memory API: eviction=LRU, selector=MRC");

    this.evictionDisabled = false;
    this.scavengerInterval = 2; // scavenger interval in sec
    this.epClz = LRUEvictionPolicy.class;
    this.rsClz = MRCRecyclingSelector.class;
    // this.scavDumpBelowRatio = 1.0;
    super.testContinuosLoadMemoryRun();
  }

  @Test
  public void testLRUEvictionAndMinAliveSelectorBytesAPI() throws IOException {
    LOG.info("Bytes API: eviction=LRU, selector=MinAlive");
    this.evictionDisabled = false;
    this.scavengerInterval = 2; // scavenger interval in sec
    this.epClz = LRUEvictionPolicy.class;
    this.rsClz = MinAliveRecyclingSelector.class;
    // this.scavDumpBelowRatio = 1.0;
    super.testContinuosLoadBytesRun();
  }

  @Ignore
  //FIXME: this test is SLOW
  @Test
  public void testLRUEvictionAndMinAliveSelectorMemoryAPI() throws IOException {
    LOG.info("Memory API: eviction=LRU, selector=MinAlive");

    this.evictionDisabled = false;
    this.scavengerInterval = 2; // scavenger interval in sec
    this.epClz = LRUEvictionPolicy.class;
    this.rsClz = MinAliveRecyclingSelector.class;
    // this.scavDumpBelowRatio = 1.0;
    super.testContinuosLoadMemoryRun();
  }

  @Ignore
  @Test
  public void testLRUEvictionAndLeastAccessedSelectorBytesAPI() throws IOException {
    LOG.info("Bytes API: eviction=LRU, selector=LeastAccessed");
    this.evictionDisabled = false;
    this.scavengerInterval = 2; // scavenger interval in sec
    this.epClz = LRUEvictionPolicy.class;
    this.rsClz = LeastAccessedRecyclingSelector.class;
    // this.scavDumpBelowRatio = 1.0;
    super.testContinuosLoadBytesRun();
  }

  @Ignore
  @Test
  public void testLRUEvictionAndLeastAccessedSelectorMemoryAPI() throws IOException {
    LOG.info("Memory API: eviction=LRU, selector=LeastAccessed");

    this.evictionDisabled = false;
    this.scavengerInterval = 2; // scavenger interval in sec
    this.epClz = LRUEvictionPolicy.class;
    this.rsClz = LeastAccessedRecyclingSelector.class;
    // this.scavDumpBelowRatio = 1.0;
    super.testContinuosLoadMemoryRun();
  }

  @Test
  public void testSLRUEvictionAndLRCSelectorBytesAPI() throws IOException {
    LOG.info("Bytes API: eviction=SLRU, selector=LRC");
    this.evictionDisabled = false;
    this.scavengerInterval = 2; // scavenger interval in sec
    this.epClz = SLRUEvictionPolicy.class;
    this.rsClz = LRCRecyclingSelector.class;
    // this.scavDumpBelowRatio = 1.0;
    super.testContinuosLoadBytesRun();
  }

  @Test
  public void testSLRUEvictionAndLRCSelectorMemoryAPI() throws IOException {
    LOG.info("Memory API: eviction=SLRU, selector=LRC");

    this.evictionDisabled = false;
    this.scavengerInterval = 2; // scavenger interval in sec
    this.epClz = SLRUEvictionPolicy.class;
    this.rsClz = LRCRecyclingSelector.class;
    // this.scavDumpBelowRatio = 1.0;
    super.testContinuosLoadMemoryRun();
  }

  @Ignore
  @Test
  public void testSLRUEvictionAndMRCSelectorBytesAPI() throws IOException {
    LOG.info("Bytes API: eviction=SLRU, selector=MRC");
    this.evictionDisabled = false;
    this.scavengerInterval = 2; // scavenger interval in sec
    this.epClz = SLRUEvictionPolicy.class;
    this.rsClz = MRCRecyclingSelector.class;
    // this.scavDumpBelowRatio = 1.0;
    super.testContinuosLoadBytesRun();
  }

  @Ignore
  @Test
  public void testSLRUEvictionAndMRCSelectorMemoryAPI() throws IOException {
    LOG.info("Memory API: eviction=SLRU, selector=MRC");

    this.evictionDisabled = false;
    this.scavengerInterval = 2; // scavenger interval in sec
    this.epClz = SLRUEvictionPolicy.class;
    this.rsClz = MRCRecyclingSelector.class;
    // this.scavDumpBelowRatio = 1.0;
    super.testContinuosLoadMemoryRun();
  }

  @Test
  public void testSLRUEvictionAndMinAliveSelectorBytesAPI() throws IOException {
    LOG.info("Bytes API: eviction=SLRU, selector=MinAlive");
    this.evictionDisabled = false;
    this.scavengerInterval = 2; // scavenger interval in sec
    this.epClz = SLRUEvictionPolicy.class;
    this.rsClz = MinAliveRecyclingSelector.class;
    // this.scavDumpBelowRatio = 1.0;
    super.testContinuosLoadBytesRun();
  }

  @Ignore
  @Test
  public void runLoop() throws IOException {
    for (int i = 0; i < 100; i++) {
      LOG.info("Run #" + (i + 1));
      if (i > 0) {
        setUp();
      }
      testSLRUEvictionAndMinAliveSelectorMemoryAPI();
      if (i < 99) {
        tearDown();
      }
    }
  }

  @Test
  public void testSLRUEvictionAndMinAliveSelectorMemoryAPI() throws IOException {
    LOG.info("Memory API: eviction=SLRU, selector=MinAlive");

    this.evictionDisabled = false;
    this.scavengerInterval = 200; // scavenger interval in sec
    this.epClz = SLRUEvictionPolicy.class;
    this.rsClz = MinAliveRecyclingSelector.class;
    // this.scavDumpBelowRatio = 1.0;
    super.testContinuosLoadMemoryRun();
  }

  @Ignore
  @Test
  public void testSLRUEvictionAndLeastAccessedSelectorBytesAPI() throws IOException {
    LOG.info("Bytes API: eviction=SLRU, selector=LeastAccessed");
    this.evictionDisabled = false;
    this.scavengerInterval = 2; // scavenger interval in sec
    this.epClz = SLRUEvictionPolicy.class;
    this.rsClz = LeastAccessedRecyclingSelector.class;
    // this.scavDumpBelowRatio = 1.0;
    super.testContinuosLoadBytesRun();
  }

  @Ignore
  @Test
  public void testSLRUEvictionAndLeastAccessedSelectorMemoryAPI() throws IOException {
    LOG.info("Memory API: eviction=SLRU, selector=LeastAccessed");

    this.evictionDisabled = false;
    this.scavengerInterval = 2; // scavenger interval in sec
    this.epClz = SLRUEvictionPolicy.class;
    this.rsClz = LeastAccessedRecyclingSelector.class;
    // this.scavDumpBelowRatio = 1.0;
    super.testContinuosLoadMemoryRun();
  }
}
