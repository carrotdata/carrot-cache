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
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotdata.cache.controllers.LRCRecyclingSelector;
import com.carrotdata.cache.controllers.MinAliveRecyclingSelector;
import com.carrotdata.cache.eviction.LRUEvictionPolicy;
import com.carrotdata.cache.eviction.SLRUEvictionPolicy;

public class TestMemoryObjectCacheMultithreadedZipf extends TestObjectCacheMultithreadedZipfBase {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestMemoryObjectCacheMultithreadedZipf.class);

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
    this.numThreads = 1;
    this.maxCacheSize = this.numThreads * 1250 * this.segmentSize;
    this.scavNumberThreads = 2;
    this.scavDumpBelowRatio = 0.2;
    this. maxOnHeapSize = 250_000;
  }

  @Test
  public void testNoEvictionBytesAPI() throws IOException {
    LOG.info("Bytes API: eviction=none, selector=null");
    this.evictionDisabled = true;
    this.scavengerInterval = 100000; // disable scavenger
    super.testContinuosLoadBytesRun();
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
  public void testLRUEvictionAndMinAliveSelectorBytesAPI() throws IOException {
    LOG.info("Bytes API: eviction=LRU, selector=MinAlive");
    this.evictionDisabled = false;
    this.scavengerInterval = 2; // scavenger interval in sec
    this.epClz = LRUEvictionPolicy.class;
    this.rsClz = MinAliveRecyclingSelector.class;
    // this.scavDumpBelowRatio = 1.0;
    super.testContinuosLoadBytesRun();
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
  public void testSLRUEvictionAndMinAliveSelectorBytesAPI() throws IOException {
    LOG.info("Bytes API: eviction=SLRU, selector=MinAlive");
    this.evictionDisabled = false;
    this.scavengerInterval = 2; // scavenger interval in sec
    this.epClz = SLRUEvictionPolicy.class;
    this.rsClz = MinAliveRecyclingSelector.class;
    // this.scavDumpBelowRatio = 1.0;
    super.testContinuosLoadBytesRun();
  }

}
