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
    this.memory = false;
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
