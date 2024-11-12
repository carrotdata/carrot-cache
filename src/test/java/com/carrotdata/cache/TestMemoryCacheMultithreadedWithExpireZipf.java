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
import java.util.Arrays;
import java.util.Collection;
import java.util.Random;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotdata.cache.controllers.AQBasedExpirationAwareAdmissionController;
import com.carrotdata.cache.controllers.AdmissionController;
import com.carrotdata.cache.controllers.BaseAdmissionController;
import com.carrotdata.cache.controllers.ExpirationAwareAdmissionController;
import com.carrotdata.cache.controllers.LRCRecyclingSelector;
import com.carrotdata.cache.controllers.MinAliveRecyclingSelector;
import com.carrotdata.cache.eviction.LRUEvictionPolicy;
import com.carrotdata.cache.eviction.SLRUEvictionPolicy;
import com.carrotdata.cache.index.CompactBaseWithExpireIndexFormat;

@RunWith(Parameterized.class)
public class TestMemoryCacheMultithreadedWithExpireZipf extends TestCacheMultithreadedZipfBase {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestMemoryCacheMultithreadedWithExpireZipf.class);
  @Parameters
  public static Collection<Object[]> data() {
      return Arrays.asList(new Object[][] {     
               /*{ AQBasedExpirationAwareAdmissionController.class, true }, */
               { AQBasedExpirationAwareAdmissionController.class, false },/**/
               /*{ BaseAdmissionController.class, true }/*, */
               // Below work worse for some reasons.  
               { ExpirationAwareAdmissionController.class, true }, //*, 
               /*{ ExpirationAwareAdmissionController.class, false }  /**/
         });
  }
  
  protected int binStartValue = 1;

  protected double binMultiplier = 2.0;

  protected int numberOfBins = 10; // number of ranks

  protected int[] expArray = new int[] {2, 2, 2, 2, 10, 10, 10, 10, 100, 100, 100, 500, 500, 500, 1000, 1000, 1000, 1000, 1000 };

  protected Random r;
  
  protected double acMaxRatio = 0.35;

  public TestMemoryCacheMultithreadedWithExpireZipf(Class<? extends AdmissionController> ac, Boolean memory) {
    this.acClz = ac;
    this.memory = memory;
  }
  
  @Before
  public void setUp() {
    this.segmentSize = 2 * 1024 * 1024;
    this.numThreads = 4;
    this.maxCacheSize = this.numThreads * 200 * this.segmentSize;
    this.binStartValue = 1;
    this.binMultiplier = 2.0;
    this.numberOfBins = 10;
    this.numRecords = 1000000;
    this.numIterations = 2 * this.numRecords;
    this.scavDumpBelowRatio = 0.2;
    this.minActiveRatio = 0.0;
    r = new Random();
  }

  @Override
  protected Builder withAddedConfigurations(Builder b) {
    b = b.withExpireStartBinValue(binStartValue).withExpireBinMultiplier(binMultiplier)
        .withNumberOfPopularityRanks(numberOfBins)
        .withMainQueueIndexFormat(CompactBaseWithExpireIndexFormat.class.getName())
        .withAdmissionQueueStartSizeRatio(acMaxRatio)
        .withSLRUInsertionPoint(7);
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
  
  @Test
  public void testLRUEvictionAndMinAliveSelectorMemoryAPI() throws IOException {
    LOG.info("Memory API: eviction=LRU, selector=MinAlive");
    this.evictionDisabled = false;
    this.scavengerInterval = 2; // scavenger interval in sec
    this.epClz = LRUEvictionPolicy.class;
    this.rsClz = MinAliveRecyclingSelector.class;
    super.testContinuosLoadMemoryRun();
  }

  //@Ignore
  @Test
  public void testSLRUEvictionAndMinAliveSelectorMemoryAPI() throws IOException {
    LOG.info("Memory API: eviction=SLRU, selector=MinAlive");
    this.evictionDisabled = false;
    this.scavengerInterval = 2; // scavenger interval in sec
    this.epClz = SLRUEvictionPolicy.class;
    this.rsClz = MinAliveRecyclingSelector.class;
    super.testContinuosLoadMemoryRun();
  }

  @Test
  public void testLRUEvictionAndLRCSelectorMemoryAPI() throws IOException {
    LOG.info("Memory API: eviction=LRU, selector=LRC");
    this.evictionDisabled = false;
    this.scavengerInterval = 2; // scavenger interval in sec
    this.epClz = LRUEvictionPolicy.class;
    this.rsClz = LRCRecyclingSelector.class;
    super.testContinuosLoadMemoryRun();
  }

  @Test
  public void testSLRUEvictionAndLRCSelectorMemoryAPI() throws IOException {
    LOG.info("Memory API: eviction=SLRU, selector=LRC");
    this.evictionDisabled = false;
    this.scavengerInterval = 2; // scavenger interval in sec
    this.epClz = SLRUEvictionPolicy.class;
    this.rsClz = LRCRecyclingSelector.class;
    super.testContinuosLoadMemoryRun();
  }
}
