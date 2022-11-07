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
package com.carrot.cache;

import java.io.IOException;
import java.util.Random;

import org.junit.Before;
import org.junit.Test;

import com.carrot.cache.controllers.AQBasedExpirationAwareAdmissionController;
import com.carrot.cache.controllers.MinAliveRecyclingSelector;
import com.carrot.cache.eviction.LRUEvictionPolicy;
import com.carrot.cache.eviction.SLRUEvictionPolicy;
import com.carrot.cache.index.CompactBaseWithExpireIndexFormat;

public class TestOffheapCacheMultithreadedWithExpireZipf extends TestCacheMultithreadedZipfBase {
  
  protected int binStartValue = 1;
  
  protected double binMultiplier = 2.0;
  
  protected int numberOfBins = 10; // number of ranks
  
  protected int[] expArray = new int[] {2, 2, 2, 2, 500, 500, 500, 1000, 1000, 1000, 1000, 1000};
  
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
     b = b.withExpireStartBinValue(binStartValue)
         .withExpireBinMultiplier(binMultiplier)
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
    System.out.println("Bytes API: eviction=LRU, selector=MinAlive");
    this.evictionDisabled = false;
    this.scavengerInterval = 2; // scavenger interval in sec
    this.epClz = LRUEvictionPolicy.class;
    this.rsClz = MinAliveRecyclingSelector.class;
    super.testContinuosLoadBytesRun();
  }
  
  @Test
  public void testSLRUEvictionAndMinAliveSelectorBytesAPI() throws IOException {
    System.out.println("Bytes API: eviction=SLRU, selector=MinAlive");
    this.evictionDisabled = false;
    this.scavengerInterval = 2; // scavenger interval in sec
    this.epClz = SLRUEvictionPolicy.class;
    this.rsClz = MinAliveRecyclingSelector.class;
    super.testContinuosLoadBytesRun();
  }
}
