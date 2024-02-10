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
import java.net.URISyntaxException;

import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.carrot.cache.controllers.LRCRecyclingSelector;
import com.carrot.cache.controllers.LeastAccessedRecyclingSelector;
import com.carrot.cache.controllers.MRCRecyclingSelector;
import com.carrot.cache.controllers.MinAliveRecyclingSelector;
import com.carrot.cache.eviction.LRUEvictionPolicy;
import com.carrot.cache.eviction.SLRUEvictionPolicy;

public class TestOffheapCompressedCacheMultithreadedZipf extends TestCompressedCacheMultithreadedZipfBase {
  
  /**
   * Eviction tested:
   * 
   * No eviction, LRU, S-LRU, FIFO;
   * 
   * Recycling selectors:
   * LAR (least-accessed), MinAlive, LRC (oldest), MRC (youngest) 
   * @throws URISyntaxException 
   * @throws IOException 
   */
  
  @BeforeClass
  public static void enableMallocDebug() {
    //UnsafeAccess.setMallocDebugEnabled(true);
  }
  
  @Before
  public void setUp() throws IOException, URISyntaxException {
    super.setUp();
    this.offheap = true;
    this.numRecords = 1000000;
    this.numIterations = this.numRecords;
    this.numThreads = 4;
    // 
    this.scavDumpBelowRatio = 1.0;
    // We reduced cache size by 10x to account for compression (which is 10-11x)
    this.maxCacheSize = 100 * this.segmentSize;
    this.scavNumberThreads = 2;
  }
  
  @Test
  public void testNoEvictionBytesAPI() throws IOException {
    System.out.println("Bytes API: eviction=none, selector=null");
    this.evictionDisabled = true;
    this.scavengerInterval = 100000; // disable scavenger
    //this.numRecords = 4000000;
    this.maxCacheSize = 100 * this.segmentSize;

    super.testContinuosLoadBytesRun();
  }
  
  @Test
  public void testNoEvictionMemoryAPI() throws IOException {
    System.out.println("Memory API: eviction=none, selector=null");
    this.evictionDisabled = true;
    this.scavengerInterval = 100000; // disable scavenger
   // this.numRecords = 1000000;
    this.maxCacheSize = 100 * this.segmentSize;
    super.testContinuosLoadMemoryRun();
  }
  
  @Test
  public void testLRUEvictionAndLRCSelectorBytesAPI() throws IOException {
    System.out.println("Bytes API: eviction=LRU, selector=LRC");
    this.evictionDisabled = false;
    this.scavengerInterval = 2; // scavenger interval in sec
    this.epClz = LRUEvictionPolicy.class;
    this.rsClz = LRCRecyclingSelector.class;
    //this.scavDumpBelowRatio = 1.0;
    super.testContinuosLoadBytesRun();
  }
  
  @Test
  public void testLRUEvictionAndLRCSelectorMemoryAPI() throws IOException {
    System.out.println("Memory API: eviction=LRU, selector=LRC");

    this.evictionDisabled = false;
    this.scavengerInterval = 2; // scavenger interval in sec
    this.epClz = LRUEvictionPolicy.class;
    this.rsClz = LRCRecyclingSelector.class;
    //this.scavDumpBelowRatio = 1.0;
    super.testContinuosLoadMemoryRun();
  }
  
  @Ignore
  @Test
  public void testLRUEvictionAndMRCSelectorBytesAPI() throws IOException {
    System.out.println("Bytes API: eviction=LRU, selector=MRC");
    this.evictionDisabled = false;
    this.scavengerInterval = 2; // scavenger interval in sec
    this.epClz = LRUEvictionPolicy.class;
    this.rsClz = MRCRecyclingSelector.class;
    //this.scavDumpBelowRatio = 1.0;
    super.testContinuosLoadBytesRun();
  }
  
  @Ignore
  @Test
  public void testLRUEvictionAndMRCSelectorMemoryAPI() throws IOException {
    System.out.println("Memory API: eviction=LRU, selector=MRC");

    this.evictionDisabled = false;
    this.scavengerInterval = 2; // scavenger interval in sec
    this.epClz = LRUEvictionPolicy.class;
    this.rsClz = MRCRecyclingSelector.class;
    //this.scavDumpBelowRatio = 1.0;
    super.testContinuosLoadMemoryRun();
  }
  
  @Test
  public void testLRUEvictionAndMinAliveSelectorBytesAPI() throws IOException {
    System.out.println("Bytes API: eviction=LRU, selector=MinAlive");
    this.evictionDisabled = false;
    this.scavengerInterval = 2; // scavenger interval in sec
    this.epClz = LRUEvictionPolicy.class;
    this.rsClz = MinAliveRecyclingSelector.class;
    //this.scavDumpBelowRatio = 1.0;
    super.testContinuosLoadBytesRun();
  }
  
  @Test
  public void testLRUEvictionAndMinAliveSelectorMemoryAPI() throws IOException {
    System.out.println("Memory API: eviction=LRU, selector=MinAlive");

    this.evictionDisabled = false;
    this.scavengerInterval = 2; // scavenger interval in sec
    this.epClz = LRUEvictionPolicy.class;
    this.rsClz = MinAliveRecyclingSelector.class;
    //this.scavDumpBelowRatio = 1.0;
    super.testContinuosLoadMemoryRun();
  }
  
  @Ignore
  @Test
  public void testLRUEvictionAndLeastAccessedSelectorBytesAPI() throws IOException {
    System.out.println("Bytes API: eviction=LRU, selector=LeastAccessed");
    this.evictionDisabled = false;
    this.scavengerInterval = 2; // scavenger interval in sec
    this.epClz = LRUEvictionPolicy.class;
    this.rsClz = LeastAccessedRecyclingSelector.class;
    //this.scavDumpBelowRatio = 1.0;
    super.testContinuosLoadBytesRun();
  }
  
  @Ignore
  @Test
  public void testLRUEvictionAndLeastAccessedSelectorMemoryAPI() throws IOException {
    System.out.println("Memory API: eviction=LRU, selector=LeastAccessed");

    this.evictionDisabled = false;
    this.scavengerInterval = 2; // scavenger interval in sec
    this.epClz = LRUEvictionPolicy.class;
    this.rsClz = LeastAccessedRecyclingSelector.class;
    //this.scavDumpBelowRatio = 1.0;
    super.testContinuosLoadMemoryRun();
  }
  
  @Test
  public void testSLRUEvictionAndLRCSelectorBytesAPI() throws IOException {
    System.out.println("Bytes API: eviction=SLRU, selector=LRC");
    this.evictionDisabled = false;
    this.scavengerInterval = 2; // scavenger interval in sec
    this.epClz = SLRUEvictionPolicy.class;
    this.rsClz = LRCRecyclingSelector.class;
    //this.scavDumpBelowRatio = 1.0;
    super.testContinuosLoadBytesRun();
  }
  
  @Test
  public void testSLRUEvictionAndLRCSelectorMemoryAPI() throws IOException {
    System.out.println("Memory API: eviction=SLRU, selector=LRC");

    this.evictionDisabled = false;
    this.scavengerInterval = 2; // scavenger interval in sec
    this.epClz = SLRUEvictionPolicy.class;
    this.rsClz = LRCRecyclingSelector.class;
    //this.scavDumpBelowRatio = 1.0;
    super.testContinuosLoadMemoryRun();
  }
  
  
  @Test
  public void testSLRUEvictionAndMinAliveSelectorBytesAPI() throws IOException {
    System.out.println("Bytes API: eviction=SLRU, selector=MinAlive");
    this.evictionDisabled = false;
    this.scavengerInterval = 2; // scavenger interval in sec
    this.epClz = SLRUEvictionPolicy.class;
    this.rsClz = MinAliveRecyclingSelector.class;
    //this.scavDumpBelowRatio = 1.0;
    super.testContinuosLoadBytesRun();
  }
  
  @Ignore
  @Test 
  public void runLoop() throws IOException, URISyntaxException {
    for (int i = 0; i < 1000; i++) {
      System.out.println("\nRun #" + (i+1));
      if (i > 0) {
        setUp();
      }
      //testSLRUEvictionAndMinAliveSelectorMemoryAPI();
      testLRUEvictionAndMinAliveSelectorMemoryAPI();
      if (i < 999) {
        tearDown();
      }
    }
  }
  
  @Test
  public void testSLRUEvictionAndMinAliveSelectorMemoryAPI() throws IOException {
    System.out.println("Memory API: eviction=SLRU, selector=MinAlive");

    this.evictionDisabled = false;
    this.scavengerInterval = 2; // scavenger interval in sec
    this.epClz = SLRUEvictionPolicy.class;
    this.rsClz = MinAliveRecyclingSelector.class;
    //this.scavDumpBelowRatio = 1.0;
    super.testContinuosLoadMemoryRun();
  }
  
  @Ignore
  @Test
  public void testSLRUEvictionAndLeastAccessedSelectorBytesAPI() throws IOException {
    System.out.println("Bytes API: eviction=SLRU, selector=LeastAccessed");
    this.evictionDisabled = false;
    this.scavengerInterval = 2; // scavenger interval in sec
    this.epClz = SLRUEvictionPolicy.class;
    this.rsClz = LeastAccessedRecyclingSelector.class;
    //this.scavDumpBelowRatio = 1.0;
    super.testContinuosLoadBytesRun();
  }
  
  @Ignore
  @Test
  public void testSLRUEvictionAndLeastAccessedSelectorMemoryAPI() throws IOException {
    System.out.println("Memory API: eviction=SLRU, selector=LeastAccessed");

    this.evictionDisabled = false;
    this.scavengerInterval = 2; // scavenger interval in sec
    this.epClz = SLRUEvictionPolicy.class;
    this.rsClz = LeastAccessedRecyclingSelector.class;
    //this.scavDumpBelowRatio = 1.0;
    super.testContinuosLoadMemoryRun();
  }
}
