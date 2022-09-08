package com.carrot.cache;

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

import com.carrot.cache.controllers.LRCRecyclingSelector;
import com.carrot.cache.controllers.LeastAccessedRecyclingSelector;
import com.carrot.cache.controllers.MRCRecyclingSelector;
import com.carrot.cache.controllers.MinAliveRecyclingSelector;
import com.carrot.cache.eviction.LRUEvictionPolicy;
import com.carrot.cache.eviction.SLRUEvictionPolicy;

public class TestOffheapCacheMultithreadedZipf extends TestCacheMultithreadedZipfBase {
  
  /**
   * Eviction tested:
   * 
   * No eviction, LRU, S-LRU, FIFO;
   * 
   * Recycling selectors:
   * LAR (least-accessed), MinAlive, LRC (oldest), MRC (youngest) 
   */
  
  @Before
  public void setUp() {
    this.offheap = true;
  }
  
  @Test
  public void testNoEvictionBytesAPI() throws IOException {
    System.out.println("Bytes API: eviction=none, selector=null");

    this.numRecords = 1000000;
    this.evictionDisabled = true;
    this.scavengerInterval = 100000; // disable scavenger
    this.numThreads = 4;
    super.testContinuosLoadBytesRun();
  }
  
  @Test
  public void testNoEvictionMemoryAPI() throws IOException {
    System.out.println("Memory API: eviction=none, selector=null");

    this.numRecords = 1000000;
    this.evictionDisabled = true;
    this.scavengerInterval = 100000; // disable scavenger
    this.numThreads = 4;
    super.testContinuosLoadMemoryRun();
  }
  
  @Test
  public void testLRUEvictionAndLRCSelectorBytesAPI() throws IOException {
    System.out.println("Bytes API: eviction=LRU, selector=LRC");
    this.numRecords = 1000000;
    this.evictionDisabled = false;
    this.scavengerInterval = 2; // scavenger interval in sec
    this.epClz = LRUEvictionPolicy.class;
    this.rsClz = LRCRecyclingSelector.class;
    this.scavDumpBelowRatio = 1.0;
    this.numThreads = 4;
    super.testContinuosLoadBytesRun();
  }
  
  @Test
  public void testLRUEvictionAndLRCSelectorMemoryAPI() throws IOException {
    System.out.println("Memory API: eviction=LRU, selector=LRC");

    this.numRecords = 1000000;
    this.evictionDisabled = false;
    this.scavengerInterval = 2; // scavenger interval in sec
    this.epClz = LRUEvictionPolicy.class;
    this.rsClz = LRCRecyclingSelector.class;
    this.scavDumpBelowRatio = 1.0;
    this.numThreads = 4;
    super.testContinuosLoadMemoryRun();
  }
  
  @Test
  public void testLRUEvictionAndMRCSelectorBytesAPI() throws IOException {
    System.out.println("Bytes API: eviction=LRU, selector=MRC");
    this.numRecords = 1000000;
    this.evictionDisabled = false;
    this.scavengerInterval = 2; // scavenger interval in sec
    this.epClz = LRUEvictionPolicy.class;
    this.rsClz = MRCRecyclingSelector.class;
    this.scavDumpBelowRatio = 1.0;
    this.numThreads = 4;
    super.testContinuosLoadBytesRun();
  }
  
  @Test
  public void testLRUEvictionAndMRCSelectorMemoryAPI() throws IOException {
    System.out.println("Memory API: eviction=LRU, selector=MRC");

    this.numRecords = 1000000;
    this.evictionDisabled = false;
    this.scavengerInterval = 2; // scavenger interval in sec
    this.epClz = LRUEvictionPolicy.class;
    this.rsClz = MRCRecyclingSelector.class;
    this.scavDumpBelowRatio = 1.0;
    this.numThreads = 4;
    super.testContinuosLoadMemoryRun();
  }
  
  @Test
  public void testLRUEvictionAndMinAliveSelectorBytesAPI() throws IOException {
    System.out.println("Bytes API: eviction=LRU, selector=MinAlive");
    this.numRecords = 1000000;
    this.evictionDisabled = false;
    this.scavengerInterval = 2; // scavenger interval in sec
    this.epClz = LRUEvictionPolicy.class;
    this.rsClz = MinAliveRecyclingSelector.class;
    this.scavDumpBelowRatio = 1.0;
    this.numThreads = 4;
    super.testContinuosLoadBytesRun();
  }
  
  @Test
  public void testLRUEvictionAndMinAliveSelectorMemoryAPI() throws IOException {
    System.out.println("Memory API: eviction=LRU, selector=MinAlive");

    this.numRecords = 1000000;
    this.evictionDisabled = false;
    this.scavengerInterval = 2; // scavenger interval in sec
    this.epClz = LRUEvictionPolicy.class;
    this.rsClz = MinAliveRecyclingSelector.class;
    this.scavDumpBelowRatio = 1.0;
    this.numThreads = 4;
    super.testContinuosLoadMemoryRun();
  }
  
  @Test
  public void testLRUEvictionAndLeastAccessedSelectorBytesAPI() throws IOException {
    System.out.println("Bytes API: eviction=LRU, selector=LeastAccessed");
    this.numRecords = 1000000;
    this.evictionDisabled = false;
    this.scavengerInterval = 2; // scavenger interval in sec
    this.epClz = LRUEvictionPolicy.class;
    this.rsClz = LeastAccessedRecyclingSelector.class;
    this.scavDumpBelowRatio = 1.0;
    this.numThreads = 4;
    super.testContinuosLoadBytesRun();
  }
  
  @Test
  public void testLRUEvictionAndLeastAccessedSelectorMemoryAPI() throws IOException {
    System.out.println("Memory API: eviction=LRU, selector=LeastAccessed");

    this.numRecords = 1000000;
    this.evictionDisabled = false;
    this.scavengerInterval = 2; // scavenger interval in sec
    this.epClz = LRUEvictionPolicy.class;
    this.rsClz = LeastAccessedRecyclingSelector.class;
    this.scavDumpBelowRatio = 1.0;
    this.numThreads = 4;
    super.testContinuosLoadMemoryRun();
  }
  
  @Test
  public void testSLRUEvictionAndLRCSelectorBytesAPI() throws IOException {
    System.out.println("Bytes API: eviction=SLRU, selector=LRC");
    this.numRecords = 1000000;
    this.evictionDisabled = false;
    this.scavengerInterval = 2; // scavenger interval in sec
    this.epClz = SLRUEvictionPolicy.class;
    this.rsClz = LRCRecyclingSelector.class;
    this.scavDumpBelowRatio = 1.0;
    this.numThreads = 4;
    super.testContinuosLoadBytesRun();
  }
  
  @Test
  public void testSLRUEvictionAndLRCSelectorMemoryAPI() throws IOException {
    System.out.println("Memory API: eviction=SLRU, selector=LRC");

    this.numRecords = 1000000;
    this.evictionDisabled = false;
    this.scavengerInterval = 2; // scavenger interval in sec
    this.epClz = SLRUEvictionPolicy.class;
    this.rsClz = LRCRecyclingSelector.class;
    this.scavDumpBelowRatio = 1.0;
    this.numThreads = 4;
    super.testContinuosLoadMemoryRun();
  }
  
  @Test
  public void testSLRUEvictionAndMRCSelectorBytesAPI() throws IOException {
    System.out.println("Bytes API: eviction=SLRU, selector=MRC");
    this.numRecords = 1000000;
    this.evictionDisabled = false;
    this.scavengerInterval = 2; // scavenger interval in sec
    this.epClz = SLRUEvictionPolicy.class;
    this.rsClz = MRCRecyclingSelector.class;
    this.scavDumpBelowRatio = 1.0;
    this.numThreads = 4;
    super.testContinuosLoadBytesRun();
  }
  
  @Test
  public void testSLRUEvictionAndMRCSelectorMemoryAPI() throws IOException {
    System.out.println("Memory API: eviction=SLRU, selector=MRC");

    this.numRecords = 1000000;
    this.evictionDisabled = false;
    this.scavengerInterval = 2; // scavenger interval in sec
    this.epClz = SLRUEvictionPolicy.class;
    this.rsClz = MRCRecyclingSelector.class;
    this.scavDumpBelowRatio = 1.0;
    this.numThreads = 4;
    super.testContinuosLoadMemoryRun();
  }
  
  @Test
  public void testSLRUEvictionAndMinAliveSelectorBytesAPI() throws IOException {
    System.out.println("Bytes API: eviction=SLRU, selector=MinAlive");
    this.numRecords = 1000000;
    this.evictionDisabled = false;
    this.scavengerInterval = 2; // scavenger interval in sec
    this.epClz = SLRUEvictionPolicy.class;
    this.rsClz = MinAliveRecyclingSelector.class;
    this.scavDumpBelowRatio = 1.0;
    this.numThreads = 4;
    super.testContinuosLoadBytesRun();
  }
  
  @Test
  public void testSLRUEvictionAndMinAliveSelectorMemoryAPI() throws IOException {
    System.out.println("Memory API: eviction=SLRU, selector=MinAlive");

    this.numRecords = 1000000;
    this.evictionDisabled = false;
    this.scavengerInterval = 2; // scavenger interval in sec
    this.epClz = SLRUEvictionPolicy.class;
    this.rsClz = MinAliveRecyclingSelector.class;
    this.scavDumpBelowRatio = 1.0;
    this.numThreads = 4;
    super.testContinuosLoadMemoryRun();
  }
  
  @Test
  public void testSLRUEvictionAndLeastAccessedSelectorBytesAPI() throws IOException {
    System.out.println("Bytes API: eviction=SLRU, selector=LeastAccessed");
    this.numRecords = 1000000;
    this.evictionDisabled = false;
    this.scavengerInterval = 2; // scavenger interval in sec
    this.epClz = SLRUEvictionPolicy.class;
    this.rsClz = LeastAccessedRecyclingSelector.class;
    this.scavDumpBelowRatio = 1.0;
    this.numThreads = 4;
    super.testContinuosLoadBytesRun();
  }
  
  @Test
  public void testSLRUEvictionAndLeastAccessedSelectorMemoryAPI() throws IOException {
    System.out.println("Memory API: eviction=SLRU, selector=LeastAccessed");

    this.numRecords = 1000000;
    this.evictionDisabled = false;
    this.scavengerInterval = 2; // scavenger interval in sec
    this.epClz = SLRUEvictionPolicy.class;
    this.rsClz = LeastAccessedRecyclingSelector.class;
    this.scavDumpBelowRatio = 1.0;
    this.numThreads = 4;
    super.testContinuosLoadMemoryRun();
  }
}
