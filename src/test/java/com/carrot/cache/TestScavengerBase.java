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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.carrot.cache.controllers.MinAliveRecyclingSelector;
import com.carrot.cache.index.CompactWithExpireIndexFormat;
import com.carrot.cache.io.BlockDataWriter;
import com.carrot.cache.io.BlockFileDataReader;
import com.carrot.cache.io.BlockMemoryDataReader;
import com.carrot.cache.io.IOTestBase;
import com.carrot.cache.util.UnsafeAccess;

public abstract class TestScavengerBase extends IOTestBase {
  
  boolean offheap = true;
  Cache cache;
  int segmentSize = 4 * 1024 * 1024;
  long maxCacheSize = 100 * segmentSize;
  int scavengerInterval = 10000; // seconds - disable it for test
  long expireTime;
  double scavDumpBelowRatio = 0.5;
  double minActiveRatio = 0.90;
      
  @Before
  public void setUp() throws IOException {
    this.r = new Random();
    long seed = System.currentTimeMillis();
    System.out.println("r.seed=" + seed);
    r.setSeed(seed);
  }
  
  @After
  public void tearDown() {
    super.tearDown();
    cache.dispose();
    UnsafeAccess.mallocStats.printStats();
  }
  
  protected Cache createCache() throws IOException {
    String cacheName = "cache";
    // Data directory
    Path path = Files.createTempDirectory(null);
    File  dir = path.toFile();
    dir.deleteOnExit();
    String dataDir = dir.getAbsolutePath();
    
    path = Files.createTempDirectory(null);
    dir = path.toFile();
    dir.deleteOnExit();
    String snapshotDir = dir.getAbsolutePath();
    
    Cache.Builder builder = new Cache.Builder(cacheName);
    
    builder
      .withCacheDataSegmentSize(segmentSize)
      .withCacheMaximumSize(maxCacheSize)
      .withScavengerRunInterval(scavengerInterval)
      .withScavengerDumpEntryBelowStart(scavDumpBelowRatio)
      //.withCacheEvictionPolicy(LRUEvictionPolicy.class.getName())
      .withRecyclingSelector(MinAliveRecyclingSelector.class.getName())
      .withDataWriter(BlockDataWriter.class.getName())
      .withMemoryDataReader(BlockMemoryDataReader.class.getName())
      .withFileDataReader(BlockFileDataReader.class.getName())
      .withMainQueueIndexFormat(CompactWithExpireIndexFormat.class.getName())
      .withSnapshotDir(snapshotDir)
      .withDataDir(dataDir)
      .withMinimumActiveDatasetRatio(minActiveRatio)
      .withEvictionDisabledMode(true);
    
    if (offheap) {
      return builder.buildMemoryCache();
    } else {
      return builder.buildDiskCache();
    }
  }
  
  @Override
  protected long getExpire(int n) {
    return System.currentTimeMillis() + expireTime;
  }
  
  //@Ignore
  @Test
  public void testAllExpired() throws IOException {
    System.out.println("Test all expired");
    Scavenger.clear();

    // Create cache
    this.cache = createCache();
    
    this.expireTime = 1000; 
    prepareData(300000);
    int loaded = loadBytesCache(cache);
    System.out.println("loaded=" + loaded + " cache size="+ cache.size() + 
      " index size=" + cache.getEngine().getMemoryIndex().size());
    // Wait expireTime
    try {
      Thread.sleep(2 * expireTime);
    } catch(InterruptedException e) {
      
    }
    long allocated = cache.getStorageAllocated();
    long used = cache.getStorageUsed();
    long size = cache.size();
    long activeSize = cache.activeSize();
    System.out.println(String.format("Before scan: allocated=%d, used=%d, size=%d, active=%d", 
      allocated, used, size, activeSize));
    // Access all objects to remove them from memory index
    // and from data segments statistics
    verifyBytesCacheNot(cache, loaded);
    
    allocated = cache.getStorageAllocated();
    used = cache.getStorageUsed();
    size = cache.size();
    activeSize = cache.activeSize();
    System.out.println(String.format("After scan: allocated=%d, used=%d, size=%d, active=%d", 
      allocated, used, size, activeSize));
    
    Scavenger scavenger = new Scavenger(cache);
    scavenger.run();
    
    allocated = cache.getStorageAllocated();
    used = cache.getStorageUsed();
    size = cache.size();
    activeSize = cache.activeSize();
    System.out.println(String.format("After Scav run: allocated=%d, used=%d, size=%d, active=%d", 
      allocated, used, size, activeSize));
    // After Scavenger run we still have active segment, which is not sealed yet
    assertEquals(0, (int) allocated);
    assertEquals(0, (int) used);
    assertEquals(0, activeSize);
  }
  
  //@Ignore
  @Test
  public void testAllExpiredNoScan() throws IOException {
    System.out.println("Test all expired - no scan");
    Scavenger.clear();

    // Create cache
    this.cache = createCache();
    
    this.expireTime = 1000; 
    prepareData(300000);
    int loaded = loadBytesCache(cache);
    System.out.println("loaded=" + loaded + " cache size="+ cache.size() + 
      " index size=" + cache.getEngine().getMemoryIndex().size());
    // Wait expireTime
    try {
      Thread.sleep(expireTime);
    } catch(InterruptedException e) {
      
    }
    long allocated = cache.getStorageAllocated();
    long used = cache.getStorageUsed();
    long size = cache.size();
    long activeSize = cache.activeSize();
    System.out.println(String.format(" allocated=%d, used=%d, size=%d, active=%d", 
      allocated, used, size, activeSize));
    // Access all objects to remove them from memory index
    // and from data segments statistics
    
    Scavenger scavenger = new Scavenger(cache);
    scavenger.run();
    
    allocated = cache.getStorageAllocated();
    used = cache.getStorageUsed();
    size = cache.size();
    activeSize = cache.activeSize();
    System.out.println(String.format("After Scav run: allocated=%d, used=%d, size=%d, active=%d", 
      allocated, used, size, activeSize));
    // After Scavenger run we still have active segment, which is not sealed yet
    assertEquals(0, (int) allocated);
    assertEquals(0, (int) used);
    assertEquals(0, activeSize);
  }
  
  //@Ignore
  @Test
  public void testNoExpiredRegularRun() throws IOException {
    System.out.println("Test no expired - regular run");
    Scavenger.clear();
    // Create cache
    this.cache = createCache();
    
    this.expireTime = 1000000; 
    prepareData(300000);
    // Fill cache completely (no eviction is enabled)
    int loaded = loadBytesCache(cache);
    System.out.println("loaded=" + loaded + " cache size="+ cache.size() + 
      " index size=" + cache.getEngine().getMemoryIndex().size());  
    long allocated = cache.getStorageAllocated();
    long used = cache.getStorageUsed();
    long size = cache.size();
    long activeSize = cache.activeSize();
    System.out.println(String.format("Before scan: allocated=%d, used=%d, size=%d, active=%d", 
      allocated, used, size, activeSize));
    // Access all objects to remove them from memory index
    // and from data segments statistics
    verifyBytesCache(cache, loaded);
    
    allocated = cache.getStorageAllocated();
    used = cache.getStorageUsed();
    size = cache.size();
    activeSize = cache.activeSize();
    System.out.println(String.format("After scan: allocated=%d, used=%d, size=%d, active=%d", 
      allocated, used, size, activeSize));
    
    long oldAllocated = allocated;
    long oldUsed = used;
    long oldSize = size;
    long oldActiveSize = activeSize;
    
    Scavenger scavenger = new Scavenger(cache);
    scavenger.run();
    
    allocated = cache.getStorageAllocated();
    used = cache.getStorageUsed();
    size = cache.size();
    activeSize = cache.activeSize();
    System.out.println(String.format("After Scav run: allocated=%d, used=%d, size=%d, active=%d", 
      allocated, used, size, activeSize));
    // After Scavenger run we still have active segment, which is not sealed yet
    double r1 = (double)(oldAllocated - allocated) / oldAllocated;
    double r2 = (double)(oldUsed - used) / oldUsed;
    double r3 = (double)(oldSize - size) / oldSize;
    double r4 = (double) (oldActiveSize - activeSize) / oldActiveSize;
    
    System.out.printf("r1 =%f r2=%f r3=%f r4=%f\n", r1, r2, r3, r4);
    assertTrue(r1 < 0.125);
    assertTrue(r2 < 0.125);
    assertTrue(r3 < 0.125);
    assertTrue(r4 < 0.125);
    
  }
  
  //@Ignore
  @Test
  public void testNoExpiredWithDeletesRegularRun() throws IOException {
    System.out.println("Test no expired with deletes - regular run");
    // Clear scavenger's statics
    Scavenger.clear();
    // Create cache
    this.scavDumpBelowRatio = 0; // remove only deleted
    this.cache = createCache();
    
    this.expireTime = 1000000; 
    prepareData(300000);
    // Fill cache completely (no eviction is enabled)
    int loaded = loadBytesCache(cache);
    System.out.println("loaded=" + loaded + " cache size="+ cache.size() + 
      " index size=" + cache.getEngine().getMemoryIndex().size());
    
    int deleted = deleteBytesCache(cache, loaded / 17);
  
    long allocated = cache.getStorageAllocated();
    long used = cache.getStorageUsed();
    long size = cache.size();
    long activeSize = cache.activeSize();
    System.out.println(String.format("Before scan: allocated=%d, used=%d, size=%d, active=%d", 
      allocated, used, size, activeSize));
    // Access all objects to remove them from memory index
    // and from data segments statistics
    verifyBytesCacheWithDeletes(cache, loaded, deleted);
    
    allocated = cache.getStorageAllocated();
    used = cache.getStorageUsed();
    size = cache.size();
    activeSize = cache.activeSize();
    System.out.println(String.format("After scan: allocated=%d, used=%d, size=%d, active=%d", 
      allocated, used, size, activeSize));
    
    long oldAllocated = allocated;
    long oldUsed = used;
    long oldSize = size;
    long oldActiveSize = activeSize;
    
    Scavenger scavenger = new Scavenger(cache);
    scavenger.run();
    
    allocated = cache.getStorageAllocated();
    used = cache.getStorageUsed();
    size = cache.size();
    activeSize = cache.activeSize();
    System.out.println(String.format("After Scav run: allocated=%d, used=%d, size=%d, active=%d", 
      allocated, used, size, activeSize));
    // After Scavenger run we still have active segment, which is not sealed yet
    double r1 = (double)(oldAllocated - allocated) / oldAllocated;
    double r2 = (double)(oldUsed - used) / oldUsed;
    double r3 = (double)(oldSize - size) / oldSize;
    double r4 = (double) (oldActiveSize - activeSize) / oldActiveSize;
    
    System.out.printf("r1 =%f r2=%f r3=%f r4=%f\n", r1, r2, r3, r4);
    // // we delete 6% of data
    assertTrue(r1 < 0.07); 
    assertTrue(r2 < 0.07);
    assertTrue(r3 < 0.07);
    assertTrue(r4 == 0);
    verifyBytesCacheWithDeletes(cache, loaded, deleted);

  }
  
  //@Ignore
  @Test
  public void testNoExpiredWithDeletesRegularRunMinActive() throws IOException {
    System.out.println("Test no expired with deletes - regular run (min active)");
    // Clear scavenger's statics
    Scavenger.clear();
    // Create cache
    this.minActiveRatio = 0.97;
    this.cache = createCache();
    
    this.expireTime = 1000000; 
    prepareData(30000);
    // Fill cache completely (no eviction is enabled)
    int loaded = loadBytesCache(cache);
    System.out.println("loaded=" + loaded + " cache size="+ cache.size() + 
      " index size=" + cache.getEngine().getMemoryIndex().size());
    
    int deleted = deleteBytesCache(cache, loaded / 7);
  
    long allocated = cache.getStorageAllocated();
    long used = cache.getStorageUsed();
    long size = cache.size();
    long activeSize = cache.activeSize();
    System.out.println(String.format("Before scan: allocated=%d, used=%d, size=%d, active=%d", 
      allocated, used, size, activeSize));
    // Access all objects to remove them from memory index
    // and from data segments statistics
    verifyBytesCacheWithDeletes(cache, loaded, deleted);
    
    allocated = cache.getStorageAllocated();
    used = cache.getStorageUsed();
    size = cache.size();
    activeSize = cache.activeSize();
    System.out.println(String.format("After scan: allocated=%d, used=%d, size=%d, active=%d", 
      allocated, used, size, activeSize));
    
    long oldAllocated = allocated;
    long oldUsed = used;
    long oldSize = size;
    long oldActiveSize = activeSize;
    
    Scavenger scavenger = new Scavenger(cache);
    scavenger.run();
    
    allocated = cache.getStorageAllocated();
    used = cache.getStorageUsed();
    size = cache.size();
    activeSize = cache.activeSize();
    System.out.println(String.format("After Scav run: allocated=%d, used=%d, size=%d, active=%d", 
      allocated, used, size, activeSize));
    // After Scavenger run we still have active segment, which is not sealed yet
    double r1 = (double)(oldAllocated - allocated) / oldAllocated;
    double r2 = (double)(oldUsed - used) / oldUsed;
    double r3 = (double)(oldSize - size) / oldSize;
    double r4 = (double) (oldActiveSize - activeSize) / oldActiveSize;
    double r5 = (double) activeSize / size;
    System.out.printf("r1 =%f r2=%f r3=%f r4=%f r5=%f\n", r1, r2, r3, r4, r5);
    // // we delete 6% of data
    assertTrue(r5 > minActiveRatio);
    verifyBytesCacheWithDeletes(cache, loaded, deleted);

  }
  
}
