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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;

import org.junit.Before;
import org.junit.Test;

import com.carrot.cache.controllers.MinAliveRecyclingSelector;
import com.carrot.cache.index.CompactWithExpireIndexFormat;
import com.carrot.cache.io.BlockDataWriter;
import com.carrot.cache.io.BlockFileDataReader;
import com.carrot.cache.io.BlockMemoryDataReader;
import com.carrot.cache.io.IOTestBase;

public abstract class TestCacheBase extends IOTestBase {
  
  boolean offheap = true;
  Cache cache;
  int segmentSize = 4 * 1024 * 1024;
  long maxCacheSize = 100 * segmentSize;
  int scavengerInterval = 10000; // seconds - disable for test
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
      .withMinimumActiveDatasetRatio(minActiveRatio);
    
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
  
  @Test
  public void testAllExpiredBytes() throws IOException {
    System.out.println("Test all expired bytes");
    Scavenger.clear();
    // Create cache
    this.cache = createCache();
    this.expireTime = 1000; 
    prepareData(300000);
    int loaded = loadBytesCache(cache);
    System.out.println("loaded=" + loaded);
    // Wait expireTime
    try {
      Thread.sleep(expireTime);
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
    
  }
  
  @Test
  public void testAllExpiredMemory() throws IOException {
    System.out.println("Test all expired memory");
    Scavenger.clear();
    // Create cache
    this.cache = createCache();
    this.expireTime = 1000; 
    prepareData(300000);
    int loaded = loadMemoryCache(cache);
    System.out.println("loaded=" + loaded);
    // Wait expireTime
    try {
      Thread.sleep(expireTime);
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
    verifyMemoryCacheNot(cache, loaded);
    
    allocated = cache.getStorageAllocated();
    used = cache.getStorageUsed();
    size = cache.size();
    activeSize = cache.activeSize();
    System.out.println(String.format("After scan: allocated=%d, used=%d, size=%d, active=%d", 
      allocated, used, size, activeSize));
    
  }
  
  @Test
  public void testNoExpiredBytes() throws IOException {
    System.out.println("Test no expired - bytes");
    Scavenger.clear();
    // Create cache
    this.cache = createCache();
    
    this.expireTime = 1000000; 
    prepareData(300000);
    // Fill cache completely (no eviction is enabled)
    int loaded = loadBytesCache(cache);
    System.out.println("loaded=" + loaded);
  
    long allocated = cache.getStorageAllocated();
    long used = cache.getStorageUsed();
    long size = cache.size();
    long activeSize = cache.activeSize();
    System.out.println(String.format("Before scan: allocated=%d, used=%d, size=%d, active=%d", 
      allocated, used, size, activeSize));
    // Access all objects to remove them from memory index
    // and from data segments statistics
    verifyBytesCache(cache, loaded);
    verifyBytesCacheByteBuffer(cache, loaded);
    
    allocated = cache.getStorageAllocated();
    used = cache.getStorageUsed();
    size = cache.size();
    activeSize = cache.activeSize();
    System.out.println(String.format("After scan: allocated=%d, used=%d, size=%d, active=%d", 
      allocated, used, size, activeSize));
    
  }
  
  @Test
  public void testNoExpiredMemory() throws IOException {
    System.out.println("Test no expired - memory");
    Scavenger.clear();
    // Create cache
    this.cache = createCache();
    
    this.expireTime = 1000000; 
    prepareData(300000);
    // Fill cache completely (no eviction is enabled)
    int loaded = loadMemoryCache(cache);
    System.out.println("loaded=" + loaded);
  
    long allocated = cache.getStorageAllocated();
    long used = cache.getStorageUsed();
    long size = cache.size();
    long activeSize = cache.activeSize();
    System.out.println(String.format("Before scan: allocated=%d, used=%d, size=%d, active=%d", 
      allocated, used, size, activeSize));
    // Access all objects to remove them from memory index
    // and from data segments statistics
    verifyMemoryCache(cache, loaded);
    
    allocated = cache.getStorageAllocated();
    used = cache.getStorageUsed();
    size = cache.size();
    activeSize = cache.activeSize();
    System.out.println(String.format("After scan: allocated=%d, used=%d, size=%d, active=%d", 
      allocated, used, size, activeSize));
    
  }
  
  @Test
  public void testNoExpiredWithDeletesBytes() throws IOException {
    System.out.println("Test no expired with deletes bytes");
    // Clear scavenger's statics
    Scavenger.clear();
    // Create cache
    this.scavDumpBelowRatio = 0; // remove only deleted
    this.cache = createCache();
    
    this.expireTime = 1000000; 
    prepareData(300000);
    // Fill cache completely (no eviction is enabled)
    int loaded = loadBytesCache(cache);
    System.out.println("loaded=" + loaded);
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

  }
  
  @Test
  public void testNoExpiredWithDeletesMemory() throws IOException {
    System.out.println("Test no expired with deletes memory");
    // Clear scavenger's statics
    Scavenger.clear();
    // Create cache
    this.scavDumpBelowRatio = 0; // remove only deleted
    this.cache = createCache();
    
    this.expireTime = 1000000; 
    prepareData(300000);
    // Fill cache completely (no eviction is enabled)
    int loaded = loadMemoryCache(cache);
    System.out.println("loaded=" + loaded);
    int deleted = deleteMemoryCache(cache, loaded / 17);
  
    long allocated = cache.getStorageAllocated();
    long used = cache.getStorageUsed();
    long size = cache.size();
    long activeSize = cache.activeSize();
    System.out.println(String.format("Before scan: allocated=%d, used=%d, size=%d, active=%d", 
      allocated, used, size, activeSize));
    // Access all objects to remove them from memory index
    // and from data segments statistics
    verifyMemoryCacheWithDeletes(cache, loaded, deleted);
    
    allocated = cache.getStorageAllocated();
    used = cache.getStorageUsed();
    size = cache.size();
    activeSize = cache.activeSize();
    System.out.println(String.format("After scan: allocated=%d, used=%d, size=%d, active=%d", 
      allocated, used, size, activeSize));
  }
  
  @Test
  public void testSaveLoad() throws IOException {
    System.out.println("Test save load");
    Scavenger.clear();
    // Create cache
    this.cache = createCache();
    
    this.expireTime = 1000000; 
    prepareData(300000);
    // Fill cache completely (no eviction is enabled)
    int loaded = loadBytesCache(cache);
    System.out.println("loaded=" + loaded);
    
    String cacheName = cache.getName();
    cache.save();
    cache.getEngine().dispose();
    
    cache = new Cache();
    cache.setName(cacheName);
    cache.load();
    
    verifyBytesCache(cache, loaded);
    
  }
}
