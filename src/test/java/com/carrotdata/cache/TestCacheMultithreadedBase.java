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

package com.carrotdata.cache;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.After;

import com.carrotdata.cache.controllers.MinAliveRecyclingSelector;
import com.carrotdata.cache.io.TestIOMultithreadedBase;

public abstract class TestCacheMultithreadedBase extends TestIOMultithreadedBase {

  protected Cache cache;
  
  protected boolean offheap = true;
  
  protected boolean evictionDisabled = false;
  
  protected int segmentSize = 4 * 1024 * 1024;
  
  protected long maxCacheSize = 100L * segmentSize;
  
  int scavengerInterval = 10000; // seconds - disable for test
    
  double scavDumpBelowRatio = 0.5;
  
  double minActiveRatio = 0.90;

 
  @After  
  public void tearDown() {
    // UnsafeAccess.mallocStats.printStats(false);
    this.cache.dispose();
  }
  
  protected  Cache createCache() throws IOException{
    String cacheName = "cache";
    // Data directory
    Path path = Files.createTempDirectory(null);
    File  dir = path.toFile();
    dir.deleteOnExit();
    String rootDir = dir.getAbsolutePath();
    
    Builder builder = new Builder(cacheName);
    
    builder
      .withCacheDataSegmentSize(segmentSize)
      .withCacheMaximumSize(maxCacheSize)
      .withScavengerRunInterval(scavengerInterval)
      .withScavengerDumpEntryBelowMin(scavDumpBelowRatio)
      //.withCacheEvictionPolicy(LRUEvictionPolicy.class.getName())
      .withRecyclingSelector(MinAliveRecyclingSelector.class.getName())
      //.withDataWriter(BlockDataWriter.class.getName())
      //.withMemoryDataReader(BlockMemoryDataReader.class.getName())
      //.withFileDataReader(BlockFileDataReader.class.getName())
      //.withMainQueueIndexFormat(CompactWithExpireIndexFormat.class.getName())
      .withCacheRootDir(rootDir)
      .withMinimumActiveDatasetRatio(minActiveRatio)
      .withEvictionDisabledMode(evictionDisabled)
      .withTLSSupported(false);
    
    if (offheap) {
      return builder.buildMemoryCache();
    } else {
      return builder.buildDiskCache();
    }
  }

  @Override
  protected boolean put(byte[] key, byte[] value, long expire) throws IOException {
    return this.cache.put(key, value, expire);
  }

  @Override
  protected boolean put(long keyPtr, int keySize, long valuePtr, int valueSize, long expire)
      throws IOException {
    return this.cache.put(keyPtr, keySize, valuePtr, valueSize, expire);
  }

  @Override
  protected boolean delete(byte[] key, int off, int len) throws IOException {
    return this.cache.delete(key, off, len);
  }

  @Override
  protected boolean delete(long keyPtr, int keySize) throws IOException {
    return this.cache.delete(keyPtr, keySize);
  }

  @Override
  protected long get(byte[] key, int off, int len, byte[] buffer, int bufferOfset)
      throws IOException {
    return this.cache.getKeyValue(key, off, len, true, buffer, bufferOfset);
  }

  @Override
  protected long get(long keyPtr, int keySize, ByteBuffer buffer) throws IOException {
    return this.cache.getKeyValue(keyPtr, keySize, true, buffer);
  }
  
  
}
