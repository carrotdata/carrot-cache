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

  protected boolean memory = true;

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

  protected Cache createCache() throws IOException {
    String cacheName = "cache";
    // Data directory
    Path path = Files.createTempDirectory(null);
    File dir = path.toFile();
    dir.deleteOnExit();
    String rootDir = dir.getAbsolutePath();

    Builder builder = new Builder(cacheName);

    builder.withCacheDataSegmentSize(segmentSize).withCacheMaximumSize(maxCacheSize)
        .withScavengerRunInterval(scavengerInterval)
        .withScavengerDumpEntryBelowMin(scavDumpBelowRatio)
        // .withCacheEvictionPolicy(LRUEvictionPolicy.class.getName())
        .withRecyclingSelector(MinAliveRecyclingSelector.class.getName())
        // .withDataWriter(BlockDataWriter.class.getName())
        // .withMemoryDataReader(BlockMemoryDataReader.class.getName())
        // .withFileDataReader(BlockFileDataReader.class.getName())
        // .withMainQueueIndexFormat(CompactWithExpireIndexFormat.class.getName())
        .withCacheRootDir(rootDir).withMinimumActiveDatasetRatio(minActiveRatio)
        .withEvictionDisabledMode(evictionDisabled).withTLSSupported(false);

    if (memory) {
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
