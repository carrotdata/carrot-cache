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

import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotdata.cache.controllers.MinAliveRecyclingSelector;
import com.carrotdata.cache.index.CompactBaseWithExpireIndexFormat;
import com.carrotdata.cache.io.BaseDataWriter;
import com.carrotdata.cache.io.BaseFileDataReader;
import com.carrotdata.cache.io.BaseMemoryDataReader;
import com.carrotdata.cache.io.MemoryBufferInputStream;
import com.carrotdata.cache.util.TestUtils;
import com.carrotdata.cache.util.Utils;

public abstract class TestCacheStreamAPIBase {
  private static final Logger LOG = LoggerFactory.getLogger(TestCacheStreamAPIBase.class);

  boolean memory = true;
  Cache cache;
  int segmentSize = 4 * 1024 * 1024;
  long maxCacheSize = 100L * segmentSize;
  int scavengerInterval = 10000; // seconds - disable for test
  long expireTime;
  double scavDumpBelowRatio = 0.5;
  double minActiveRatio = 0.90;

  @Before
  public void setUp() throws IOException {
  }

  @After
  public void tearDown() throws IOException {
    cache.dispose();
    TestUtils.deleteCacheFiles(cache);
  }

  protected Cache createCache(String cacheName) throws IOException {
    // Data directory
    Path path = Files.createTempDirectory(null);
    File dir = path.toFile();
    dir.deleteOnExit();
    String rootDir = dir.getAbsolutePath();

    Builder builder = new Builder(cacheName);

    builder.withCacheDataSegmentSize(segmentSize).withCacheMaximumSize(maxCacheSize)
        .withScavengerRunInterval(scavengerInterval)
        .withScavengerDumpEntryBelowMin(scavDumpBelowRatio)
        .withRecyclingSelector(MinAliveRecyclingSelector.class.getName())
        .withDataWriter(BaseDataWriter.class.getName())
        .withMemoryDataReader(BaseMemoryDataReader.class.getName())
        .withFileDataReader(BaseFileDataReader.class.getName())
        .withMainQueueIndexFormat(CompactBaseWithExpireIndexFormat.class.getName())
        .withCacheRootDir(rootDir).withMinimumActiveDatasetRatio(minActiveRatio)
        .withCacheStreamingSupportBufferSize(1 << 19).withEvictionDisabledMode(true);

    if (memory) {
      return builder.buildMemoryCache();
    } else {
      return builder.buildDiskCache();
    }
  }

  @Test
  public void testStreamAPI() throws IOException {
    LOG.info("Test stream API");
    Scavenger.clear();
    // Create cache
    this.cache = createCache("cache1");
    this.expireTime = 1000000;
    byte[] key = "stream_key".getBytes();

    long streamLength = 81 * (1 << 20) + 135;
    MemoryBufferInputStream source = new MemoryBufferInputStream(streamLength);
    OutputStream os =
        cache.getOutputStream(key, 0, key.length, System.currentTimeMillis() + expireTime);

    byte[] buffer = new byte[4096];
    int totalRead = 0;
    while (totalRead < streamLength) {
      int read = source.read(buffer);
      if (read < 0) {
        break;
      } else {
        os.write(buffer, 0, read);
      }
      totalRead += read;
    }

    os.close();
    source.reset();

    InputStream is = cache.getInputStream(key, 0, key.length);
    byte[] buf = new byte[4096];

    totalRead = 0;

    while (totalRead < streamLength) {
      int toRead = (int) Math.min(buffer.length, streamLength - totalRead);
      if (toRead < 4096) {
        LOG.info("toRead={}  ", toRead);
      }
      readFully(is, buf, toRead);
      readFully(source, buffer, toRead);
      assertTrue(Utils.compareTo(buffer, 0, toRead, buf, 0, toRead) == 0);
      totalRead += toRead;
    }
    is.close();
    source.close();
  }

  private void readFully(InputStream is, byte[] buf, int toRead) throws IOException {
    int totalRead = 0;
    while (totalRead < toRead) {
      int read = is.read(buf, totalRead, toRead - totalRead);
      totalRead += read;
    }
  }
}