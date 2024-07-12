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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotdata.cache.compression.CodecFactory;
import com.carrotdata.cache.compression.CompressionCodec;
import com.carrotdata.cache.compression.zstd.ZstdCompressionCodec;
import com.carrotdata.cache.index.CompactBaseWithExpireIndexFormat;
import com.carrotdata.cache.io.CompressedBlockDataWriter;
import com.carrotdata.cache.io.CompressedBlockFileDataReader;
import com.carrotdata.cache.io.CompressedBlockMemoryDataReader;
import com.carrotdata.cache.util.CacheConfig;

public abstract class TestCacheWithCompressionBase extends TestCacheBase {
  private static final Logger LOG = LoggerFactory.getLogger(TestCacheWithCompressionBase.class);

  boolean randomData = false;
  boolean dictionaryEnabled = true;
  boolean asyncTrainingMode = false;
  static String cacheName1 = "cache1";
  static String cacheName2 = "cache2";

  @Before
  public void setUp() throws IOException {
    super.setUp();
    this.numRecords = 150000;
    // We need expiration support
    this.indexFormat = CompactBaseWithExpireIndexFormat.class.getName();
  }

  @After
  public void tearDown() throws IOException {
    super.tearDown();
    cleanDictionaries();
    ZstdCompressionCodec.reset();
    CodecFactory.getInstance().clear();
  }

  @Override
  protected Cache createCache() throws IOException {
    return createCache(cacheName);
  }

  @Override
  protected Cache createCache(String cacheName) throws IOException {
    initTest(cacheName);
    // Data directory
    Path path = Files.createTempDirectory(null);
    File dir = path.toFile();
    dir.deleteOnExit();
    String rootDir = dir.getAbsolutePath();

    Builder builder = new Builder(cacheName);

    builder.withCacheDataSegmentSize(segmentSize).withCacheMaximumSize(maxCacheSize)
        .withScavengerRunInterval(scavengerInterval)
        .withScavengerDumpEntryBelowMin(scavDumpBelowRatio).withRecyclingSelector(recycleSelector)
        .withDataWriter(dataWriter).withMemoryDataReader(dataReaderMemory)
        .withFileDataReader(dataReaderFile).withMainQueueIndexFormat(indexFormat)
        .withCacheRootDir(rootDir).withMinimumActiveDatasetRatio(minActiveRatio)
        .withCacheStreamingSupportBufferSize(1 << 19).withEvictionDisabledMode(true);
    if (maxKeyValueSize > 0) {
      builder.withMaximumKeyValueSize(maxKeyValueSize);
    }
    if (memory) {
      return builder.buildMemoryCache();
    } else {
      return builder.buildDiskCache();
    }
  }

  protected void initTest(String cacheName) throws IOException {
    this.dataWriter = CompressedBlockDataWriter.class.getName();
    this.dataReaderMemory = CompressedBlockMemoryDataReader.class.getName();
    this.dataReaderFile = CompressedBlockFileDataReader.class.getName();
    CacheConfig config = CacheConfig.getInstance();
    config.setCacheCompressionDictionaryEnabled(cacheName, dictionaryEnabled);
    config.setCacheCompressionEnabled(cacheName, true);
    config.setCacheTLSSupported(cacheName, true);
    if (dictionaryEnabled) {
      config.setCacheCompressionDictionaryTrainingAsync(cacheName, asyncTrainingMode);
    }
    // Clean up dictionaries
    String dictDir = config.getCacheDictionaryDir(cacheName);
    File dir = new File(dictDir);
    if (dir.exists()) {
      File[] files = dir.listFiles();
      Arrays.stream(files).forEach(x -> x.delete());
    }

    initCodec(cacheName);
  }

  protected void cleanDictionaries() {
    cleanDictionaries(cacheName);
    cleanDictionaries(cacheName1);
    cleanDictionaries(cacheName2);
  }

  protected void cleanDictionaries(String cacheName) {
    // Clean up dictionaries
    CacheConfig config = CacheConfig.getInstance();
    String dictDir = config.getCacheDictionaryDir(cacheName);
    File dir = new File(dictDir);
    if (dir.exists()) {
      File[] files = dir.listFiles();
      Arrays.stream(files).forEach(x -> x.delete());
    }
  }

  protected void initCodec(String cacheName) throws IOException {
    CodecFactory factory = CodecFactory.getInstance();
    factory.clear();
    CompressionCodec codec = factory.getCompressionCodecForCache(cacheName);
    if (codec == null) {
      factory.initCompressionCodecForCache(cacheName, null);
    }
  }

  @Override
  protected void prepareData() {
    /* DEBUG */ LOG.info("Test dictionary=" + dictionaryEnabled + " random data=" + randomData);
    if (randomData) {
      prepareRandomData(numRecords);
    } else {
      try {
        prepareGithubData(numRecords);
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

}
