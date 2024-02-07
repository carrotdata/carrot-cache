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
import java.util.Arrays;

import org.junit.After;
import org.junit.Before;

import com.carrot.cache.compression.CodecFactory;
import com.carrot.cache.compression.CompressionCodec;
import com.carrot.cache.compression.zstd.ZstdCompressionCodec;
import com.carrot.cache.index.CompactBaseWithExpireIndexFormat;
import com.carrot.cache.io.CompressedBlockDataWriter;
import com.carrot.cache.io.CompressedBlockFileDataReader;
import com.carrot.cache.io.CompressedBlockMemoryDataReader;
import com.carrot.cache.util.CarrotConfig;

public class TestCacheWithCompressionBase extends TestCacheBase {
  
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
    File  dir = path.toFile();
    dir.deleteOnExit();
    String rootDir = dir.getAbsolutePath();
    
    Builder builder = new Builder(cacheName);
    
    builder
      .withCacheDataSegmentSize(segmentSize)
      .withCacheMaximumSize(maxCacheSize)
      .withScavengerRunInterval(scavengerInterval)
      .withScavengerDumpEntryBelowStart(scavDumpBelowRatio)
      .withRecyclingSelector(recycleSelector)
      .withDataWriter(dataWriter)
      .withMemoryDataReader(dataReaderMemory)
      .withFileDataReader(dataReaderFile)
      .withMainQueueIndexFormat(indexFormat)
      .withCacheRootDir(rootDir)
      .withMinimumActiveDatasetRatio(minActiveRatio)
      .withCacheStreamingSupportBufferSize(1 << 19)
      .withEvictionDisabledMode(true);
    if (maxKeyValueSize > 0) {
      builder.withMaximumKeyValueSize(maxKeyValueSize);
    }
    if (offheap) {
      return builder.buildMemoryCache();
    } else {
      return builder.buildDiskCache();
    }
  }
  
  protected void initTest(String cacheName) throws IOException {
    this.dataWriter = CompressedBlockDataWriter.class.getName();
    this.dataReaderMemory = CompressedBlockMemoryDataReader.class.getName();
    this.dataReaderFile = CompressedBlockFileDataReader.class.getName(); 
    CarrotConfig config = CarrotConfig.getInstance();
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
      Arrays.stream(files).forEach( x -> x.delete());
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
    CarrotConfig config = CarrotConfig.getInstance();
    String dictDir = config.getCacheDictionaryDir(cacheName);
    File dir = new File(dictDir);
    if (dir.exists()) {
      File[] files = dir.listFiles();
      Arrays.stream(files).forEach( x -> x.delete());
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
    /*DEBUG*/ System.out.println("Test dictionary=" + dictionaryEnabled + " random data=" + randomData);
    if (randomData) {
      prepareRandomData(numRecords);
    } else {
      try {
        prepareGithubData(numRecords);
      } catch(Exception e) {
        throw new RuntimeException(e);
      }
    }
  }
  
}
