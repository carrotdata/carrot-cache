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
package com.carrotdata.cache.io;

import static com.carrotdata.cache.compression.CompressionCodec.COMP_META_SIZE;
import static com.carrotdata.cache.compression.CompressionCodec.COMP_SIZE_OFFSET;
import static com.carrotdata.cache.compression.CompressionCodec.DICT_VER_OFFSET;
import static com.carrotdata.cache.compression.CompressionCodec.SIZE_OFFSET;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Random;

import org.junit.After;
import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotdata.cache.compression.CodecFactory;
import com.carrotdata.cache.compression.CompressionCodec;
import com.carrotdata.cache.compression.zstd.ZstdCompressionCodec;
import com.carrotdata.cache.index.MemoryIndex;
import com.carrotdata.cache.index.MemoryIndex.Type;
import com.carrotdata.cache.util.CacheConfig;
import com.carrotdata.cache.util.UnsafeAccess;
import com.carrotdata.cache.util.Utils;

public class IOCompressionTestBase extends IOTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(IOCompressionTestBase.class);

  @Before
  public void setUp() throws URISyntaxException, IOException {
    this.index = new MemoryIndex(cacheName, Type.MQ);
    this.segmentSize = 8 * 1024 * 1024;
    this.numRecords = 20000;
    this.r = new Random();
    long ptr = UnsafeAccess.mallocZeroed(this.segmentSize);
    this.segment = Segment.newSegment(ptr, this.segmentSize, 1, 1);
    this.segment.init(cacheName);
  }

  private void initCodec() throws IOException {
    CodecFactory factory = CodecFactory.getInstance();
    factory.clear();
    CompressionCodec codec = factory.getCompressionCodecForCache(cacheName);
    if (codec == null) {
      factory.initCompressionCodecForCache(cacheName, null);
    }
  }
  
  protected void initTestForSegment(boolean randomData, boolean dictionaryEnabled) throws URISyntaxException, IOException {
    initTest(randomData, dictionaryEnabled);
    CompressedBlockDataWriter writer = new CompressedBlockDataWriter();
    writer.init(cacheName);
    this.segment.setDataWriterAndEngine(writer, null);
  }
  
  protected void initTestForEngine(boolean randomData, boolean dictionaryEnabled) throws URISyntaxException, IOException {
    initTest(randomData, dictionaryEnabled);
    CacheConfig config = CacheConfig.getInstance();
    config.setCacheSegmentSize(cacheName, this.segmentSize);
    config.setCacheMaximumSize(cacheName, this.cacheSize);
  }
  
  protected void initTest(boolean randomData, boolean dictionaryEnabled) throws IOException, URISyntaxException {
    /*DEBUG*/ LOG.info("Test dictionary=" + dictionaryEnabled + " random data=" + randomData);
    if (randomData) {
      prepareRandomData(numRecords);
    } else {
      prepareGithubData(numRecords);
    }
    Path dataDir = Files.createTempDirectory("test");
    File rootDir = dataDir.toFile();
    rootDir.deleteOnExit();
    
    CacheConfig config = CacheConfig.getInstance();
    config.setCacheRootDir(cacheName, rootDir.getAbsolutePath());
    config.setCacheCompressionDictionaryEnabled(cacheName, dictionaryEnabled);
    config.setCacheCompressionEnabled(cacheName, true);
    config.setCacheTLSSupported(cacheName, true);
    if (dictionaryEnabled) {
      config.setCacheCompressionDictionaryTrainingAsync(cacheName, false);
    }
    // Clean up dictionaries
    String dictDir = config.getCacheDictionaryDir(cacheName);
    File dir = new File(dictDir);
    if (dir.exists()) {
      File[] files = dir.listFiles();
      Arrays.stream(files).forEach( x -> x.delete());
    }
    
    initCodec();
  }
  
  @After
  public void tearDown() throws IOException {
    //LOG.info("Data size=" + segment.getSegmentDataSize()); 
    super.tearDown();
    if (segment != null) {
      this.segment.dispose();
    }
    ZstdCompressionCodec.reset();
  }
  
  @Override
  protected void verifyBytes(int num) {
    long ptr = segment.getAddress();
    CompressionCodec codec = CodecFactory.getInstance().getCompressionCodecForCache(cacheName);
    
    byte[] buf = null;
    int decLen = 0;
    int compLen = 0;
    int dictId = 0;
    int off = -1;
    
    for (int i = 0; i < num; i++) {
      if (off == decLen || off == -1) {
        if (off != -1) {
          // Advance pointer - its not a first block
          ptr += COMP_META_SIZE + compLen;
        }
        //next block decompression
        decLen = UnsafeAccess.toInt(ptr + SIZE_OFFSET);
        dictId = UnsafeAccess.toInt(ptr + DICT_VER_OFFSET);
        compLen = UnsafeAccess.toInt(ptr + COMP_SIZE_OFFSET);

        if (buf == null || buf.length < decLen) {
          buf = new byte[decLen];
        }
        int len = decLen;
        if (dictId >= 0) {
          len = codec.decompress(ptr + COMP_META_SIZE, compLen, buf, dictId);
        } else {
          UnsafeAccess.copy(ptr + COMP_META_SIZE, buf, 0, decLen);
        }
        assertEquals(decLen, len);
        off = 0;
      }
      
      byte[] key = keys[i];
      byte[] value = values[i];
      
      int kSize = Utils.readUVInt(buf, off);
      int kSizeSize = Utils.sizeUVInt(kSize);
      int vSize = Utils.readUVInt(buf, off + kSizeSize);
      int vSizeSize = Utils.sizeUVInt(vSize);
      assertEquals(key.length, kSize);
      assertEquals(value.length, vSize);
      assertTrue(Utils.compareTo(key, 0, key.length, buf, off + kSizeSize + vSizeSize, kSize) == 0);
      assertTrue(Utils.compareTo(value, 0, value.length, buf, off + kSizeSize + vSizeSize + kSize, vSize) == 0);
      off += kSize + vSize + kSizeSize + vSizeSize;
    }
  }
  
  @Override
  protected void verifyMemory(int num) {
    long ptr = segment.getAddress();
    CompressionCodec codec = CodecFactory.getInstance().getCompressionCodecForCache(cacheName);
    
    byte[] buf = null;
    int decLen = 0;
    int compLen = 0;
    int dictId = 0;
    int off = -1;
    
    for (int i = 0; i < num; i++) {
      if (off == decLen || off == -1) {
        if (off != -1) {
          // Advance pointer - its not a first block
          ptr += COMP_META_SIZE + compLen;
        }
        //next block decompression
        decLen = UnsafeAccess.toInt(ptr + SIZE_OFFSET);
        dictId = UnsafeAccess.toInt(ptr + DICT_VER_OFFSET);
        compLen = UnsafeAccess.toInt(ptr + COMP_SIZE_OFFSET);
        if (buf == null || buf.length < decLen) {
          buf = new byte[decLen];
        }
        int len = decLen;
        if (dictId >= 0) {
          len = codec.decompress(ptr + COMP_META_SIZE, compLen, buf, dictId);
        } else {
          UnsafeAccess.copy(ptr + COMP_META_SIZE, buf, 0, decLen);
        }
        assertEquals(decLen, len);
        off = 0;
      }
      byte[] key = keys[i];
      byte[] value = values[i];
      
      int kSize = Utils.readUVInt(buf, off);
      int kSizeSize = Utils.sizeUVInt(kSize);
      int vSize = Utils.readUVInt(buf, off + kSizeSize);
      int vSizeSize = Utils.sizeUVInt(vSize);
      long mKey = mKeys[i];
      long mValue = mValues[i];
      assertEquals(key.length, kSize);
      assertEquals(value.length, vSize);
      assertTrue(Utils.compareTo(buf, off + kSizeSize + vSizeSize, kSize,  mKey, kSize) == 0);
      assertTrue(Utils.compareTo(buf, off + kSizeSize + vSizeSize + kSize, vSize, mValue, vSize) == 0);
      off += kSize + vSize + kSizeSize + vSizeSize;
    }
  }
}
