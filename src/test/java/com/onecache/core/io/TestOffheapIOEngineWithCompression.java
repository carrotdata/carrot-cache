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
package com.onecache.core.io;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Random;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.onecache.core.compression.zstd.ZstdCompressionCodec;
import com.onecache.core.util.CacheConfig;

public class TestOffheapIOEngineWithCompression extends IOCompressionTestBase{
  
  @Before
  public void setUp() {
    r = new Random();
    long seed = System.currentTimeMillis();
    r.setSeed(seed);
    /*DEBUG*/ System.out.println("r.seed=" + seed);
  }
  
  @After
  public void tearDown() throws IOException {
    super.tearDown();
    engine.dispose();
    ZstdCompressionCodec.reset();
  }
  
  
  @Test
  public void testLoadReadBytesSingleSegmentNoDictionaryNoRandom() throws IOException, URISyntaxException {
    createEngine(4 * 1024 * 1024, 4 * 1024 * 1024, false, false, 10000);
    testLoadReadBytesSingleSegment();
  }
  
  
  @Test
  public void testLoadReadBytesSingleSegmentNoDictionaryRandom() throws IOException, URISyntaxException {
    createEngine(4 * 1024 * 1024, 4 * 1024 * 1024, true, false, 10000);
    testLoadReadBytesSingleSegment();
  }

  
  @Test
  public void testLoadReadBytesSingleSegmentDictionaryNoRandom() throws IOException, URISyntaxException {
    createEngine(4 * 1024 * 1024, 4 * 1024 * 1024, false, true, 10000);
    testLoadReadBytesSingleSegment();
  }
  
  
  @Test
  public void testLoadReadBytesSingleSegmentDictionaryRandom() throws IOException, URISyntaxException {
    createEngine(4 * 1024 * 1024, 4 * 1024 * 1024, true, true, 10000);
    testLoadReadBytesSingleSegment();
  }
  
  private void testLoadReadBytesSingleSegment() throws IOException {
    /*DEBUG*/ System.out.println("testLoadReadBytesSingleSegment");
    this.numRecords = 10000;
    int loaded = loadBytesEngine(engine);
    /*DEBUG*/ System.out.println("loaded=" + loaded);
    verifyBytesEngine(engine, loaded);
    verifyBytesEngineByteBuffer(engine, loaded);
  }
  
  
  @Test
  public void testLoadReadMemorySingleSegmentNoDictionaryNoRandom() throws IOException, URISyntaxException {
    createEngine(4 * 1024 * 1024, 4 * 1024 * 1024, false, false, 10000);
    testLoadReadMemorySingleSegment();
  }
  
  
  @Test
  public void testLoadReadMemorySingleSegmentNoDictionaryRandom() throws IOException, URISyntaxException {
    createEngine(4 * 1024 * 1024, 4 * 1024 * 1024, true, false, 10000);
    testLoadReadMemorySingleSegment();
  }
  
  
  @Test
  public void testLoadReadMemorySingleSegmentDictionaryNoRandom() throws IOException, URISyntaxException {
    createEngine(4 * 1024 * 1024, 4 * 1024 * 1024, false, true, 10000);
    testLoadReadMemorySingleSegment();
  }
  
  
  @Test
  public void testLoadReadMemorySingleSegmentDictionaryRandom() throws IOException, URISyntaxException {
    createEngine(4 * 1024 * 1024, 4 * 1024 * 1024, true, true, 10000);
    testLoadReadMemorySingleSegment();
  }
  
  private void testLoadReadMemorySingleSegment() throws IOException {
    /*DEBUG*/ System.out.println("testLoadReadMemorySingleSegment");
    int loaded = loadMemoryEngine(engine);
    /*DEBUG*/ System.out.println("loaded=" + loaded);
    verifyMemoryEngine(engine, loaded);
  }
  
  @Test
  public void testLoadReadBytesMultipleSegmentsNoDictionaryNoRandom() throws IOException, URISyntaxException {
    createEngine(4 * 1024 * 1024, 20 * 1024 * 1024, false, false, 100000);
    testLoadReadBytesMultipleSegments();
  }
  
  @Test
  public void testLoadReadBytesMultipleSegmentsNoDictionaryRandom() throws IOException, URISyntaxException {
    createEngine(4 * 1024 * 1024, 20 * 1024 * 1024, true, false, 100000);
    testLoadReadBytesMultipleSegments();
  }
  
  @Test
  public void testLoadReadBytesMultipleSegmentsDictionaryNoRandom() throws IOException, URISyntaxException {
    createEngine(4 * 1024 * 1024, 20 * 1024 * 1024, false, true, 100000);
    testLoadReadBytesMultipleSegments();
  }
  
  @Test
  public void testLoadReadBytesMultipleSegmentsDictionaryRandom() throws IOException, URISyntaxException {
    createEngine(4 * 1024 * 1024, 20 * 1024 * 1024, true, true, 100000);
    testLoadReadBytesMultipleSegments();
  }
  
  private void testLoadReadBytesMultipleSegments() throws IOException {
    /*DEBUG*/ System.out.println("testLoadReadBytesMultipleSegments");
    this.numRecords = 100000;
    int loaded = loadBytesEngine(engine);
    /*DEBUG*/ System.out.println("loaded=" + loaded);
    verifyBytesEngine(engine, loaded);
    verifyBytesEngineByteBuffer(engine, loaded);
  }
  
  @Test
  public void testLoadReadMemoryMultipleSegmentsNoDictionaryNoRandom() throws IOException, URISyntaxException {
    createEngine(4 * 1024 * 1024, 20 * 1024 * 1024, false, false, 100000);
    testLoadReadMemoryMultipleSegments();
  }
  
  @Test
  public void testLoadReadMemoryMultipleSegmentsNoDictionaryRandom() throws IOException, URISyntaxException {
    createEngine(4 * 1024 * 1024, 20 * 1024 * 1024, true, false, 100000);
    testLoadReadMemoryMultipleSegments();
  }
  
  
  @Test
  public void testLoadReadMemoryMultipleSegmentsDictionaryNoRandom() throws IOException, URISyntaxException {
    createEngine(4 * 1024 * 1024, 20 * 1024 * 1024, false, true, 100000);
    testLoadReadMemoryMultipleSegments();
  }
  
  
  @Test
  public void testLoadReadMemoryMultipleSegmentsDictionaryRandom() throws IOException, URISyntaxException {
    createEngine(4 * 1024 * 1024, 20 * 1024 * 1024, true, true, 100000);
    testLoadReadMemoryMultipleSegments();
  }
  
  private void testLoadReadMemoryMultipleSegments() throws IOException {
    /*DEBUG*/ System.out.println("testLoadReadMemoryMultipleSegments");
    this.numRecords = 100000;
    int loaded = loadMemoryEngine(engine);
    /*DEBUG*/ System.out.println("loaded=" + loaded);
    verifyMemoryEngine(engine, loaded);
  }
  
  
  @Test
  public void testLoadReadBytesMultipleSegmentsWithDeletesNoDictionaryNoRandom() 
      throws IOException, URISyntaxException {
    createEngine(4 * 1024 * 1024, 20 * 4 * 1024 * 1024, false, false, 100000);
    testLoadReadBytesMultipleSegmentsWithDeletes();
  }

  
  @Test
  public void testLoadReadBytesMultipleSegmentsWithDeletesNoDictionaryRandom() 
      throws IOException, URISyntaxException {
    createEngine(4 * 1024 * 1024, 20 * 4 * 1024 * 1024, true, false, 100000);
    testLoadReadBytesMultipleSegmentsWithDeletes();
  }
  
  
  @Test
  public void testLoadReadBytesMultipleSegmentsWithDeletesDictionaryNoRandom() 
      throws IOException, URISyntaxException {
    createEngine(4 * 1024 * 1024, 20 * 4 * 1024 * 1024, false, true, 100000);
    testLoadReadBytesMultipleSegmentsWithDeletes();
  }

  
  @Test
  public void testLoadReadBytesMultipleSegmentsWithDeletesDictionaryRandom() 
      throws IOException, URISyntaxException {
    createEngine(4 * 1024 * 1024, 20 * 4 * 1024 * 1024, true, true, 100000);
    testLoadReadBytesMultipleSegmentsWithDeletes();
  }
  
  
  private void testLoadReadBytesMultipleSegmentsWithDeletes() throws IOException {
    /*DEBUG*/ System.out.println("testLoadReadBytesMultipleSegmentsWithDeletes");
    int loaded = loadBytesEngine(engine);
    /*DEBUG*/ System.out.println("loaded=" + loaded);
    deleteBytesEngine(engine, loaded / 2);
    verifyBytesEngineWithDeletes(engine, loaded, loaded / 2);
  }
  
  
  @Test
  public void testLoadReadMemoryMultipleSegmentsWithDeletesNoDictionaryNoRandom() 
      throws IOException, URISyntaxException {
    createEngine(4 * 1024 * 1024, 20 * 4 * 1024 * 1024, false, false, 100000);
    testLoadReadMemoryMultipleSegmentsWithDeletes();
  }

  
  @Test
  public void testLoadReadMemoryMultipleSegmentsWithDeletesNoDictionaryRandom() 
      throws IOException, URISyntaxException {
    createEngine(4 * 1024 * 1024, 20 * 4 * 1024 * 1024, true, false, 100000);
    testLoadReadMemoryMultipleSegmentsWithDeletes();
  }
  
  
  @Test
  public void testLoadReadMemoryMultipleSegmentsWithDeletesDictionaryNoRandom() 
      throws IOException, URISyntaxException {
    createEngine(4 * 1024 * 1024, 20 * 4 * 1024 * 1024, false, true, 100000);
    testLoadReadBytesMultipleSegmentsWithDeletes();
  }

  
  @Test
  public void testLoadReadMemoryMultipleSegmentsWithDeletesDictionaryRandom() 
      throws IOException, URISyntaxException {
    createEngine(4 * 1024 * 1024, 20 * 4 * 1024 * 1024, true, true, 100000);
    testLoadReadMemoryMultipleSegmentsWithDeletes();
  }
  
  private void testLoadReadMemoryMultipleSegmentsWithDeletes() throws IOException {
    /*DEBUG*/ System.out.println("testLoadReadMemoryMultipleSegmentsWithDeletes");
    int loaded = loadMemoryEngine(engine);
    /*DEBUG*/ System.out.println("loaded=" + loaded);
    deleteMemoryEngine(engine, loaded / 2);
    verifyMemoryEngineWithDeletes(engine, loaded, loaded / 2);
  }
  
  @Test
  public void testLoadSave() throws IOException, URISyntaxException {
    /*DEBUG*/ System.out.println("testLoadSave");
    createEngine(4 * 1024 * 1024, 20 * 4 * 1024 * 1024, false, true, 100000);
    int loaded = loadMemoryEngine(engine);
    /*DEBUG*/ System.out.println("loaded=" + loaded);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    this.engine.save(dos);
    // Get current config, we will reuse it later because it keeps location of a dictionary folder
    CacheConfig config = CacheConfig.getInstance();
    // Dispose engine
    engine.dispose();
    // Re-create new one (creates new root directory for cache)
    engine = getEngine(config);
 
    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    DataInputStream dis = new DataInputStream(bais);
    engine.load(dis);
    verifyMemoryEngine(engine, loaded);
  }
  
  protected void createEngine(long segmentSize, long cacheSize, boolean random, boolean dictionary, 
      int numRecords) throws IOException, URISyntaxException {
    this.numRecords = numRecords;
    this.segmentSize = (int) segmentSize;
    this.cacheSize = cacheSize;
    initTestForEngine(random, dictionary);
    CacheConfig conf = CacheConfig.getInstance();
    this.engine = getEngine(conf);
  }
  
  protected IOEngine getEngine(CacheConfig conf) {
    conf.setDataWriter(cacheName, CompressedBlockDataWriter.class.getName());
    conf.setMemoryDataReader(cacheName, CompressedBlockMemoryDataReader.class.getName());
    return new OffheapIOEngine(conf);
  }
   
}
