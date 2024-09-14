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
package com.carrotdata.cache.io;

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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotdata.cache.compression.zstd.ZstdCompressionCodec;
import com.carrotdata.cache.util.CacheConfig;

public class TestMemoryIOEngineWithCompression extends IOCompressionTestBase {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestMemoryIOEngineWithCompression.class);

  @Before
  public void setUp() {
    r = new Random();
    long seed = System.currentTimeMillis();
    r.setSeed(seed);
    /* DEBUG */ LOG.info("r.seed=" + seed);
  }

  @After
  public void tearDown() throws IOException {
    super.tearDown();
    engine.dispose();
    ZstdCompressionCodec.reset();
  }

  @Test
  public void testLoadReadBytesSingleSegmentNoDictionaryNoRandom()
      throws IOException, URISyntaxException {
    createEngine(4 * 1024 * 1024, 4 * 1024 * 1024, false, false, 10000);
    testLoadReadBytesSingleSegment();
  }

  @Test
  public void testLoadReadBytesSingleSegmentNoDictionaryRandom()
      throws IOException, URISyntaxException {
    createEngine(4 * 1024 * 1024, 4 * 1024 * 1024, true, false, 10000);
    testLoadReadBytesSingleSegment();
  }

  @Test
  public void testLoadReadBytesSingleSegmentDictionaryNoRandom()
      throws IOException, URISyntaxException {
    createEngine(4 * 1024 * 1024, 4 * 1024 * 1024, false, true, 10000);
    testLoadReadBytesSingleSegment();
  }

  @Test
  public void testLoadReadBytesSingleSegmentDictionaryRandom()
      throws IOException, URISyntaxException {
    createEngine(4 * 1024 * 1024, 4 * 1024 * 1024, true, true, 10000);
    testLoadReadBytesSingleSegment();
  }

  private void testLoadReadBytesSingleSegment() throws IOException {
    /* DEBUG */ LOG.info("testLoadReadBytesSingleSegment");
    this.numRecords = 10000;
    int loaded = loadBytesEngine(engine);
    /* DEBUG */ LOG.info("loaded=" + loaded);
    verifyBytesEngine(engine, loaded);
    verifyBytesEngineByteBuffer(engine, loaded);
  }

  @Test
  public void testLoadReadMemorySingleSegmentNoDictionaryNoRandom()
      throws IOException, URISyntaxException {
    createEngine(4 * 1024 * 1024, 4 * 1024 * 1024, false, false, 10000);
    testLoadReadMemorySingleSegment();
  }

  @Test
  public void testLoadReadMemorySingleSegmentNoDictionaryRandom()
      throws IOException, URISyntaxException {
    createEngine(4 * 1024 * 1024, 4 * 1024 * 1024, true, false, 10000);
    testLoadReadMemorySingleSegment();
  }

  @Test
  public void testLoadReadMemorySingleSegmentDictionaryNoRandom()
      throws IOException, URISyntaxException {
    createEngine(4 * 1024 * 1024, 4 * 1024 * 1024, false, true, 10000);
    testLoadReadMemorySingleSegment();
  }

  @Test
  public void testLoadReadMemorySingleSegmentDictionaryRandom()
      throws IOException, URISyntaxException {
    createEngine(4 * 1024 * 1024, 4 * 1024 * 1024, true, true, 10000);
    testLoadReadMemorySingleSegment();
  }

  private void testLoadReadMemorySingleSegment() throws IOException {
    /* DEBUG */ LOG.info("testLoadReadMemorySingleSegment");
    int loaded = loadMemoryEngine(engine);
    /* DEBUG */ LOG.info("loaded=" + loaded);
    verifyMemoryEngine(engine, loaded);
  }

  @Test
  public void testLoadReadBytesMultipleSegmentsNoDictionaryNoRandom()
      throws IOException, URISyntaxException {
    createEngine(4 * 1024 * 1024, 20 * 1024 * 1024, false, false, 100000);
    testLoadReadBytesMultipleSegments();
  }

  @Test
  public void testLoadReadBytesMultipleSegmentsNoDictionaryRandom()
      throws IOException, URISyntaxException {
    createEngine(4 * 1024 * 1024, 20 * 1024 * 1024, true, false, 100000);
    testLoadReadBytesMultipleSegments();
  }

  @Test
  public void testLoadReadBytesMultipleSegmentsDictionaryNoRandom()
      throws IOException, URISyntaxException {
    createEngine(4 * 1024 * 1024, 20 * 1024 * 1024, false, true, 100000);
    testLoadReadBytesMultipleSegments();
  }

  @Test
  public void testLoadReadBytesMultipleSegmentsDictionaryRandom()
      throws IOException, URISyntaxException {
    createEngine(4 * 1024 * 1024, 20 * 1024 * 1024, true, true, 100000);
    testLoadReadBytesMultipleSegments();
  }

  private void testLoadReadBytesMultipleSegments() throws IOException {
    /* DEBUG */ LOG.info("testLoadReadBytesMultipleSegments");
    this.numRecords = 100000;
    int loaded = loadBytesEngine(engine);
    /* DEBUG */ LOG.info("loaded=" + loaded);
    verifyBytesEngine(engine, loaded);
    verifyBytesEngineByteBuffer(engine, loaded);
  }

  @Test
  public void testLoadReadMemoryMultipleSegmentsNoDictionaryNoRandom()
      throws IOException, URISyntaxException {
    createEngine(4 * 1024 * 1024, 20 * 1024 * 1024, false, false, 100000);
    testLoadReadMemoryMultipleSegments();
  }

  @Test
  public void testLoadReadMemoryMultipleSegmentsNoDictionaryRandom()
      throws IOException, URISyntaxException {
    createEngine(4 * 1024 * 1024, 20 * 1024 * 1024, true, false, 100000);
    testLoadReadMemoryMultipleSegments();
  }

  @Test
  public void testLoadReadMemoryMultipleSegmentsDictionaryNoRandom()
      throws IOException, URISyntaxException {
    createEngine(4 * 1024 * 1024, 20 * 1024 * 1024, false, true, 100000);
    testLoadReadMemoryMultipleSegments();
  }

  @Test
  public void testLoadReadMemoryMultipleSegmentsDictionaryRandom()
      throws IOException, URISyntaxException {
    createEngine(4 * 1024 * 1024, 20 * 1024 * 1024, true, true, 100000);
    testLoadReadMemoryMultipleSegments();
  }

  private void testLoadReadMemoryMultipleSegments() throws IOException {
    /* DEBUG */ LOG.info("testLoadReadMemoryMultipleSegments");
    this.numRecords = 100000;
    int loaded = loadMemoryEngine(engine);
    /* DEBUG */ LOG.info("loaded=" + loaded);
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
    /* DEBUG */ LOG.info("testLoadReadBytesMultipleSegmentsWithDeletes");
    int loaded = loadBytesEngine(engine);
    /* DEBUG */ LOG.info("loaded=" + loaded);
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
    /* DEBUG */ LOG.info("testLoadReadMemoryMultipleSegmentsWithDeletes");
    int loaded = loadMemoryEngine(engine);
    /* DEBUG */ LOG.info("loaded=" + loaded);
    deleteMemoryEngine(engine, loaded / 2);
    verifyMemoryEngineWithDeletes(engine, loaded, loaded / 2);
  }

  @Test
  public void testLoadSave() throws IOException, URISyntaxException {
    /* DEBUG */ LOG.info("testLoadSave");
    createEngine(4 * 1024 * 1024, 20 * 4 * 1024 * 1024, false, true, 100000);
    int loaded = loadMemoryEngine(engine);
    /* DEBUG */ LOG.info("loaded=" + loaded);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    this.engine.save(dos);
    this.engine.getMemoryIndex().save(dos);
    // Get current config, we will reuse it later because it keeps location of a dictionary folder
    CacheConfig config = CacheConfig.getInstance();
    // Dispose engine
    engine.dispose();
    // Re-create new one (creates new root directory for cache)
    engine = getEngine(config);

    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    DataInputStream dis = new DataInputStream(bais);
    engine.load(dis);
    engine.getMemoryIndex().load(dis);
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
    return new MemoryIOEngine(conf);
  }

}
