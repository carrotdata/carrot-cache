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
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotdata.cache.util.CacheConfig;

public class TestFileIOEngine extends IOTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(TestFileIOEngine.class);

  FileIOEngine engine;
  long cacheSize;

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
  }

  @Test
  public void testLoadReadBytesSingleSegment() throws IOException {
    /* DEBUG */ LOG.info("testLoadReadBytesSingleSegment");
    createEngine(4 * 1024 * 1024, 4 * 1024 * 1024);
    prepareRandomData(10000);
    int loaded = loadBytesEngine(engine);
    /* DEBUG */ LOG.info("loaded=" + loaded);
    verifyBytesEngine(engine, loaded);
    verifyBytesEngineByteBuffer(engine, loaded);
  }

  @Test
  public void testLoadReadMemorySingleSegment() throws IOException {
    /* DEBUG */ LOG.info("testLoadReadMemorySingleSegment");
    createEngine(4 * 1024 * 1024, 4 * 1024 * 1024);
    prepareRandomData(10000);
    int loaded = loadMemoryEngine(engine);
    /* DEBUG */ LOG.info("loaded=" + loaded);
    verifyMemoryEngine(engine, loaded);
  }

  @Test
  public void testLoadReadBytesMultipleSegments() throws IOException {
    /* DEBUG */ LOG.info("testLoadReadBytesMultipleSegments");
    createEngine(4 * 1024 * 1024, 20 * 4 * 1024 * 1024);
    prepareRandomData(100000);
    int loaded = loadBytesEngine(engine);
    /* DEBUG */ LOG.info("loaded=" + loaded);
    verifyBytesEngine(engine, loaded);
    verifyBytesEngineByteBuffer(engine, loaded);
  }

  @Test
  public void testLoadReadMemoryMultipleSegments() throws IOException {
    /* DEBUG */ LOG.info("testLoadReadMemoryMultipleSegments");
    createEngine(4 * 1024 * 1024, 20 * 4 * 1024 * 1024);
    prepareRandomData(100000);
    int loaded = loadMemoryEngine(engine);
    /* DEBUG */ LOG.info("loaded=" + loaded);
    verifyMemoryEngine(engine, loaded);
  }

  @Test
  public void testLoadReadBytesMultipleSegmentsWithDeletes() throws IOException {
    /* DEBUG */ LOG.info("testLoadReadBytesMultipleSegmentsWithDeletes");
    createEngine(4 * 1024 * 1024, 20 * 4 * 1024 * 1024);
    prepareRandomData(100000);
    int loaded = loadBytesEngine(engine);
    /* DEBUG */ LOG.info("loaded=" + loaded);
    deleteBytesEngine(engine, loaded / 2);
    verifyBytesEngineWithDeletes(engine, loaded, loaded / 2);
  }

  @Test
  public void testLoadReadMemoryMultipleSegmentsWithDeletes() throws IOException {
    /* DEBUG */ LOG.info("testLoadReadMemoryMultipleSegmentsWithDeletes");
    createEngine(4 * 1024 * 1024, 20 * 4 * 1024 * 1024);
    prepareRandomData(100000);
    int loaded = loadMemoryEngine(engine);
    /* DEBUG */ LOG.info("loaded=" + loaded);
    deleteMemoryEngine(engine, loaded / 2);
    verifyMemoryEngineWithDeletes(engine, loaded, loaded / 2);
  }

  @Test
  public void testLoadSave() throws IOException {
    /* DEBUG */ LOG.info("testLoadSave");
    // data directory
    Path path = Files.createTempDirectory(null);
    File dir = path.toFile();
    dir.deleteOnExit();
    String dataDir = dir.getAbsolutePath();

    createEngine(4 * 1024 * 1024, 4 * 4 * 1024 * 1024, dataDir);
    prepareRandomData(100000);
    int loaded = loadMemoryEngine(engine);
    /* DEBUG */ LOG.info("loaded=" + loaded);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    this.engine.save(dos);
    // Dispose engine
    engine.dispose();
    // Re-create new one
    createEngine(4 * 1024 * 1024, 4 * 4 * 1024 * 1024, dataDir);
    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    DataInputStream dis = new DataInputStream(bais);
    engine.load(dis);
    verifyMemoryEngine(engine, loaded);
  }

  private void createEngine(long segmentSize, long cacheSize) throws IOException {
    this.segmentSize = (int) segmentSize;
    this.cacheSize = cacheSize;
    CacheConfig conf = CacheConfig.getInstance();
    conf.setCacheSegmentSize("default", segmentSize);
    conf.setCacheMaximumSize("default", cacheSize);
    this.engine = new FileIOEngine(conf);
  }

  private void createEngine(long segmentSize, long cacheSize, String dataDir) throws IOException {
    this.segmentSize = (int) segmentSize;
    this.cacheSize = cacheSize;
    CacheConfig conf = CacheConfig.getInstance();
    conf.setCacheSegmentSize("default", segmentSize);
    conf.setCacheMaximumSize("default", cacheSize);
    this.engine = new FileIOEngine(conf);
  }
}