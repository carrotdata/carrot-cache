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
package com.carrot.cache.io;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Random;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.carrot.cache.util.CarrotCacheConfig;
import com.carrot.cache.util.TestUtils;

public class TestOffheapIOEngine extends IOTestBase{
  
  OffheapIOEngine engine;
  long cacheSize;
  
  @Before
  public void setUp() {
    r = new Random();
    long seed = System.currentTimeMillis();
    r.setSeed(seed);
    /*DEBUG*/ System.out.println("r.seed=" + seed);
  }
  
  @After
  public void tearDown() {
    super.tearDown();
    engine.dispose();
  }
  
  @Test
  public void testLoadReadBytesSingleSegment() throws IOException {
    /*DEBUG*/ System.out.println("testLoadReadBytesSingleSegment");
    createEngine(4 * 1024 * 1024, 4 * 1024 * 1024);
    prepareData(10000);
    int loaded = loadBytesEngine(engine);
    /*DEBUG*/ System.out.println("loaded=" + loaded);
    verifyBytesEngine(engine, loaded);
    verifyBytesEngineByteBuffer(engine, loaded);
  }
  
  @Test
  public void testLoadReadMemorySingleSegment() throws IOException {
    /*DEBUG*/ System.out.println("testLoadReadMemorySingleSegment");
    createEngine(4 * 1024 * 1024, 4 * 1024 * 1024);
    prepareData(10000);
    int loaded = loadMemoryEngine(engine);
    /*DEBUG*/ System.out.println("loaded=" + loaded);
    verifyMemoryEngine(engine, loaded);
  }
  
  @Test
  public void testLoadReadBytesMultipleSegments() throws IOException {
    /*DEBUG*/ System.out.println("testLoadReadBytesMultipleSegments");
    createEngine(4 * 1024 * 1024, 20 * 4 * 1024 * 1024);
    prepareData(100000);
    int loaded = loadBytesEngine(engine);
    /*DEBUG*/ System.out.println("loaded=" + loaded);
    verifyBytesEngine(engine, loaded);
    verifyBytesEngineByteBuffer(engine, loaded);
  }
  
  @Test
  public void testLoadReadMemoryMultipleSegments() throws IOException {
    /*DEBUG*/ System.out.println("testLoadReadMemoryMultipleSegments");
    createEngine(4 * 1024 * 1024, 20 * 4 * 1024 * 1024);
    prepareData(100000);
    int loaded = loadMemoryEngine(engine);
    /*DEBUG*/ System.out.println("loaded=" + loaded);
    verifyMemoryEngine(engine, loaded);
  }
  
  @Test
  public void testLoadReadBytesMultipleSegmentsWithDeletes() throws IOException {
    /*DEBUG*/ System.out.println("testLoadReadBytesMultipleSegmentsWithDeletes");
    createEngine(4 * 1024 * 1024, 20 * 4 * 1024 * 1024);
    prepareData(100000);
    int loaded = loadBytesEngine(engine);
    /*DEBUG*/ System.out.println("loaded=" + loaded);
    deleteBytesEngine(engine, loaded / 2);
    verifyBytesEngineWithDeletes(engine, loaded, loaded / 2);
  }
  
  @Test
  public void testLoadReadMemoryMultipleSegmentsWithDeletes() throws IOException {
    /*DEBUG*/ System.out.println("testLoadReadMemoryMultipleSegmentsWithDeletes");
    createEngine(4 * 1024 * 1024, 20 * 4 * 1024 * 1024);
    prepareData(100000);
    int loaded = loadMemoryEngine(engine);
    /*DEBUG*/ System.out.println("loaded=" + loaded);
    deleteMemoryEngine(engine, loaded / 2);
    verifyMemoryEngineWithDeletes(engine, loaded, loaded / 2);
  }
  
  @Test
  public void testLoadSave() throws IOException {
    /*DEBUG*/ System.out.println("testLoadSave");
    createEngine(4 * 1024 * 1024, 4 * 4 * 1024 * 1024);
    prepareData(100000);
    int loaded = loadMemoryEngine(engine);
    /*DEBUG*/ System.out.println("loaded=" + loaded);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    this.engine.save(dos);
    // Dispose engine
    engine.dispose();
    // Re-create new one
    createEngine(4 * 1024 * 1024, 4 * 4 * 1024 * 1024);
    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    DataInputStream dis = new DataInputStream(bais);
    engine.load(dis);
    verifyMemoryEngine(engine, loaded);
  }
  
  private void createEngine(long segmentSize, long cacheSize) throws IOException {
    this.segmentSize = (int) segmentSize;
    this.cacheSize = cacheSize;
    CarrotCacheConfig conf = TestUtils.mockConfigForTests(this.segmentSize, this.cacheSize);
    this.engine = new OffheapIOEngine(conf);
  }
   
}
