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

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Random;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.carrot.cache.Cache;
import com.carrot.cache.index.MemoryIndex;
import com.carrot.cache.index.MemoryIndex.Type;
import com.carrot.cache.io.Segment.Info;
import com.carrot.cache.util.UnsafeAccess;

public class TestSegmentBlockDataWriterReaderMemory extends IOTestBase{
  
  int blockSize = 4096;
  
  @Before
  public void setUp() {
    this.index = new MemoryIndex("default", Type.MQ);
    this.segmentSize = 4 * 1024 * 1024;
    this.numRecords = 10000;
    
    this.r = new Random();
    long seed = System.currentTimeMillis();
    r.setSeed(seed);
    
    System.out.println("r.seed="+ seed);
    segment = Segment.newSegment(this.segmentSize, 1, 1, System.currentTimeMillis());
    prepareData(this.numRecords);
    BlockDataWriter bdw = new BlockDataWriter();
    bdw.setBlockSize(blockSize);
    segment.setDataWriter(bdw);
  }
  
  @After
  public void tearDown() {
    super.tearDown();
    this.segment.dispose();
    UnsafeAccess.mallocStats.printStats();

  }
  
  @Test
  public void testWritesBytes() throws IOException {
    int count = loadBytes();
    long expire = expires[count -1];
    assertEquals(expire, segment.getInfo().getMaxExpireAt());
    verifyBytesBlock(count, blockSize);
    
    DataReader reader = new BlockMemoryDataReader();
    reader.init("default");
    IOEngine engine  = Mockito.mock(IOEngine.class);
    Mockito.when(engine.getSegmentById(Mockito.anyInt())).thenReturn(segment);
    verifyBytesWithReader(count, reader, engine);
  }
  
  @Test
  public void testWritesMemory() throws IOException {
    int count = loadMemory();
    long expire = expires[count -1];
    assertEquals(expire, segment.getInfo().getMaxExpireAt());
    verifyMemoryBlock(count, blockSize);
    
    DataReader reader = new BlockMemoryDataReader();
    reader.init("default");
    IOEngine engine  = Mockito.mock(IOEngine.class);
    Mockito.when(engine.getSegmentById(Mockito.anyInt())).thenReturn(segment);
    verifyMemoryWithReader(count, reader, engine);

  }

  @Test
  public void testSegmentScanner() throws IOException {
    int count = loadBytes();
    
    verifyBytesBlock(count, blockSize);
    
    DataReader reader = new BlockMemoryDataReader();
    reader.init("default");
    IOEngine engine  = Mockito.mock(IOEngine.class);
    Mockito.when(engine.getSegmentById(Mockito.anyInt())).thenReturn(segment);
    Cache cache = Mockito.mock(Cache.class);
    Mockito.when(cache.getName()).thenReturn("default");
    Mockito.when(engine.getCache()).thenReturn(cache);
    // Must be sealed for scanner
    segment.seal();
    
    verifyBytesWithReader(count, reader, engine);
    
    
    SegmentScanner scanner = reader.getSegmentScanner(engine, segment);
    verifyScanner(scanner, count);
  }
  
  @Test
  public void testSaveLoad() throws IOException {
    int count = loadBytes();
    // now save Info and Segment separately
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    Info info = segment.getInfo();
    info.save(dos);
    segment.save(dos);
    // Now load back
    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    DataInputStream dis = new DataInputStream(bais);
    info = new Info();
    info.load(dis);
    Segment seg = new Segment();
    seg.load(dis);
    seg.setInfo(info);
    seg.setDataWriter(new BlockDataWriter());
    segment.dispose();
    segment = seg;
    verifyBytesBlock(count, blockSize);
    
    DataReader reader = new BlockMemoryDataReader();
    reader.init("default");
    IOEngine engine  = Mockito.mock(IOEngine.class);
    Mockito.when(engine.getSegmentById(Mockito.anyInt())).thenReturn(segment);
    verifyBytesWithReader(count, reader, engine);
  }
}
