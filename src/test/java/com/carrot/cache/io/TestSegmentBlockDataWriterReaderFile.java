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

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Random;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.carrot.cache.Cache;
import com.carrot.cache.index.MemoryIndex;
import com.carrot.cache.index.MemoryIndex.Type;
import com.carrot.cache.util.TestUtils;
import com.carrot.cache.util.UnsafeAccess;

public class TestSegmentBlockDataWriterReaderFile extends IOTestBase{
  
  @Before
  public void setUp() {
    this.index = new MemoryIndex("default", Type.MQ);
    this.segmentSize = 15 * 1024 * 1024;
    this.numRecords = 10000;
    
    this.r = new Random();
    long seed = 1661456181831L;//System.currentTimeMillis();
    r.setSeed(seed);
    
    System.out.println("r.seed="+ seed);
    segment = Segment.newSegment(this.segmentSize, 1, 1);
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
    
    RandomAccessFile file = TestUtils.saveToFile(segment);
    
    DataReader reader = new BlockFileDataReader();
    reader.init("default");
    FileIOEngine engine  = Mockito.mock(FileIOEngine.class);
    Mockito.when(engine.getSegmentById(Mockito.anyInt())).thenReturn(segment);
    Mockito.when(engine.getFileFor(Mockito.anyInt())).thenReturn(file);
    verifyBytesWithReader(count, reader, engine);
    verifyBytesWithReaderByteBuffer(count, reader, engine);

  }
  
  @Test
  public void testWritesMemory() throws IOException {
    int count = loadMemory();
    long expire = expires[count -1];
    assertEquals(expire, segment.getInfo().getMaxExpireAt());
    verifyMemoryBlock(count, blockSize);
    
    RandomAccessFile file = TestUtils.saveToFile(segment);
    
    DataReader reader = new BlockFileDataReader();
    reader.init("default");
    FileIOEngine engine  = Mockito.mock(FileIOEngine.class);
    Mockito.when(engine.getSegmentById(Mockito.anyInt())).thenReturn(segment);
    Mockito.when(engine.getFileFor(Mockito.anyInt())).thenReturn(file);
    verifyMemoryWithReader(count, reader, engine);
    verifyMemoryWithReaderByteBuffer(count, reader, engine);

  }

  @Test
  public void testSegmentScanner() throws IOException {
    int count = loadBytes();
    
    verifyBytesBlock(count, blockSize);
    RandomAccessFile file = TestUtils.saveToFile(segment);

    DataReader reader = new BlockFileDataReader();
    reader.init("default");
    FileIOEngine engine  = Mockito.mock(FileIOEngine.class);
    Mockito.when(engine.getSegmentById(Mockito.anyInt())).thenReturn(segment);
    Cache cache = Mockito.mock(Cache.class);
    Mockito.when(cache.getName()).thenReturn("default");
    Mockito.when(engine.getOrCreateFileFor(Mockito.anyInt())).thenReturn(file);
    Mockito.when(engine.getFileFor(Mockito.anyInt())).thenReturn(file);
    Mockito.when(engine.getFilePrefetchBufferSize()).thenReturn(1024 * 1024);
    // Must be sealed for scanner
    segment.seal();
    
    verifyBytesWithReader(count, reader, engine);
    
    SegmentScanner scanner = reader.getSegmentScanner(engine, segment);
    verifyScannerFile(scanner, count);
  }
  
}
