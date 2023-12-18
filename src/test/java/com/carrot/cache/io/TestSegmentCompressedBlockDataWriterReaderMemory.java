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
import java.net.URISyntaxException;
import java.util.Random;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.carrot.cache.index.MemoryIndex;
import com.carrot.cache.index.MemoryIndex.Type;
import com.carrot.cache.util.CarrotConfig;

/**
 * 
 * Test dictionary off/ on, compressible/non-compressible data
 *
 */
public class TestSegmentCompressedBlockDataWriterReaderMemory extends IOTestBase{
  
  @Before
  public void setUp() throws URISyntaxException, IOException {
    this.index = new MemoryIndex(cacheName, Type.MQ);
    this.segmentSize = 8 * 1024 * 1024;
    this.numRecords = 10000;
    this.r = new Random();
    this.segment = Segment.newSegment(this.segmentSize, 1, 1);
    this.segment.init(cacheName);
  }

  private void initTest(boolean randomData, boolean dictionaryEnabled) throws URISyntaxException, IOException {
    if (randomData) {
      prepareRandomData(numRecords);
    } else {
      prepareGithubData(numRecords);
    }
    if (!dictionaryEnabled) {
      CarrotConfig config = CarrotConfig.getInstance();
      config.setCacheCompressionDictionaryEnabled(cacheName, dictionaryEnabled);
    }
    CompressedBlockDataWriter writer = new CompressedBlockDataWriter();
    writer.init(cacheName);
    this.segment.setDataWriter(writer);
  }
  
  @After
  public void tearDown() throws IOException {
    super.tearDown();
    this.segment.dispose();
  }
  
  @Test
  public void testWritesBytesNoDictionaryNotRandom() throws IOException, URISyntaxException {
    initTest(false, false);
    testWritesBytes();
  }
 
  @Test
  public void testWritesBytesNoDictionaryRandom() throws IOException, URISyntaxException {
    initTest(true, false);
    testWritesBytes();
  }
  
  @Test
  public void testWritesBytesDictionaryRandom() throws IOException, URISyntaxException {
    initTest(true, true);
    testWritesBytes();
  }
  
  @Test
  public void testWritesBytesDictionaryNotRandom() throws IOException, URISyntaxException {
    initTest(false, true);
    testWritesBytes();
  }
  
  private void testWritesBytes() throws IOException {
    int count = loadBytes();
    long expire = expires[count - 1];
    assertEquals(expire, segment.getInfo().getMaxExpireAt());
    verifyBytes(count);
    
    DataReader reader = new CompressedBlockMemoryDataReader();
    reader.init(cacheName);
    IOEngine engine  = Mockito.mock(IOEngine.class);
    Mockito.when(engine.getSegmentById(Mockito.anyInt())).thenReturn(segment);
    verifyBytesWithReader(count, reader, engine);
    verifyBytesWithReaderByteBuffer(count, reader, engine);
  }
  
  @Test
  public void testWritesMemoryNoDictionaryNotRandom() throws IOException, URISyntaxException {
    initTest(false, false);
    testWritesMemory();
  }
 
  @Test
  public void testWritesMemoryNoDictionaryRandom() throws IOException, URISyntaxException {
    initTest(true, false);
    testWritesMemory();
  }
  
  @Test
  public void testWritesMemoryDictionaryRandom() throws IOException, URISyntaxException {
    initTest(true, true);
    testWritesMemory();
  }
  
  @Test
  public void testWritesMemoryDictionaryNotRandom() throws IOException, URISyntaxException {
    initTest(false, true);
    testWritesMemory();
  }
  
  private void testWritesMemory() throws IOException {
    int count = loadMemory(); 
    long expire = expires[count - 1];
    assertEquals(expire, segment.getInfo().getMaxExpireAt());
    verifyMemory(count);
    
    DataReader reader = new CompressedBlockMemoryDataReader();
    reader.init(cacheName);
    IOEngine engine  = Mockito.mock(IOEngine.class);
    Mockito.when(engine.getSegmentById(Mockito.anyInt())).thenReturn(segment);
    verifyMemoryWithReader(count, reader, engine);
    verifyMemoryWithReaderByteBuffer(count, reader, engine);

  }
  
  @Test
  public void testSegmentScanner() throws IOException, URISyntaxException {
    initTest(false, false);
    int count = loadBytes();
    DataReader reader = new CompressedBlockMemoryDataReader();
    reader.init(cacheName);
    IOEngine engine  = Mockito.mock(IOEngine.class);
    Mockito.when(engine.getSegmentById(Mockito.anyInt())).thenReturn(segment);
    // Seal the segment
    segment.seal();
    SegmentScanner scanner = reader.getSegmentScanner(engine, segment);
    verifyScanner(scanner, count);
  }
  
  @Test
  public void testSaveLoad() throws IOException, URISyntaxException {
    initTest(false, false);

    int count = loadBytes();
 
    // now save Info and Segment separately
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    segment.save(dos);
    // Now load back
    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    DataInputStream dis = new DataInputStream(bais);
    Segment seg = new Segment();
    seg.load(dis);
    DataWriter writer = new CompressedBlockDataWriter();
    writer.init(cacheName);
    seg.setDataWriter(writer);
    segment.dispose();
    segment = seg;
    verifyBytes(count);
    DataReader reader = new CompressedBlockMemoryDataReader();
    reader.init(cacheName);
    IOEngine engine  = Mockito.mock(IOEngine.class);
    Mockito.when(engine.getSegmentById(Mockito.anyInt())).thenReturn(segment);
    verifyBytesWithReader(count, reader, engine);
  }
  
}
