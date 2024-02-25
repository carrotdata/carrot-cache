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

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;

import org.junit.Test;
import org.mockito.Mockito;

import com.onecache.core.io.CompressedBlockDataWriter;
import com.onecache.core.io.CompressedBlockMemoryDataReader;
import com.onecache.core.io.DataReader;
import com.onecache.core.io.DataWriter;
import com.onecache.core.io.IOEngine;
import com.onecache.core.io.Segment;
import com.onecache.core.io.SegmentScanner;

/**
 * 
 * Test dictionary off/ on, compressible/non-compressible data
 *
 */
public class TestSegmentCompressedBlockDataWriterReaderMemory extends IOCompressionTestBase{
  @Test
  public void testWritesBytesNoDictionaryNotRandom() throws IOException, URISyntaxException {
    initTestForSegment(false, false);
    testWritesBytes();
  }
 
  @Test
  public void testWritesBytesNoDictionaryRandom() throws IOException, URISyntaxException {
    initTestForSegment(true, false);
    testWritesBytes();
  }
  
  @Test
  public void testWritesBytesDictionaryRandom() throws IOException, URISyntaxException {
    initTestForSegment(true, true);
    testWritesBytes();
  }
  
  //@Ignore
  @Test
  public void testWritesBytesDictionaryNotRandom() throws IOException, URISyntaxException {
    initTestForSegment(false, true);
    testWritesBytes();
  }
  
  private void testWritesBytes() throws IOException {
    int count = loadBytes();
    /*DEBUG*/ System.out.println("count=" + count);
    long expire = expires[count - 1];
    assertEquals(expire, segment.getInfo().getMaxExpireAt());
    long t1 = System.nanoTime();
    verifyBytes(count);
    long t2 = System.nanoTime();
    /*DEBUG*/ System.out.println("verified bytes in " + (t2-t1)/1000+ " micros");
    DataReader reader = new CompressedBlockMemoryDataReader();
    reader.init(cacheName);
    IOEngine engine  = Mockito.mock(IOEngine.class);
    Mockito.when(engine.getSegmentById(Mockito.anyInt())).thenReturn(segment);
    t1 = System.nanoTime();
    verifyBytesWithReader(count, reader, engine);
    t2 = System.nanoTime();
    /*DEBUG*/ System.out.println("verified bytes reader in " + (t2-t1)/1000+ " micros");

    verifyBytesWithReaderByteBuffer(count, reader, engine);
  }
  

  @Test
  public void testWritesMemoryNoDictionaryNotRandom() throws IOException, URISyntaxException {
    initTestForSegment(false, false);
    testWritesMemory();
  }

  @Test
  public void testWritesMemoryNoDictionaryRandom() throws IOException, URISyntaxException {
    initTestForSegment(true, false);
    testWritesMemory();
  }
  
  @Test
  public void testWritesMemoryDictionaryRandom() throws IOException, URISyntaxException {
    initTestForSegment(true, true);
    testWritesMemory();
  }
  
  @Test
  public void testWritesMemoryDictionaryNotRandom() throws IOException, URISyntaxException {
    initTestForSegment(false, true);
    testWritesMemory();
  }
  
  private void testWritesMemory() throws IOException {
    int count = loadMemory(); 
    long expire = expires[count - 1];
    assertEquals(expire, segment.getInfo().getMaxExpireAt());
    long t1 = System.nanoTime();
    verifyMemory(count);
    long t2 = System.nanoTime();
    /*DEBUG*/ System.out.println("verified memory in " + (t2 -t1)/1000 + " micros");
    DataReader reader = new CompressedBlockMemoryDataReader();
    reader.init(cacheName);
    IOEngine engine  = Mockito.mock(IOEngine.class);
    Mockito.when(engine.getSegmentById(Mockito.anyInt())).thenReturn(segment);
    t1 = System.nanoTime();
    verifyMemoryWithReader(count, reader, engine);
    t2 = System.nanoTime();
    /*DEBUG*/ System.out.println("verified memory reader in " + (t2 -t1)/1000 + " micros");

    verifyMemoryWithReaderByteBuffer(count, reader, engine);

  }
  
  @Test
  public void testSegmentScanner() throws IOException, URISyntaxException {
    initTestForSegment(false, false);
    int count = loadBytes();
    DataReader reader = new CompressedBlockMemoryDataReader();
    reader.init(cacheName);
    IOEngine engine  = Mockito.mock(IOEngine.class);
    Mockito.when(engine.getSegmentById(Mockito.anyInt())).thenReturn(segment);
    // Seal the segment
    segment.seal();
    SegmentScanner scanner = reader.getSegmentScanner(engine, segment);
    long t1 = System.nanoTime();
    verifyScanner(scanner, count);
    long t2 = System.nanoTime();
    /*DEBUG*/ System.out.println("verified scanner in " + (t2-t1)/1000 + " micros");
  }
  
  @Test
  public void testSaveLoad() throws IOException, URISyntaxException {
    initTestForSegment(false, false);

    int count = loadBytes();
    verifyBytes(count);

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
  
  
  @Test
  public void testSegmentScannerWithDictionary() throws IOException, URISyntaxException {
    initTestForSegment(false, true);
    int count = loadBytes();
    DataReader reader = new CompressedBlockMemoryDataReader();
    reader.init(cacheName);
    IOEngine engine  = Mockito.mock(IOEngine.class);
    Mockito.when(engine.getSegmentById(Mockito.anyInt())).thenReturn(segment);
    // Seal the segment
    segment.seal();
    SegmentScanner scanner = reader.getSegmentScanner(engine, segment);
    long t1 = System.nanoTime();
    verifyScanner(scanner, count);
    long t2 = System.nanoTime();
    /*DEBUG*/ System.out.println("verified scanner in " + (t2-t1)/1000 + " micros");
  }
  
  @Test
  public void testSaveLoadWithDictionary() throws IOException, URISyntaxException {
    initTestForSegment(false, true);

    int count = loadBytes();
    verifyBytes(count);

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
