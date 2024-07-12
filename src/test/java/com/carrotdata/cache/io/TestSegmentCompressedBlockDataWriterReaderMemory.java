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

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;

import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Test dictionary off/ on, compressible/non-compressible data
 */
public class TestSegmentCompressedBlockDataWriterReaderMemory extends IOCompressionTestBase {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestSegmentCompressedBlockDataWriterReaderMemory.class);

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

  @Test
  public void testWritesBytesDictionaryNotRandom() throws IOException, URISyntaxException {
    initTestForSegment(false, true);
    testWritesBytes();
  }

  private void testWritesBytes() throws IOException {
    int count = loadBytes();
    /* DEBUG */ LOG.info("count=" + count);
    long expire = expires[count - 1];
    assertEquals(expire, segment.getInfo().getMaxExpireAt());
    long t1 = System.nanoTime();
    verifyBytes(count);
    long t2 = System.nanoTime();
    /* DEBUG */ LOG.info("verified bytes in " + (t2 - t1) / 1000 + " micros");
    DataReader reader = new CompressedBlockMemoryDataReader();
    reader.init(cacheName);
    IOEngine engine = Mockito.mock(IOEngine.class);
    Mockito.when(engine.getSegmentById(Mockito.anyInt())).thenReturn(segment);
    t1 = System.nanoTime();
    verifyBytesWithReader(count, reader, engine);
    t2 = System.nanoTime();
    /* DEBUG */ LOG.info("verified bytes reader in " + (t2 - t1) / 1000 + " micros");

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
    /* DEBUG */ LOG.info("verified memory in " + (t2 - t1) / 1000 + " micros");
    DataReader reader = new CompressedBlockMemoryDataReader();
    reader.init(cacheName);
    IOEngine engine = Mockito.mock(IOEngine.class);
    Mockito.when(engine.getSegmentById(Mockito.anyInt())).thenReturn(segment);
    t1 = System.nanoTime();
    verifyMemoryWithReader(count, reader, engine);
    t2 = System.nanoTime();
    /* DEBUG */ LOG.info("verified memory reader in " + (t2 - t1) / 1000 + " micros");

    verifyMemoryWithReaderByteBuffer(count, reader, engine);

  }

  @Test
  public void testSegmentScanner() throws IOException, URISyntaxException {
    initTestForSegment(false, false);
    int count = loadBytes();
    DataReader reader = new CompressedBlockMemoryDataReader();
    reader.init(cacheName);
    IOEngine engine = Mockito.mock(IOEngine.class);
    Mockito.when(engine.getSegmentById(Mockito.anyInt())).thenReturn(segment);
    // Seal the segment
    segment.seal();
    SegmentScanner scanner = reader.getSegmentScanner(engine, segment);
    long t1 = System.nanoTime();
    verifyScanner(scanner, count);
    long t2 = System.nanoTime();
    /* DEBUG */ LOG.info("verified scanner in " + (t2 - t1) / 1000 + " micros");
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
    seg.setDataWriterAndEngine(writer, null);
    segment.dispose();
    segment = seg;
    verifyBytes(count);
    DataReader reader = new CompressedBlockMemoryDataReader();
    reader.init(cacheName);
    IOEngine engine = Mockito.mock(IOEngine.class);
    Mockito.when(engine.getSegmentById(Mockito.anyInt())).thenReturn(segment);
    verifyBytesWithReader(count, reader, engine);
  }

  @Test
  public void testSegmentScannerWithDictionary() throws IOException, URISyntaxException {
    initTestForSegment(false, true);
    int count = loadBytes();
    DataReader reader = new CompressedBlockMemoryDataReader();
    reader.init(cacheName);
    IOEngine engine = Mockito.mock(IOEngine.class);
    Mockito.when(engine.getSegmentById(Mockito.anyInt())).thenReturn(segment);
    // Seal the segment
    segment.seal();
    SegmentScanner scanner = reader.getSegmentScanner(engine, segment);
    long t1 = System.nanoTime();
    verifyScanner(scanner, count);
    long t2 = System.nanoTime();
    /* DEBUG */ LOG.info("verified scanner in " + (t2 - t1) / 1000 + " micros");
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
    seg.setDataWriterAndEngine(writer, null);
    segment.dispose();
    segment = seg;
    verifyBytes(count);
    DataReader reader = new CompressedBlockMemoryDataReader();
    reader.init(cacheName);
    IOEngine engine = Mockito.mock(IOEngine.class);
    Mockito.when(engine.getSegmentById(Mockito.anyInt())).thenReturn(segment);
    verifyBytesWithReader(count, reader, engine);
  }
}