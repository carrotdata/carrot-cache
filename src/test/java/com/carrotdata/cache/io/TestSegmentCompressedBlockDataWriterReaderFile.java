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

import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.URISyntaxException;

import org.junit.Test;
import org.mockito.Mockito;

import com.carrotdata.cache.Cache;
import com.carrotdata.cache.util.TestUtils;

public class TestSegmentCompressedBlockDataWriterReaderFile extends IOCompressionTestBase {

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
    long expire = expires[count - 1];
    assertEquals(expire, segment.getInfo().getMaxExpireAt());

    RandomAccessFile file = TestUtils.saveToFile(segment);

    DataReader reader = new CompressedBlockFileDataReader();
    reader.init(cacheName);
    FileIOEngine engine = Mockito.mock(FileIOEngine.class);
    Mockito.when(engine.getSegmentById(Mockito.anyInt())).thenReturn(segment);
    Mockito.when(engine.getFileFor(Mockito.anyInt())).thenReturn(file);
    verifyBytesWithReader(count, reader, engine);
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

    RandomAccessFile file = TestUtils.saveToFile(segment);

    DataReader reader = new CompressedBlockFileDataReader();
    reader.init(cacheName);
    FileIOEngine engine = Mockito.mock(FileIOEngine.class);
    Mockito.when(engine.getSegmentById(Mockito.anyInt())).thenReturn(segment);
    Mockito.when(engine.getFileFor(Mockito.anyInt())).thenReturn(file);
    verifyMemoryWithReader(count, reader, engine);
    verifyMemoryWithReaderByteBuffer(count, reader, engine);

  }

  @Test
  public void testSegmentScanner() throws IOException, URISyntaxException {
    initTestForSegment(false, false);
    int count = loadBytes();
    RandomAccessFile file = TestUtils.saveToFile(segment);
    DataReader reader = new CompressedBlockFileDataReader();
    reader.init(cacheName);
    FileIOEngine engine = Mockito.mock(FileIOEngine.class);
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

  @Test
  public void testSegmentScannerWithDictionary() throws IOException, URISyntaxException {
    initTestForSegment(false, true);
    int count = loadBytes();
    RandomAccessFile file = TestUtils.saveToFile(segment);
    DataReader reader = new CompressedBlockFileDataReader();
    reader.init(cacheName);
    FileIOEngine engine = Mockito.mock(FileIOEngine.class);
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
