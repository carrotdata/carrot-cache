/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
