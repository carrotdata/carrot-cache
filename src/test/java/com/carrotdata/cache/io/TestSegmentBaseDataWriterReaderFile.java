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

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Random;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotdata.cache.index.MemoryIndex;
import com.carrotdata.cache.index.MemoryIndex.Type;
import com.carrotdata.cache.util.TestUtils;
import com.carrotdata.cache.util.UnsafeAccess;

public class TestSegmentBaseDataWriterReaderFile extends IOTestBase {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestSegmentBaseDataWriterReaderFile.class);

  @Before
  public void setUp() {
    this.index = new MemoryIndex("default", Type.MQ);
    this.segmentSize = 4 * 1024 * 1024;
    this.numRecords = 10000;
    this.r = new Random();
    long seed = System.currentTimeMillis();
    r.setSeed(seed);
    /* DEBUG */ LOG.info("r.seed=" + seed);
    long ptr = UnsafeAccess.mallocZeroed(this.segmentSize);
    segment = Segment.newSegment(ptr, this.segmentSize, 1, 1);
    segment.init("default");
    prepareRandomData(this.numRecords);
    FileIOEngine engine = Mockito.mock(FileIOEngine.class);
    DataWriter writer = new BaseDataWriter();
    Mockito.when(engine.getWriteBatches()).thenReturn(new WriteBatches(writer));
    segment.setDataWriterAndEngine(new BaseDataWriter(), engine);
  }

  @After
  public void tearDown() throws IOException {
    super.tearDown();
    this.segment.dispose();
  }

  @Test
  public void testWritesBytes() throws IOException {
    int count = loadBytes();
    verifyBytes(count);

    RandomAccessFile file = TestUtils.saveToFile(segment);

    DataReader reader = new BaseFileDataReader();
    FileIOEngine engine = Mockito.mock(FileIOEngine.class);
    Mockito.when(engine.getSegmentById(Mockito.anyInt())).thenReturn(segment);
    Mockito.when(engine.getFileFor(Mockito.anyInt())).thenReturn(file);
    verifyBytesWithReader(count, reader, engine);
    verifyBytesWithReaderByteBuffer(count, reader, engine);

  }

  @Test
  public void testWritesMemory() throws IOException {
    int count = loadMemory();
    verifyMemory(count);

    RandomAccessFile file = TestUtils.saveToFile(segment);

    DataReader reader = new BaseFileDataReader();
    FileIOEngine engine = Mockito.mock(FileIOEngine.class);
    Mockito.when(engine.getSegmentById(Mockito.anyInt())).thenReturn(segment);
    Mockito.when(engine.getFileFor(Mockito.anyInt())).thenReturn(file);
    verifyMemoryWithReader(count, reader, engine);
    verifyMemoryWithReaderByteBuffer(count, reader, engine);

  }

  @Test
  public void testSegmentScanner() throws IOException {
    int count = loadBytes();
    RandomAccessFile file = TestUtils.saveToFile(segment);

    DataReader reader = new BaseFileDataReader();
    FileIOEngine engine = Mockito.mock(FileIOEngine.class);
    Mockito.when(engine.getSegmentById(Mockito.anyInt())).thenReturn(segment);
    Mockito.when(engine.getFileFor(Mockito.anyInt())).thenReturn(file);
    Mockito.when(engine.getFilePrefetchBufferSize()).thenReturn(1024 * 1024);
    // Seal the segment
    segment.seal();

    SegmentScanner scanner = reader.getSegmentScanner(engine, segment);
    verifyScannerFile(scanner, count);
  }

}
