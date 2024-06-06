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
import java.util.Random;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotdata.cache.Cache;
import com.carrotdata.cache.index.MemoryIndex;
import com.carrotdata.cache.index.MemoryIndex.Type;
import com.carrotdata.cache.util.TestUtils;
import com.carrotdata.cache.util.UnsafeAccess;

public class TestSegmentBlockDataWriterReaderFile extends IOTestBase {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestSegmentBlockDataWriterReaderFile.class);

  @Before
  public void setUp() {
    this.index = new MemoryIndex("default", Type.MQ);
    this.segmentSize = 15 * 1024 * 1024;
    this.numRecords = 10000;

    this.r = new Random();
    long seed = System.currentTimeMillis();
    r.setSeed(seed);

    LOG.info("r.seed=" + seed);
    long ptr = UnsafeAccess.mallocZeroed(this.segmentSize);
    segment = Segment.newSegment(ptr, this.segmentSize, 1, 1);
    segment.init("default");
    prepareRandomData(this.numRecords);
    BlockDataWriter bdw = new BlockDataWriter();
    bdw.setBlockSize(blockSize);
    segment.setDataWriterAndEngine(bdw, null);
  }

  @After
  public void tearDown() throws IOException {
    super.tearDown();
    this.segment.dispose();
    UnsafeAccess.mallocStats.printStats();

  }

  @Test
  public void testWritesBytes() throws IOException {
    int count = loadBytes();
    long expire = expires[count - 1];
    assertEquals(expire, segment.getInfo().getMaxExpireAt());
    verifyBytesBlock(count, blockSize);

    RandomAccessFile file = TestUtils.saveToFile(segment);

    DataReader reader = new BlockFileDataReader();
    reader.init("default");
    FileIOEngine engine = Mockito.mock(FileIOEngine.class);
    Mockito.when(engine.getSegmentById(Mockito.anyInt())).thenReturn(segment);
    Mockito.when(engine.getFileFor(Mockito.anyInt())).thenReturn(file);
    verifyBytesWithReader(count, reader, engine);
    verifyBytesWithReaderByteBuffer(count, reader, engine);

  }

  @Test
  public void testWritesMemory() throws IOException {
    int count = loadMemory();
    long expire = expires[count - 1];
    assertEquals(expire, segment.getInfo().getMaxExpireAt());
    verifyMemoryBlock(count, blockSize);

    RandomAccessFile file = TestUtils.saveToFile(segment);

    DataReader reader = new BlockFileDataReader();
    reader.init("default");
    FileIOEngine engine = Mockito.mock(FileIOEngine.class);
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
