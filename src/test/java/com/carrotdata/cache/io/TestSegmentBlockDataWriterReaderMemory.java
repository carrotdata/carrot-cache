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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotdata.cache.Cache;
import com.carrotdata.cache.index.MemoryIndex;
import com.carrotdata.cache.index.MemoryIndex.Type;
import com.carrotdata.cache.io.Segment.Info;
import com.carrotdata.cache.util.UnsafeAccess;

public class TestSegmentBlockDataWriterReaderMemory extends IOTestBase {
  private static final Logger LOG =
      LoggerFactory.getLogger(TestSegmentBlockDataWriterReaderMemory.class);

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

    DataReader reader = new BlockMemoryDataReader();
    reader.init("default");
    IOEngine engine = Mockito.mock(IOEngine.class);
    Mockito.when(engine.getSegmentById(Mockito.anyInt())).thenReturn(segment);
    verifyBytesWithReader(count, reader, engine);
    verifyBytesWithReaderByteBuffer(count, reader, engine);

  }

  @Test
  public void testWritesMemory() throws IOException {
    int count = loadMemory();
    long expire = expires[count - 1];
    assertEquals(expire, segment.getInfo().getMaxExpireAt());
    verifyMemoryBlock(count, blockSize);

    DataReader reader = new BlockMemoryDataReader();
    reader.init("default");
    IOEngine engine = Mockito.mock(IOEngine.class);
    Mockito.when(engine.getSegmentById(Mockito.anyInt())).thenReturn(segment);
    verifyMemoryWithReader(count, reader, engine);
    verifyMemoryWithReaderByteBuffer(count, reader, engine);

  }

  @Test
  public void testSegmentScanner() throws IOException {
    int count = loadBytes();

    verifyBytesBlock(count, blockSize);

    DataReader reader = new BlockMemoryDataReader();
    reader.init("default");
    IOEngine engine = Mockito.mock(IOEngine.class);
    Mockito.when(engine.getSegmentById(Mockito.anyInt())).thenReturn(segment);
    Cache cache = Mockito.mock(Cache.class);
    Mockito.when(cache.getName()).thenReturn("default");
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
    seg.setDataWriterAndEngine(new BlockDataWriter(), null);
    segment.dispose();
    segment = seg;
    verifyBytesBlock(count, blockSize);

    DataReader reader = new BlockMemoryDataReader();
    reader.init("default");
    IOEngine engine = Mockito.mock(IOEngine.class);
    Mockito.when(engine.getSegmentById(Mockito.anyInt())).thenReturn(segment);
    verifyBytesWithReader(count, reader, engine);
  }
}
