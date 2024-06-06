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

import static com.carrotdata.cache.io.BlockReaderWriterSupport.META_SIZE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Random;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotdata.cache.util.TestUtils;
import com.carrotdata.cache.util.UnsafeAccess;
import com.carrotdata.cache.util.Utils;

public class TestPrefetchBuffer extends IOTestBase {
  private static final Logger LOG = LoggerFactory.getLogger(TestPrefetchBuffer.class);

  @Before
  public void setUp() {
    this.segmentSize = 4 * 1024 * 1024;
    long ptr = UnsafeAccess.mallocZeroed(this.segmentSize);
    this.segment = Segment.newSegment(ptr, segmentSize, 1, 1);
    this.segment.init("default");
    this.numRecords = 10000;
    this.r = new Random();
    long seed = System.currentTimeMillis();
    r.setSeed(seed);
    LOG.info("r.seed=" + seed);
    prepareRandomData(this.numRecords);
  }

  @After
  public void tearDown() throws IOException {
    super.tearDown();
    segment.dispose();
  }

  @Test
  public void testPrefetchBufferWithBaseWriter() throws IOException {
    segment.setDataWriterAndEngine(new BaseDataWriter(), null);
    int n = loadBytes();
    RandomAccessFile raf = TestUtils.saveToFile(segment);
    PrefetchBuffer pbuf = new PrefetchBuffer(raf, 256 * 1024);
    int count = 0;
    while (count < n) {
      byte[] key = keys[count];
      byte[] value = values[count];
      int kSize = pbuf.keyLength();
      int vSize = pbuf.valueLength();
      assertEquals(key.length, kSize);
      assertEquals(value.length, vSize);
      int kSizeSize = Utils.sizeUVInt(kSize);
      int vSizeSize = Utils.sizeUVInt(vSize);
      int fullSize = Utils.kvSize(kSize, vSize);
      pbuf.ensure(fullSize); // TODO - move this code to PrefetchBuffer
      byte[] buf = pbuf.getBuffer();
      int off = pbuf.getBufferOffset();
      assertTrue(Utils.compareTo(buf, off + kSizeSize + vSizeSize, kSize, key, 0, kSize) == 0);
      assertTrue(
        Utils.compareTo(buf, off + kSizeSize + vSizeSize + kSize, vSize, value, 0, vSize) == 0);
      boolean result = pbuf.next();
      assertTrue(result);
      count++;
    }
    raf.close();
  }

  @Test
  public void testPrefetchBufferWithBlockWriter() throws IOException {

    BlockDataWriter writer = new BlockDataWriter();
    int blockSize = 4096;
    writer.setBlockSize(blockSize);
    segment.setDataWriterAndEngine(writer, null);
    int n = loadBytes();
    verifyBytesBlock(n, blockSize);
    RandomAccessFile raf = TestUtils.saveToFile(segment);
    PrefetchBuffer pbuf = new PrefetchBuffer(raf, 256 * 1024 + 24);
    byte[] buffer = pbuf.getBuffer();

    for (int i = 0; i < n; i++) {
      int blockDataSize = UnsafeAccess.toInt(buffer, pbuf.getBufferOffset());
      pbuf.skip(META_SIZE);
      int count = 0;
      int scanned = 0;
      while (scanned < blockDataSize) {
        byte[] key = keys[count + i];
        byte[] value = values[count + i];
        int kSize = pbuf.keyLength();
        int vSize = pbuf.valueLength();
        assertEquals(key.length, kSize);
        assertEquals(value.length, vSize);
        int kSizeSize = Utils.sizeUVInt(kSize);
        int vSizeSize = Utils.sizeUVInt(vSize);
        int fullSize = Utils.kvSize(kSize, vSize);
        pbuf.ensure(fullSize); // TODO - move this code to PrefetchBuffer
        int off = pbuf.getBufferOffset();
        assertTrue(Utils.compareTo(buffer, off + kSizeSize + vSizeSize, kSize, key, 0, kSize) == 0);
        assertTrue(Utils.compareTo(buffer, off + kSizeSize + vSizeSize + kSize, vSize, value, 0,
          vSize) == 0);
        boolean result = pbuf.next();
        assertTrue(result);
        count++;
        scanned += fullSize;
      }
      i += count - 1;
      pbuf.skip(
        ((blockDataSize + META_SIZE - 1) / blockSize + 1) * blockSize - scanned - META_SIZE);
    }

    raf.close();
  }
}
