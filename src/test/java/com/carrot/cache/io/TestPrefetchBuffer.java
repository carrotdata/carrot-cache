/**
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
package com.carrot.cache.io;

import static com.carrot.cache.io.BlockReaderWriterSupport.META_SIZE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Random;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.carrot.cache.util.TestUtils;
import com.carrot.cache.util.UnsafeAccess;
import com.carrot.cache.util.Utils;

public class TestPrefetchBuffer extends IOTestBase {
  
  @Before
  public void setUp() {
    this.segmentSize = 4 * 1024 * 1024;
    this.segment = Segment.newSegment(segmentSize, 1, 1);
    this.numRecords = 10000;
    this.r = new Random();
    long seed = System.currentTimeMillis();
    r.setSeed(seed);
    System.out.println("r.seed=" + seed);
    prepareData(this.numRecords);
  }
  
  @After
  public void tearDown() {
    super.tearDown();
    segment.dispose();
  }
    
  @Test
  public void testPrefetchBufferWithBaseWriter() throws IOException {
    segment.setDataWriter(new BaseDataWriter());
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
      assertTrue(Utils.compareTo(buf, off + kSizeSize + vSizeSize + kSize, vSize, value, 0, vSize) == 0);
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
    segment.setDataWriter(writer);
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
      while(scanned < blockDataSize) {
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
        assertTrue(Utils.compareTo(buffer, off + kSizeSize + vSizeSize + kSize, vSize, value, 0, vSize) == 0);
        boolean result = pbuf.next();
        assertTrue(result);
        count++;
        scanned += fullSize;
      }
      i += count - 1;
      pbuf.skip(((blockDataSize + META_SIZE - 1)/blockSize + 1) * blockSize - scanned - META_SIZE);
    }
    
    raf.close();
  }
}
