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
package com.carrot.cache.index;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Random;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.carrot.cache.util.TestUtils;
import com.carrot.cache.util.UnsafeAccess;
import com.carrot.cache.util.Utils;

/**
 * Main Queue index format tests
 *
 */
public class TestMQIndexFormat {
  
  static int numRecords = 10;
  static byte[][] keys = new byte[numRecords][];
  static byte[][] values = new byte[numRecords][];
  static long[] mKeys = new long[numRecords];
  static long[] mValues = new long[numRecords];
  static short[] sids = new short[numRecords];
  static int[] offsets = new int[numRecords];
  static int[] lengths = new int[numRecords];
  
  static int keySize = 16;
  static int valueSize = 16;
  static long indexBuffer;
  static long indexBufferMem;
  static long buf;
  static int indexBufferSize;
  static int entrySize;
  static MQIndexFormat format;
  
  @BeforeClass
  public static void setUp() {
    format = new MQIndexFormat();
    entrySize = format.fullEntrySize(keySize, valueSize);
    indexBufferSize = numRecords * entrySize;
    buf = UnsafeAccess.mallocZeroed(entrySize);
    indexBuffer = UnsafeAccess.mallocZeroed(indexBufferSize);
    indexBufferMem = UnsafeAccess.mallocZeroed(indexBufferSize);

    Random r = new Random();
    for (int i = 0; i < numRecords; i++) {
      keys[i] = TestUtils.randomBytes(keySize);
      values[i] = TestUtils.randomBytes(valueSize);
      mKeys[i] = UnsafeAccess.mallocZeroed(keySize);
      mValues[i] = UnsafeAccess.mallocZeroed(valueSize);
      sids[i] = (short)r.nextInt(1000);
      offsets[i] = r.nextInt(10000000);
      lengths[i] = r.nextInt(100000);
      format.writeIndex(indexBuffer + i * entrySize, keys[i], 0, keySize, values[i], 0, valueSize,
        sids[i], offsets[i], lengths[i]);
      format.writeIndex(indexBufferMem + i * entrySize, mKeys[i], keySize, mValues[i], valueSize,
        sids[i], offsets[i], lengths[i]);
    }
    
  }
  
  @AfterClass
  public static void tearDown() {
    
    Arrays.stream(mKeys).forEach(x -> UnsafeAccess.free(x));
    Arrays.stream(mValues).forEach(x -> UnsafeAccess.free(x));
    UnsafeAccess.free(indexBuffer);
    UnsafeAccess.free(indexBufferMem);
    UnsafeAccess.free(buf);
  }
  
  @Test
  public void testIndexWriteByteArrays() {
    long ptr = indexBuffer;
    for (int i = 0; i < numRecords; i++) {
      long hash = Utils.hash8(keys[i], 0, keys[i].length);
      assertTrue(format.equals(ptr, hash));
      
      int size = format.getKeyValueSize(ptr);
      assertEquals(lengths[i], size);
      short sid = (short)format.getSegmentId(ptr);
      assertEquals(sids[i], sid);
      int offset = (int)format.getOffset(ptr);
      assertEquals(offsets[i], offset);
      int count = format.getHitCount(ptr);
      assertEquals(0, count);
      format.hit(ptr);
      count = format.getHitCount(ptr);
      assertEquals(1, count);
      format.hit(ptr);
      count = format.getHitCount(ptr);
      assertEquals(1, count);   
      ptr += entrySize;
    }
    
  }
  
  @Test
  public void testIndexWriteMemory() {
    
    long ptr = indexBufferMem;
    for (int i = 0; i < numRecords; i++) {
      long hash = Utils.hash8(mKeys[i], keySize);
      assertTrue(format.equals(ptr, hash));
      int size = format.getKeyValueSize(ptr);
      assertEquals(lengths[i], size);
      short sid = (short)format.getSegmentId(ptr);
      assertEquals(sids[i], sid);
      int offset = (int)format.getOffset(ptr);
      assertEquals(offsets[i], offset);
      int count = format.getHitCount(ptr);
      assertEquals(0, count);
      format.hit(ptr);
      count = format.getHitCount(ptr);
      assertEquals(1, count);
      format.hit(ptr);
      count = format.getHitCount(ptr);
      assertEquals(1, count);   
      ptr += entrySize;
    }
  }
  
}
