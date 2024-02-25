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
package com.onecache.core.index;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.Random;

import org.junit.After;
import org.junit.BeforeClass;

import com.onecache.core.util.TestUtils;
import com.onecache.core.index.IndexFormat;
import com.onecache.core.index.MemoryIndex;
import com.onecache.core.util.UnsafeAccess;
import com.onecache.core.util.Utils;

public class TestMemoryIndexBase {
  MemoryIndex memoryIndex;
  
  int numRecords = 10;
  byte[][] keys;
  byte[][] values;
  long[] mKeys;
  long[] mValues;
  short[] sids;
  int[] offsets;
  int[] sizes;
  long[] expires;
  
  static int keySize = 16;
  static int valueSize = 16;
  
  @BeforeClass
  public static void enableMallocDebug() {
//    UnsafeAccess.setMallocDebugEnabled(true);
//    UnsafeAccess.setMallocDebugStackTraceEnabled(true);
//    UnsafeAccess.setStackTraceRecordingFilter(x -> x == 1024);
//    UnsafeAccess.setStackTraceRecordingLimit(20000);
  }
  
  @After
  public void tearDown() {
    memoryIndex.dispose();
    Arrays.stream(mKeys).forEach(x -> UnsafeAccess.free(x));
    Arrays.stream(mValues).forEach(x -> UnsafeAccess.free(x));
    UnsafeAccess.mallocStats.printStats();
  }
  
  protected void prepareData(int numRecords) {
    this.numRecords = numRecords;
    keys = new byte[numRecords][];
    values = new byte[numRecords][];
    mKeys = new long[numRecords];
    mValues = new long[numRecords];
    sids = new short[numRecords];
    offsets = new int[numRecords];
    sizes = new int[numRecords];
    expires = new long[numRecords];
    
    Random r = new Random();
    long seed = System.currentTimeMillis();
    r.setSeed(seed);
    System.out.println("seed="+ seed);
    
    for (int i = 0; i < numRecords; i++) {
      keys[i] = TestUtils.randomBytes(keySize, r);
      values[i] = TestUtils.randomBytes(valueSize, r);
      mKeys[i] = TestUtils.randomMemory(keySize, r);
      mValues[i] = TestUtils.randomMemory(valueSize, r);
      sids[i] = (short) r.nextInt(32000);
      offsets[i] = nextOffset(r, 100000000);
      sizes[i] = nextSize(keySize, valueSize);
      expires[i] = nextExpire();
    }  
  }
  
  /**
   * Subclasses can overwrite
   * @param r random
   * @param max maximum value
   * @return next offset (for block based formats - must have granularity of a block size)
   */
  int nextOffset(Random r, int max) {
    return r.nextInt(max);
  }
  
  /**
   * Subclasses can overwrite
   * @param r random
   * @param max maximum value
   * @return next size or -1 (size is not supported by format)
   */
  int nextSize(int keySize, int valueSize) {
    IndexFormat format = this.memoryIndex.getIndexFormat();
    return format.isSizeSupported()? Utils.kvSize(keySize, valueSize): -1;
  }
  
  /**
   * Subclasses can override it
   * @return next expiration time
   */
  long nextExpire() {
    return -1;
  }
  
  protected void deleteIndexBytes(int expected) {
    int deleted = 0;
    for(int i = 0; i < numRecords; i++) {
      boolean result = memoryIndex.delete(keys[i], 0, keySize);
      if (result) {
        deleted++;
      }
    }
    assertEquals(expected, deleted);
    assertEquals(0L, memoryIndex.size());
  }
  
  protected void deleteIndexMemory(int expected) {
    int deleted = 0;
    for(int i = 0; i < numRecords; i++) {
      boolean result = memoryIndex.delete(mKeys[i], keySize);
      if (result) {
        deleted++;
      }
    }
    assertEquals(expected, deleted);
    assertEquals(0L, memoryIndex.size());
  }
  
  protected void verifyIndexBytesNot() {
    int entrySize = memoryIndex.getIndexFormat().indexEntrySize();
    long buf = UnsafeAccess.mallocZeroed(entrySize);
    for (int i = 0; i < numRecords; i++) {
      int result = (int) memoryIndex.find(keys[i], 0, keySize, false, buf, entrySize);
      assertEquals(-1, result);
    }
    UnsafeAccess.free(buf);
  }
  
  protected void verifyIndexMemoryNot() {
    int entrySize = memoryIndex.getIndexFormat().indexEntrySize();
    long buf = UnsafeAccess.mallocZeroed(entrySize);
    
    for (int i = 0; i < numRecords; i++) {
      int result = (int) memoryIndex.find(mKeys[i], keySize, false, buf, entrySize);
      assertEquals(-1, result);
    }
    UnsafeAccess.free(buf);
  }
  
  protected boolean sameExpire (long exp, long value) {
    return Math.abs(exp - value) <= 1000;
  }
}
