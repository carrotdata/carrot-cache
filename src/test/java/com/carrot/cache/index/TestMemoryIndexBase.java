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

import org.junit.After;
import org.junit.BeforeClass;

import com.carrot.cache.util.TestUtils;
import com.carrot.cache.util.UnsafeAccess;

public class TestMemoryIndexBase {
  MemoryIndex memoryIndex;
  
  int numRecords = 10;
  byte[][] keys = new byte[numRecords][];
  byte[][] values = new byte[numRecords][];
  long[] mKeys = new long[numRecords];
  long[] mValues = new long[numRecords];
  short[] sids = new short[numRecords];
  int[] offsets = new int[numRecords];
  int[] lengths = new int[numRecords];
  
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
    lengths = new int[numRecords];
    
    Random r = new Random();
    long seed = System.currentTimeMillis();
    r.setSeed(seed);
    System.out.println("seed="+ seed);
    
    for (int i = 0; i < numRecords; i++) {
      keys[i] = TestUtils.randomBytes(keySize, r);
      values[i] = TestUtils.randomBytes(valueSize, r);
      mKeys[i] = TestUtils.randomMemory(keySize, r);
      mValues[i] = TestUtils.randomMemory(valueSize, r);
      sids[i] = (short) r.nextInt(1000);
      offsets[i] = r.nextInt(100000);
      lengths[i] = r.nextInt(10000);
    }  
  }
  
  
  protected void deleteIndexBytes() {
    for(int i = 0; i < numRecords; i++) {
      boolean result = memoryIndex.delete(keys[i], 0, keySize);
      assertTrue(result);
    }
    assertEquals(0L, memoryIndex.size());
  }
  
  protected void deleteIndexMemory() {
    for(int i = 0; i < numRecords; i++) {
      boolean result = memoryIndex.delete(mKeys[i], keySize);
      assertTrue(result);
    }
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
}
