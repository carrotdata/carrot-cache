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
package com.carrot.cache.io;

import java.util.Arrays;
import java.util.Random;

import org.junit.After;
import org.junit.BeforeClass;

import com.carrot.cache.util.TestUtils;
import com.carrot.cache.util.UnsafeAccess;

public abstract class TestBase {
  
  int numRecords = 10;
  
  byte[][] keys;
  byte[][] values;
  long[] mKeys;
  long[] mValues;
  short[] sids;
  int[] offsets;
  int[] lengths;
  
  int keySize = 16;
  int valueSize = 16;
  
  @BeforeClass
  public static void enableMallocDebug() {
//    UnsafeAccess.setMallocDebugEnabled(true);
//    UnsafeAccess.setMallocDebugStackTraceEnabled(true);
//    UnsafeAccess.setStackTraceRecordingFilter(x -> x == 1024);
//    UnsafeAccess.setStackTraceRecordingLimit(20000);
  }
  
  @After
  public void tearDown() {
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
      keySize = nextKeySize();
      valueSize = nextValueSize();
      keys[i] = TestUtils.randomBytes(keySize, r);
      values[i] = TestUtils.randomBytes(valueSize, r);
      mKeys[i] = TestUtils.randomMemory(keySize, r);
      mValues[i] = TestUtils.randomMemory(valueSize, r);
      sids[i] = (short) r.nextInt(1000);
      offsets[i] = r.nextInt(100000);
      lengths[i] = r.nextInt(10000);
    }  
  }
  
  
  protected abstract int nextKeySize();
  
  protected abstract int nextValueSize();
  
}
