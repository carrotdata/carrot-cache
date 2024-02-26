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
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Random;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.onecache.core.util.TestUtils;
import com.onecache.core.util.UnsafeAccess;
import com.onecache.core.util.Utils;

/**
 * Index format test base
 *
 */
public abstract class TestIndexFormatBase {
  
 int numRecords = 1;
 byte[][] keys;
 byte[][] values;
 long[] mKeys;
 long[] mValues;
 short[] sids;
 int[] offsets;
 int[] lengths;
 long[] expire;
 int keySize = 16;
 int valueSize = 16;
 long indexBuffer;
 long indexBufferMem;
 long buf;
 int indexBufferSize;
 int entrySize;
 IndexFormat format;
  
  @Before
  public void setUp() {
    
    this.format = getIndexFormat();
    int headerSize = this.format.getIndexBlockHeaderSize();
    this.numRecords = getNumberOfRecords();
    boolean expireSupported = this.format.isExpirationSupported();
    boolean sizeSupported = this.format.isSizeSupported();
    keys = new byte[numRecords][];
    values = new byte[numRecords][];
    mKeys = new long[numRecords];
    mValues = new long[numRecords];
    sids = new short[numRecords];
    offsets = new int[numRecords];
    lengths = new int[numRecords];
    expire = new long[numRecords];
    
    this.entrySize = this.format.fullEntrySize(this.keySize, this.valueSize);
    this.indexBufferSize = headerSize + this.numRecords * this.entrySize;
    this.buf = UnsafeAccess.mallocZeroed(this.entrySize);
    this.indexBuffer = UnsafeAccess.mallocZeroed(this.indexBufferSize);
    this.indexBufferMem = UnsafeAccess.mallocZeroed(this.indexBufferSize);

    Random r = new Random();
    for (int i = 0; i < this.numRecords; i++) {
      this.keys[i] = TestUtils.randomBytes(this.keySize);
      this.values[i] = TestUtils.randomBytes(this.valueSize);
      this.mKeys[i] = TestUtils.randomMemory(this.keySize);
      this.mValues[i] = TestUtils.randomMemory(this.valueSize);
      this.sids[i] = (short)r.nextInt(1000);
      
      this.offsets[i] = getDataOffset(r, 10000000);
      
      this.lengths[i] = sizeSupported? r.nextInt(100000): -1;
      this.expire[i] = expireSupported? System.currentTimeMillis() + i: -1;
      this.format.writeIndex(this.indexBuffer, this.indexBuffer + headerSize + i * this.entrySize, 
        this.keys[i], 0, this.keySize, this.values[i], 0, this.valueSize,
        this.sids[i], this.offsets[i], this.lengths[i], this.expire[i]);
      this.format.writeIndex(this.indexBufferMem, this.indexBufferMem + headerSize + 
        i * this.entrySize, this.mKeys[i], this.keySize, this.mValues[i], this.valueSize,
        this.sids[i], this.offsets[i], this.lengths[i], this.expire[i]);
    }
  }
  
  @After
  public void tearDown() {
    Arrays.stream(this.mKeys).forEach(x -> UnsafeAccess.free(x));
    Arrays.stream(this.mValues).forEach(x -> UnsafeAccess.free(x));
    UnsafeAccess.free(this.indexBuffer);
    UnsafeAccess.free(this.indexBufferMem);
    UnsafeAccess.free(this.buf);
  }
  
  @Test
  public void testIndexWriteByteArrays() {
    int headerSize = this.format.getIndexBlockHeaderSize();
    long ptr = indexBuffer + headerSize;
    boolean expireSupported = this.format.isExpirationSupported();
    for (int i = 0; i < numRecords; i++) {
      long hash = Utils.hash64(keys[i], 0, keys[i].length);
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
      long expire = format.getExpire(this.indexBuffer, ptr);
      long expExpire = this.expire[i];
      if (expireSupported) {
        expire = expire / 1000 * 1000;
        expExpire = expExpire / 1000 * 1000;
      }
      assertTrue(Math.abs(expExpire - expire) <= 1000);
      ptr += entrySize;
    }
  }
  
  @Test
  public void testIndexWriteMemory() {
    int headerSize = this.format.getIndexBlockHeaderSize();
    long ptr = indexBufferMem + headerSize;
    boolean expireSupported = this.format.isExpirationSupported();

    for (int i = 0; i < numRecords; i++) {
      long hash = Utils.hash64(mKeys[i], keySize);
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
      long expire = format.getExpire(this.indexBufferMem, ptr);
      long expExpire = this.expire[i];
      if (expireSupported) {
        expire = expire / 1000 * 1000;
        expExpire = expExpire / 1000 * 1000;
      }
      assertTrue(Math.abs(expExpire - expire) <= 1000);

      ptr += entrySize;
    }
  }
  
  protected abstract IndexFormat getIndexFormat();
  
  protected int getNumberOfRecords() {
    return 100;
  }
  
  protected int getDataOffset(Random r, int max) {
    return r.nextInt(max);
  }
}
