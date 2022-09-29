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
package com.carrot.cache.index;

import com.carrot.cache.util.CarrotConfig;
import com.carrot.cache.util.UnsafeAccess;
import com.carrot.cache.util.Utils;

/**
 * 
 * Index format for main queue (cache)
 * It does not support expiration
 * 
 */
public class SubCompactBaseIndexFormat implements IndexFormat {
  int  L; // index.slots.power from configuration
  
  /**
   * Cache name for this index format
   * @param cacheName
   */
  public void setCacheName(String cacheName) {
    CarrotConfig config = CarrotConfig.getInstance();
    this.L = config.getStartIndexNumberOfSlotsPower(cacheName);
  }
  /*
   * MQ Index item is 16 bytes:
   * 4 bytes - hashed key value (high 6 bytes of an 8 byte hash)
   * 4 bytes - total item size (key + value) - first bit is hit count
   * 6 bytes - location in the storage - ( 2 - segment id, 4 offset in the segment) 
   */
  public SubCompactBaseIndexFormat() {
  }
  
  @Override
  public boolean equals(long ptr, long hash) {
    int off = hashOffset();
    int v = UnsafeAccess.toInt(ptr + off);
    hash = (int) (hash >>> (32 - L));
    return v == hash;
  }

  @Override
  public int indexEntrySize() {
    return 14;
  }

  @Override
  public int fullEntrySize(long ptr) {
    return indexEntrySize();
  }
  
  @Override
  public long advance(long current) {
    return current + fullEntrySize(current);
  }

  @Override
  public int getKeyValueSize(long buffer) {
    int off = sizeOffset();
    return UnsafeAccess.toInt(buffer + off) & 0x7fffffff;
  }

  @Override
  public int getSegmentId(long buffer) {
    int off = sidOffset();
    return UnsafeAccess.toShort(buffer + off) & 0xffff;
  }

  @Override
  public long getOffset(long buffer) {
    int off = dataOffsetOffset();
    return UnsafeAccess.toInt(buffer + off) & 0xffffffff;
  }

  @Override
  public int getEmbeddedOffset() {
    //TODO
    return 0; 
  }
  
  @Override
  public long getExpire(long ibPtr, long buffer) {
    // Does not support expiration
    return -1;
  }

  @Override
  public int getIndexBlockHeaderSize() {
    return 3 * Utils.SIZEOF_SHORT;
  }

  @Override
  public int getHitCount(long buffer) {
    int off = sizeOffset();
    int ref = UnsafeAccess.toInt(buffer + off);
    return  ref >>> 31;   
  }

  @Override
  public void hit(long ptr) {
    ptr += sizeOffset();
    int v = UnsafeAccess.toInt(ptr);
    v |= 0x80000000;
    UnsafeAccess.putInt(ptr, v);
  }

  @Override
  public int fullEntrySize(int keySize, int valueSize) {
    return indexEntrySize();
  }

  @Override
  public int getHashBit(long ptr, int n) {
    int off = hashOffset();
    // TODO:test
    return (UnsafeAccess.toInt(ptr + off) >>> 32 - n + L) & 1;
  }
  
  @Override
  public void writeIndex(
      long ibPtr,
      long ptr,
      byte[] key,
      int keyOffset,
      int keySize,
      byte[] value,
      int valueOffset,
      int valueSize,
      int sid,
      int dataOffset,
      int dataSize,
      long expire /* not supported here*/) 
  {
    long hash = Utils.hash64(key, keyOffset, keySize);
    int $hash = (int)(hash >>> 32 - L);

    ptr += hashOffset();
    UnsafeAccess.putInt(ptr, $hash);
    ptr += Utils.SIZEOF_INT; 
    UnsafeAccess.putInt(ptr, dataSize);
    ptr += Utils.SIZEOF_INT;
    UnsafeAccess.putShort(ptr, (short)sid);
    ptr += Utils.SIZEOF_SHORT; 
    UnsafeAccess.putInt(ptr, dataOffset);
  }

  @Override
  public void writeIndex(
      long ibPtr,
      long ptr,
      long keyPtr,
      int keySize,
      long valuePtr,
      int valueSize,
      int sid,
      int dataOffset,
      int dataSize,
      long expire) 
  {
    long hash = Utils.hash64(keyPtr, keySize);
    int $hash = (int)(hash >>> 32 - L);

    ptr += hashOffset();
    UnsafeAccess.putInt(ptr, $hash);
    ptr += Utils.SIZEOF_INT; 
    UnsafeAccess.putInt(ptr, dataSize);
    ptr += Utils.SIZEOF_INT;
    UnsafeAccess.putShort(ptr, (short)sid);
    ptr += Utils.SIZEOF_SHORT; 
    UnsafeAccess.putInt(ptr, dataOffset);
  }
  
  /**
   * Offsets in index field sections
   * @return offset
   */
  int hashOffset() {
    return 0;
  }
  /**
   * Offsets in index field sections
   * @return offset
   */
  int sidOffset() {
    return 8;
  }
  /**
   * Offsets in index field sections
   * @return offset
   */
  int dataOffsetOffset() {
    return 10;
  }
  
  /**
   * Size offset
   * @return offset
   */
  int sizeOffset() {
    return 4;
  }
  
  /**
   * Offsets in index field sections
   * @return offset
   */
  int expireOffset() {
    // Not supported
    return -1;
  }
}
