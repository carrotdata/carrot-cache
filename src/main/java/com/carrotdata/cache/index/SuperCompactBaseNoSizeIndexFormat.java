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
package com.carrotdata.cache.index;

import com.carrotdata.cache.util.CacheConfig;
import com.carrotdata.cache.util.UnsafeAccess;
import com.carrotdata.cache.util.Utils;

/**
 * Compact index format takes only 9 bytes per cached entry: 
 * 3 bytes:  keep 24 bits of a hashed key 8 byte value starting with a bit L + 1,
 * L is defined in configuration file as 'index.slots.power', default value is 10.
 * Memory index hash table size = 2**L
 * 2 bytes - segment id (lowest 15 bits only, bit 15 is used for 1-bit hit counter) 
 * 4 - data offset in a segment
 */
public class SuperCompactBaseNoSizeIndexFormat extends AbstractIndexFormat {

  int L; // index.slots.power from configuration

  public SuperCompactBaseNoSizeIndexFormat() {
    super();
  }

  /**
   * Cache name for this index format
   * @param cacheName cache name
   */
  public void setCacheName(String cacheName) {
    super.setCacheName(cacheName);
    CacheConfig config = CacheConfig.getInstance();
    this.L = config.getStartIndexNumberOfSlotsPower(cacheName);
  }

  @Override
  public final boolean equals(long ptr, long hash) {
    int off = this.hashOffset;
    int v1 = UnsafeAccess.toInt(ptr + off) & 0xffffffff;
    v1 >>>= 8;
    hash = ((hash >>> (64 - L - 24)) & 0xffffff);
    
    return v1 == hash;
  }

  @Override
  public int indexEntrySize() {
    return 4 * Utils.SIZEOF_SHORT + Utils.SIZEOF_BYTE;
  }

  @Override
  public final int fullEntrySize(long ptr) {
    return this.indexEntrySize;
  }

  @Override
  public final int fullEntrySize(int keySize, int valueSize) {
    return this.indexEntrySize;
  }

  @Override
  public final long advance(long current) {
    return current + this.indexEntrySize;
  }

  @Override
  public int getKeyValueSize(long buffer) {
    // this index format does not store k-v size
    return -1;
  }

  @Override
  public final int getSegmentId(long buffer) {
    int off = this.sidOffset;
    return UnsafeAccess.toShort(buffer + off) & 0x7fff;
  }

  @Override
  public final long getOffset(long buffer) {
    int off = this.dataOffsetOffset;
    return UnsafeAccess.toInt(buffer + off);
  }

  @Override
  public int getIndexBlockHeaderSize() {
    return 3 * Utils.SIZEOF_SHORT;
  }

  public final int getHitCount(long buffer) {
    int off = this.sidOffset;
    return (UnsafeAccess.toShort(buffer + off) & 0x8000) >>> 15;
  }

  @Override
  public final void hit(long ptr) {
    int off = this.sidOffset;
    int v = UnsafeAccess.toShort(ptr + off) & 0xffff;
    v |= 0x8000;
    UnsafeAccess.putShort(ptr + off, (short) v);
  }

  @Override
  public int getEmbeddedOffset() {
    return Utils.SIZEOF_SHORT;
  }

  @Override
  public final int getHashBit(long ptr, int n) {
    int off = this.hashOffset;
    // TODO:test
    return ((UnsafeAccess.toInt(ptr + off)) >>> (32 - n + L)) & 1;
  }

  @Override
  public void writeIndex(long ibPtr, long ptr, byte[] key, int keyOffset, int keySize, byte[] value,
      int valueOffset, int valueSize, int sid, int dataOffset, int dataSize, long expire) {
    long hash = Utils.hash64(key, keyOffset, keySize);
    int $hash =  (int) (hash >>> 64 - L - 32);
    UnsafeAccess.putInt(ptr + hashOffset(), $hash);
    UnsafeAccess.putShort(ptr + sidOffset(), (short) sid);
    UnsafeAccess.putInt(ptr + dataOffsetOffset(), dataOffset);
  }

  @Override
  public void writeIndex(long ibPtr, long ptr, long keyPtr, int keySize, long valuePtr,
      int valueSize, int sid, int dataOffset, int dataSize, long expire) {
    long hash = Utils.hash64(keyPtr, keySize);
    int $hash =  (int) (hash >>> 64 - L - 32);
    UnsafeAccess.putInt(ptr + hashOffset(), $hash);
    UnsafeAccess.putShort(ptr + sidOffset(), (short) sid);
    UnsafeAccess.putInt(ptr + dataOffsetOffset(), dataOffset);
  }

  /**
   * Offsets in index field sections
   * @return offset
   */
  public int hashOffset() {
    return 0;
  }

  /**
   * Offsets in index field sections
   * @return offset
   */
  public int sidOffset() {
    return Utils.SIZEOF_SHORT + Utils.SIZEOF_BYTE;
  }

  /**
   * Offsets in index field sections
   * @return offset
   */
  public int dataOffsetOffset() {
    return 2 * Utils.SIZEOF_SHORT + Utils.SIZEOF_BYTE;
  }

  /**
   * Offsets in index field sections
   * @return offset
   */
  public int expireOffset() {
    // Not supported
    return -1;
  }

  @Override
  public final boolean isSizeSupported() {
    return false;
  }

  @Override
  public int sizeOffset() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public final void updateIndex(long ptr, int sid, int dataOffset) {
    UnsafeAccess.putShort(ptr + sidOffset(), (short) (sid & 0xffff));
    UnsafeAccess.putInt(ptr + dataOffsetOffset(), dataOffset);
  }
}
