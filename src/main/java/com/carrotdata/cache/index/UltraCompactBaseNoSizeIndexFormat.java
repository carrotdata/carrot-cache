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
 * Compact index format takes only 8 bytes per cached entry: 
 * 3 bytes:  keep 24 bits of a hashed key 8 byte value starting with a bit L + 1,
 * L is defined in configuration file as 'index.slots.power', default value is 10.
 * Memory index hash table size = 2**L (lowest 0 bit is used as 1 - bit counter)
 * 12 its - segment id (max number of segments is 4096) 
 * 28 bits - data offset in a segment ( max offset - 256M - its the max segment size as well)
 */
public class UltraCompactBaseNoSizeIndexFormat extends AbstractIndexFormat {

  int L; // index.slots.power from configuration
  
  public UltraCompactBaseNoSizeIndexFormat() {
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
    v1 &= ~1;
    hash = ((hash >>> (64 - L - 24)) & 0xffffff);
    hash &= ~1;
    return v1 == hash;
  }

  @Override
  public int indexEntrySize() {
    return 4 * Utils.SIZEOF_SHORT;
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
    // we use only high 12 bits
    //TODO: TEST
    return (int) (UnsafeAccess.toShort(buffer + off) & 0xffff) >>> 4;
  }

  @Override
  public final long getOffset(long buffer) {
    int off = this.sidOffset + 1;
    // Use only lower 28 bits
    return UnsafeAccess.toInt(buffer + off) & 0xfffffff;
  }

  @Override
  public int getIndexBlockHeaderSize() {
    return 3 * Utils.SIZEOF_SHORT;
  }

  public final int getHitCount(long ptr) {
    int off = this.hashOffset;
    int v1 = UnsafeAccess.toInt(ptr + off) & 0xffffffff;
    // Use 9 th bit - 256
    return (v1 & 256) >>> 8;
  }

  @Override
  public final void hit(long ptr) {
    int off = this.hashOffset;
    int v = UnsafeAccess.toInt(ptr + off) & 0xffffffff;
    v |= 256;
    UnsafeAccess.putInt(ptr + off, v);
  }

  @Override
  public int getEmbeddedOffset() {
    return Utils.SIZEOF_SHORT;
  }

  @Override
  public final int getHashBit(long ptr, int n) {
    int off = this.hashOffset;
    return ((UnsafeAccess.toInt(ptr + off)) >>> (32 - n + L)) & 1;
  }

  @Override
  public void writeIndex(long ibPtr, long ptr, byte[] key, int keyOffset, int keySize, byte[] value,
      int valueOffset, int valueSize, int sid, int dataOffset, int dataSize, long expire) {
    long hash = Utils.hash64(key, keyOffset, keySize);
    int $hash =  (int) (hash >>> 64 - L - 32);
    // set hit bit to 0
    $hash &= ~256;
    UnsafeAccess.putInt(ptr + hashOffset(), $hash);
    UnsafeAccess.putShort(ptr + sidOffset(), (short) (sid << 4));
    // lowest 4 bites of sid
    int sid4 = sid << 28;
    // Set highest 4 bits to 0
    dataOffset &= 0xfffffff;
    dataOffset |= sid4;
    UnsafeAccess.putInt(ptr + sidOffset() + 1, dataOffset);
  }

  @Override
  public void writeIndex(long ibPtr, long ptr, long keyPtr, int keySize, long valuePtr,
      int valueSize, int sid, int dataOffset, int dataSize, long expire) {
    long hash = Utils.hash64(keyPtr, keySize);
    int $hash =  (int) (hash >>> 64 - L - 32);
    // set hit bit to 0
    $hash &= ~256;
    UnsafeAccess.putInt(ptr + hashOffset(), $hash);
    UnsafeAccess.putShort(ptr + sidOffset(), (short) (sid << 4));
    // lowest 4 bites of sid
    int sid4 = sid << 28;
    // Set highest 4 bits to 0
    dataOffset &= 0xfffffff;
    dataOffset |= sid4;
    UnsafeAccess.putInt(ptr + sidOffset() + 1, dataOffset);
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
    // We are not using it directly
    return sidOffset() + 1;
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
    UnsafeAccess.putShort(ptr + sidOffset(), (short) (sid << 4));
    // lowest 4 bites of sid
    int sid4 = sid << 28;
    // Set highest 4 bits to 0
    dataOffset &= 0xfffffff;
    dataOffset |= sid4;
    UnsafeAccess.putInt(ptr + sidOffset() + 1, dataOffset);
  }
}
