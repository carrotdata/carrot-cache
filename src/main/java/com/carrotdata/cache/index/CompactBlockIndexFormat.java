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
package com.carrotdata.cache.index;

import com.carrotdata.cache.util.CacheConfig;
import com.carrotdata.cache.util.UnsafeAccess;
import com.carrotdata.cache.util.Utils;

/**
 * Compact index format takes only 8 bytes per cached entry: First 4 bytes: Bit 31th is used to keep
 * "hit count" (only 1 hit can be recorded) Bits 30 - 0 keep 31 bits of a hashed key 8 byte value
 * starting with a bit L L is defined in configuration file as 'index.slots.power', default value is
 * 10 Memory index hash table size = 2**L 2 bytes - segment id 2 - bytes data block number which
 * "potentially" stores this key-value real offset is data block * block size. With 64K blocks
 * addressed (2 bytes) and 4K block size We can address maximum 64K * 4K = 256MB size segments.
 */
public class CompactBlockIndexFormat extends AbstractIndexFormat {

  int blockSize;
  int L; // index.slots.power from configuration

  public CompactBlockIndexFormat() {
    super();
  }

  /**
   * Cache name for this index format
   * @param cacheName
   */
  public void setCacheName(String cacheName) {
    super.setCacheName(cacheName);
    CacheConfig config = CacheConfig.getInstance();
    this.blockSize = config.getBlockWriterBlockSize(cacheName);
    this.L = config.getStartIndexNumberOfSlotsPower(cacheName);
  }

  @Override
  public final boolean equals(long ptr, long hash) {
    int off = this.hashOffset;
    int v = UnsafeAccess.toInt(ptr + off);
    v &= 0x7fffffff;
    hash = hash >>> (32 - L + 1);
    hash &= 0x7fffffff;
    return v == hash;
  }

  @Override
  public int indexEntrySize() {
    return Utils.SIZEOF_INT + 2 * Utils.SIZEOF_SHORT;
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
  public final int getKeyValueSize(long buffer) {
    // this index format does not store k-v size
    return -1;
  }

  @Override
  public final int getSegmentId(long buffer) {
    int off = this.sidOffset;
    return UnsafeAccess.toShort(buffer + off) & 0xffff;
  }

  @Override
  public final long getOffset(long buffer) {
    int off = this.dataOffsetOffset;
    int blockNumber = UnsafeAccess.toShort(buffer + off) & 0xffff;
    return blockNumber * this.blockSize;
  }

  @Override
  public int getIndexBlockHeaderSize() {
    return 3 * Utils.SIZEOF_SHORT;
  }

  public final int getHitCount(long ptr) {
    int off = this.hashOffset;
    int ref = UnsafeAccess.toInt(ptr + off);
    return ref >>> 31;
  }

  @Override
  public final void hit(long ptr) {
    int off = this.hashOffset;
    int v = UnsafeAccess.toInt(ptr + off);
    v |= 0x80000000;
    UnsafeAccess.putInt(ptr + off, v);
  }

  @Override
  public int getEmbeddedOffset() {
    return Utils.SIZEOF_INT;
  }

  @Override
  public final int getHashBit(long ptr, int n) {
    int off = this.hashOffset;
    // TODO:test
    return (UnsafeAccess.toInt(ptr + off) >>> 32 - n + L - 1) & 1;
  }

  @Override
  public void writeIndex(long ibPtr, long ptr, byte[] key, int keyOffset, int keySize, byte[] value,
      int valueOffset, int valueSize, int sid, int dataOffset, int dataSize, long expire) {
    long hash = Utils.hash64(key, keyOffset, keySize);
    int $hash = (int) (hash >>> 32 - L + 1) & 0x7fffffff;
    UnsafeAccess.putInt(ptr + this.hashOffset, $hash);
    UnsafeAccess.putShort(ptr + this.sidOffset, (short) (sid & 0xffff));
    UnsafeAccess.putShort(ptr + this.dataOffsetOffset,
      (short) ((dataOffset / this.blockSize) & 0xffff));
  }

  @Override
  public void writeIndex(long ibPtr, long ptr, long keyPtr, int keySize, long valuePtr,
      int valueSize, int sid, int dataOffset, int dataSize, long expire) {
    long hash = Utils.hash64(keyPtr, keySize);
    int $hash = (int) (hash >>> 32 - L + 1) & 0x7fffffff;
    UnsafeAccess.putInt(ptr + this.hashOffset, $hash);
    UnsafeAccess.putShort(ptr + this.sidOffset, (short) (sid & 0xffff));
    UnsafeAccess.putShort(ptr + this.dataOffsetOffset,
      (short) ((dataOffset / this.blockSize) & 0xffff));
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
    return Utils.SIZEOF_INT;
  }

  /**
   * Offsets in index field sections
   * @return offset
   */
  public int dataOffsetOffset() {
    return Utils.SIZEOF_INT + Utils.SIZEOF_SHORT;
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
    return 0;
  }

  @Override
  public final void updateIndex(long ptr, int sid, int dataOffset) {
    UnsafeAccess.putShort(ptr + this.sidOffset, (short) (sid & 0xffff));
    UnsafeAccess.putShort(ptr + this.dataOffsetOffset,
      (short) ((dataOffset / this.blockSize) & 0xffff));
  }
}
