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
 * Index format for main queue (cache) It does not support expiration
 */
public class SubCompactBaseIndexFormat extends AbstractIndexFormat {
  int L; // index.slots.power from configuration

  /*
   * MQ Index item is 14 bytes: 4 bytes - hashed key value (high 6 bytes of an 8 byte hash) 4 bytes
   * - total item size (key + value) - first bit is hit count 6 bytes - location in the storage - (
   * 2 - segment id, 4 offset in the segment)
   */
  public SubCompactBaseIndexFormat() {
    super();
  }

  /**
   * Cache name for this index format
   * @param cacheName
   */
  public void setCacheName(String cacheName) {
    super.setCacheName(cacheName);
    CacheConfig config = CacheConfig.getInstance();
    this.L = config.getStartIndexNumberOfSlotsPower(cacheName);
  }

  @Override
  public final boolean equals(long ptr, long hash) {
    int off = this.hashOffset;
    int v = UnsafeAccess.toInt(ptr + off);
    hash = (int) (hash >>> (32 - L));
    return v == hash;
  }

  @Override
  public int indexEntrySize() {
    return 14;
  }

  @Override
  public final int fullEntrySize(long ptr) {
    return this.indexEntrySize;
  }

  @Override
  public final long advance(long current) {
    return current + this.indexEntrySize;
  }

  @Override
  public int getKeyValueSize(long buffer) {
    int off = this.sizeOffset;
    return UnsafeAccess.toInt(buffer + off) & 0x7fffffff;
  }

  @Override
  public final int getSegmentId(long buffer) {
    int off = this.sidOffset;
    return UnsafeAccess.toShort(buffer + off) & 0xffff;
  }

  @Override
  public final long getOffset(long buffer) {
    int off = this.dataOffsetOffset;
    return UnsafeAccess.toInt(buffer + off) & 0xffffffff;
  }

  @Override
  public int getEmbeddedOffset() {
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
  public final int getHitCount(long buffer) {
    int off = this.sizeOffset;
    int ref = UnsafeAccess.toInt(buffer + off);
    return ref >>> 31;
  }

  @Override
  public final void hit(long ptr) {
    ptr += this.sizeOffset;
    int v = UnsafeAccess.toInt(ptr);
    v |= 0x80000000;
    UnsafeAccess.putInt(ptr, v);
  }

  @Override
  public int fullEntrySize(int keySize, int valueSize) {
    return this.indexEntrySize;
  }

  @Override
  public final int getHashBit(long ptr, int n) {
    int off = this.hashOffset;
    // TODO:test
    return (UnsafeAccess.toInt(ptr + off) >>> 32 - n + L) & 1;
  }

  @Override
  public void writeIndex(long ibPtr, long ptr, byte[] key, int keyOffset, int keySize, byte[] value,
      int valueOffset, int valueSize, int sid, int dataOffset, int dataSize,
      long expire /* not supported here */) {
    long hash = Utils.hash64(key, keyOffset, keySize);
    int $hash = (int) (hash >>> 32 - L);

    ptr += this.hashOffset;
    UnsafeAccess.putInt(ptr, $hash);
    ptr += Utils.SIZEOF_INT;
    UnsafeAccess.putInt(ptr, dataSize);
    ptr += Utils.SIZEOF_INT;
    UnsafeAccess.putShort(ptr, (short) sid);
    ptr += Utils.SIZEOF_SHORT;
    UnsafeAccess.putInt(ptr, dataOffset);
  }

  @Override
  public void writeIndex(long ibPtr, long ptr, long keyPtr, int keySize, long valuePtr,
      int valueSize, int sid, int dataOffset, int dataSize, long expire) {
    long hash = Utils.hash64(keyPtr, keySize);
    int $hash = (int) (hash >>> 32 - L);

    ptr += this.hashOffset;
    UnsafeAccess.putInt(ptr, $hash);
    ptr += Utils.SIZEOF_INT;
    UnsafeAccess.putInt(ptr, dataSize);
    ptr += Utils.SIZEOF_INT;
    UnsafeAccess.putShort(ptr, (short) sid);
    ptr += Utils.SIZEOF_SHORT;
    UnsafeAccess.putInt(ptr, dataOffset);
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
    return 8;
  }

  /**
   * Offsets in index field sections
   * @return offset
   */
  public int dataOffsetOffset() {
    return 10;
  }

  /**
   * Size offset
   * @return offset
   */
  public int sizeOffset() {
    return 4;
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
  public final void updateIndex(long ptr, int sid, int dataOffset) {
    ptr += this.hashOffset + 2 * Utils.SIZEOF_INT;
    UnsafeAccess.putShort(ptr, (short) (sid & 0xffff));
    ptr += Utils.SIZEOF_SHORT;
    UnsafeAccess.putInt(ptr, dataOffset);
  }

}