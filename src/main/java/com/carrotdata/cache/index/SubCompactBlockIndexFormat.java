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
 * Compact index format takes only 6 bytes per cached entry: First 2 bytes: Bits 15 - 0 keep 16 bits
 * of a hashed key 8 byte value starting with a bit L + 1 L is defined in configuration file as
 * 'index.slots.power', default value is 10 Memory index hash table size = 2**L 2 bytes - segment id
 * (lowest 15 bits only, bit 15 is used for 1-bit hit counter) 2 - bytes data block number which
 * "potentially" stores this key-value real offset is data block * block size. With 64K blocks
 * addressed and 4K block size We can address maximum 64K * 4K = 256MB size segments. With 32K
 * maximum segments (15 bits) Total maximum cache size supported is 32K * 256MB = 8TB.
 */
public class SubCompactBlockIndexFormat extends AbstractIndexFormat {

  int blockSize;
  int L; // index.slots.power from configuration

  public SubCompactBlockIndexFormat() {
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
    int v = UnsafeAccess.toShort(ptr + off) & 0xffff;
    hash = ((hash >>> (64 - L - 16)) & 0xffff);
    return v == hash;
  }

  @Override
  public int indexEntrySize() {
    return 3 * Utils.SIZEOF_SHORT;
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
    int blockNumber = UnsafeAccess.toShort(buffer + off) & 0xffff;
    return blockNumber * this.blockSize;
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
    return ((UnsafeAccess.toShort(ptr + off) & 0xffff) >>> (16 - n + L)) & 1;
  }

  @Override
  public void writeIndex(long ibPtr, long ptr, byte[] key, int keyOffset, int keySize, byte[] value,
      int valueOffset, int valueSize, int sid, int dataOffset, int dataSize, long expire) {
    long hash = Utils.hash64(key, keyOffset, keySize);
    short $hash = (short) (hash >>> 64 - L - 16);
    UnsafeAccess.putShort(ptr + hashOffset(), $hash);
    UnsafeAccess.putShort(ptr + sidOffset(), (short) sid);
    UnsafeAccess.putShort(ptr + dataOffsetOffset(), (short) (dataOffset / this.blockSize));
  }

  @Override
  public void writeIndex(long ibPtr, long ptr, long keyPtr, int keySize, long valuePtr,
      int valueSize, int sid, int dataOffset, int dataSize, long expire) {
    long hash = Utils.hash64(keyPtr, keySize);
    short $hash = (short) (hash >>> 64 - L - 16);
    UnsafeAccess.putShort(ptr + hashOffset(), $hash);
    UnsafeAccess.putShort(ptr + sidOffset(), (short) sid);
    UnsafeAccess.putShort(ptr + dataOffsetOffset(), (short) (dataOffset / this.blockSize));
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
    return Utils.SIZEOF_SHORT;
  }

  /**
   * Offsets in index field sections
   * @return offset
   */
  public int dataOffsetOffset() {
    return 2 * Utils.SIZEOF_SHORT;
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
    UnsafeAccess.putShort(ptr + dataOffsetOffset(), (short) (dataOffset / this.blockSize));
  }
}
