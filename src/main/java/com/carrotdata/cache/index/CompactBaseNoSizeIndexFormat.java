/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */
package com.carrotdata.cache.index;

import com.carrotdata.cache.util.UnsafeAccess;
import com.carrotdata.cache.util.Utils;

/**
 * Index format for main queue (cache) It does not support expiration
 */
public class CompactBaseNoSizeIndexFormat extends AbstractIndexFormat {
  /*
   * MQ Index item is 12 bytes: 6 bytes - hashed key value (high 6 bytes of an 8 byte hash) 6 bytes
   * - location in the storage - ( 2 - segment id, 4 offset in the segment)
   */

  public CompactBaseNoSizeIndexFormat() {
    super();
  }

  @Override
  public final boolean equals(long ptr, long hash) {
    int off = this.hashOffset;
    int v = (int) (UnsafeAccess.toInt(ptr + off) & 0xffffffffL);
    v &= 0x7fffffff;
    off += Utils.SIZEOF_INT;
    short s = (short) (UnsafeAccess.toShort(ptr + off) & 0xffff);
    int vv = (int) (hash >>> 32);
    vv &= 0x7fffffff;
    short ss = (short) (hash >>> 16);
    return v == vv && s == ss;
  }

  @Override
  public int indexEntrySize() {
    return 12;
  }

  @Override
  public final int fullEntrySize(long ptr) {
    return this.indexEntrySize;
  }

  @Override
  public final long advance(long current) {
    return current + indexEntrySize;
  }

  @Override
  public final int getKeyValueSize(long buffer) {
    return -1;
  }

  @Override
  public final int getSegmentId(long buffer) {
    return UnsafeAccess.toShort(buffer + this.sidOffset) & 0xffff;
  }

  @Override
  public final long getOffset(long buffer) {
    return UnsafeAccess.toInt(buffer + this.dataOffsetOffset) & 0xffffffff;
  }

  @Override
  public int getEmbeddedOffset() {
    // TODO
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
  public final int getHitCount(long ptr) {
    int ref = UnsafeAccess.toInt(ptr + this.hashOffset);
    return ref >>> 31;
  }

  @Override
  public final void hit(long ptr) {
    int v = UnsafeAccess.toInt(ptr + this.hashOffset);
    v |= 0x80000000;
    UnsafeAccess.putInt(ptr + this.hashOffset, v);
  }

  @Override
  public final int fullEntrySize(int keySize, int valueSize) {
    return this.indexEntrySize;
  }

  @Override
  public final int getHashBit(long ptr, int n) {
    ptr += this.hashOffset;
    return (int) (UnsafeAccess.toLong(ptr) >>> 64 - n) & 1;
  }

  @Override
  public void writeIndex(long ibPtr, long ptr, byte[] key, int keyOffset, int keySize, byte[] value,
      int valueOffset, int valueSize, int sid, int dataOffset, int dataSize,
      long expire /* not supported here */) {
    long hash = Utils.hash64(key, keyOffset, keySize);
    int v = (int) (hash >>> 32);
    v &= 0x7fffffff;
    ptr += this.hashOffset;
    UnsafeAccess.putInt(ptr, v);
    ptr += Utils.SIZEOF_INT;
    short s = (short) (hash >>> 16);
    UnsafeAccess.putShort(ptr, s);
    ptr += Utils.SIZEOF_SHORT;
    UnsafeAccess.putShort(ptr, (short) sid);
    ptr += Utils.SIZEOF_SHORT;
    UnsafeAccess.putInt(ptr, dataOffset);
  }

  @Override
  public void writeIndex(long ibPtr, long ptr, long keyPtr, int keySize, long valuePtr,
      int valueSize, int sid, int dataOffset, int dataSize, long expire) {
    long hash = Utils.hash64(keyPtr, keySize);
    int v = (int) (hash >>> 32);
    v &= 0x7fffffff;
    ptr += this.hashOffset;
    UnsafeAccess.putInt(ptr, v);
    ptr += Utils.SIZEOF_INT;
    UnsafeAccess.putShort(ptr, (short) (hash >>> 16));
    ptr += Utils.SIZEOF_SHORT;
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
    return 6;
  }

  /**
   * Offsets in index field sections
   * @return offset
   */
  public int dataOffsetOffset() {
    return 8;
  }

  /**
   * Size offset
   * @return offset
   */
  public int sizeOffset() {
    return -1;
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
  public boolean isSizeSupported() {
    return false;
  }

  @Override
  public final void updateIndex(long ptr, int sid, int dataOffset) {
    ptr += this.hashOffset + Utils.SIZEOF_INT + Utils.SIZEOF_SHORT;
    UnsafeAccess.putShort(ptr, (short) (sid & 0xffff));
    ptr += Utils.SIZEOF_SHORT;
    UnsafeAccess.putInt(ptr, dataOffset);

  }
}
