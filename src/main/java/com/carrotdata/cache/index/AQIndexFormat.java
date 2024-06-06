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

import com.carrotdata.cache.util.UnsafeAccess;
import com.carrotdata.cache.util.Utils;

public final class AQIndexFormat extends AbstractIndexFormat {
  /*
   * AQ Index item is 8 bytes hashed key value
   */
  public AQIndexFormat() {
    super();
  }

  @Override
  public boolean equals(long ptr, long hash) {
    return UnsafeAccess.toLong(ptr) == hash;
  }

  @Override
  public int indexEntrySize() {
    return Utils.SIZEOF_LONG;
  }

  @Override
  public int fullEntrySize(long ptr) {
    return Utils.SIZEOF_LONG;
  }

  @Override
  public long advance(long current) {
    return current + Utils.SIZEOF_LONG;
  }

  @Override
  public int getKeyValueSize(long buffer) {
    // Does not support
    return 0;
  }

  @Override
  public int getSegmentId(long buffer) {
    // Does not support
    return 0;
  }

  @Override
  public long getOffset(long buffer) {
    // Does not support
    return 0;
  }

  @Override
  public long getExpire(long ibPtr, long buffer) {
    // Does not support
    return 0;
  }

  @Override
  public int getIndexBlockHeaderSize() {
    return 3 * Utils.SIZEOF_SHORT;
  }

  @Override
  public int getHitCount(long buffer) {
    // Does not support
    return 0;
  }

  @Override
  public void hit(long ptr) {
    // do nothing
  }

  @Override
  public int fullEntrySize(int keySize, int valueSize) {
    return Utils.SIZEOF_LONG;
  }

  @Override
  public void writeIndex(long ibPtr, long ptr, byte[] key, int keyOffset, int keySize, byte[] value,
      int valueOffset, int valueSize, int sid, int dataOffset, int dataSize, long expire) {
    long hash = Utils.hash64(key, keyOffset, keySize);
    UnsafeAccess.putLong(ptr, hash);
  }

  @Override
  public void writeIndex(long ibPtr, long ptr, long keyPtr, int keySize, long valuePtr,
      int valueSize, int sid, int dataOffset, int dataSize, long expire) {
    long hash = Utils.hash64(keyPtr, keySize);
    UnsafeAccess.putLong(ptr, hash);
  }

  @Override
  public boolean isSizeSupported() {
    return false;
  }

  @Override
  public int hashOffset() {
    return 0;
  }

  @Override
  public int sidOffset() {
    return 0;
  }

  @Override
  public int dataOffsetOffset() {
    return 0;
  }

  @Override
  public int expireOffset() {
    return 0;
  }

  @Override
  public int sizeOffset() {
    return 0;
  }

  @Override
  public void updateIndex(long ptr, int sid, int dataOffset) {
    // Do nothing
  }

}
