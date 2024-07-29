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

/**
 * Format of an index entry (10 bytes): expire - 2 bytes ,hash - 3 bytes sid - 12 bits, offset - 28 bits
 * bytes
 */
public final class UltraCompactBaseNoSizeWithExpireIndexFormat
    extends UltraCompactBaseNoSizeIndexFormat {

  public UltraCompactBaseNoSizeWithExpireIndexFormat() {
    super();
    this.superIndexBlockHeaderSize = super.getIndexBlockHeaderSize();
  }

  @Override
  public boolean isExpirationSupported() {
    return true;
  }

  @Override
  public long getExpire(long ibPtr, long ptr) {
    ibPtr += this.superIndexBlockHeaderSize;
    ptr += this.expireOffset;
    return this.expireSupport.getExpire(ibPtr, ptr);
  }

  @Override
  public boolean begin(long ibPtr, boolean force) {
    ibPtr += this.superIndexBlockHeaderSize;
    return this.expireSupport.begin(ibPtr, force);
  }

  @Override
  public void end(long ibPtr) {
    ibPtr += this.superIndexBlockHeaderSize;
    this.expireSupport.end(ibPtr);
  }

  @Override
  public int getIndexBlockHeaderSize() {
    return super.getIndexBlockHeaderSize()
        + (this.expireSupport != null ? this.expireSupport.metaSectionSize : 0);
  }

  @Override
  public int getEmbeddedOffset() {
    return super.getEmbeddedOffset()
        + (this.expireSupport != null ? this.expireSupport.fieldSize : 0);
  }

  @Override
  public int indexEntrySize() {
    return super.indexEntrySize() + (this.expireSupport != null ? this.expireSupport.fieldSize : 0);
  }

  @Override
  public void writeIndex(long ibPtr, long ptr, byte[] key, int keyOffset, int keySize, byte[] value,
      int valueOffset, int valueSize, int sid, int dataOffset, int dataSize, long expire) {
    super.writeIndex(ibPtr, ptr, key, keyOffset, keySize, value, valueOffset, valueSize, sid,
      dataOffset, dataSize, expire);
    ibPtr += this.superIndexBlockHeaderSize;
    this.expireSupport.setExpire(ibPtr, ptr + expireOffset(), expire);

  }

  @Override
  public void writeIndex(long ibPtr, long ptr, long keyPtr, int keySize, long valuePtr,
      int valueSize, int sid, int dataOffset, int dataSize, long expire) {
    super.writeIndex(ibPtr, ptr, keyPtr, keySize, valuePtr, valueSize, sid, dataOffset, dataSize,
      expire);
    ibPtr += this.superIndexBlockHeaderSize;
    this.expireSupport.setExpire(ibPtr, ptr + expireOffset(), expire);
  }

  @Override
  public int hashOffset() {
    return super.hashOffset() + (this.expireSupport != null ? this.expireSupport.fieldSize : 0);
  }

  @Override
  public int sidOffset() {
    return super.sidOffset() + (this.expireSupport != null ? this.expireSupport.fieldSize : 0);
  }

  @Override
  public int dataOffsetOffset() {
    return super.dataOffsetOffset()
        + (this.expireSupport != null ? this.expireSupport.fieldSize : 0);
  }

  @Override
  public int expireOffset() {
    return 0;
  }

  @Override
  public long getAndSetExpire(long ibPtr, long expPtr, long expire) {
    long oldExpire = getExpire(ibPtr, expPtr);
    ibPtr += this.superIndexBlockHeaderSize;
    expPtr += this.expireOffset;
    expireSupport.setExpire(ibPtr, expPtr, expire);
    return oldExpire;
  }
}
