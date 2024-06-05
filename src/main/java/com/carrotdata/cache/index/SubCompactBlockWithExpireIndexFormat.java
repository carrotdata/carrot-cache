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

/**
 * Format of an index entry (8 bytes): expire - 2 bytes hash - 2 bytes sid - 2 bytes offset - 2
 * bytes
 */
public final class SubCompactBlockWithExpireIndexFormat extends SubCompactBlockIndexFormat {

  public SubCompactBlockWithExpireIndexFormat() {
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
    this.expireSupport.setExpire(ibPtr, ptr + this.expireOffset, expire);

  }

  @Override
  public void writeIndex(long ibPtr, long ptr, long keyPtr, int keySize, long valuePtr,
      int valueSize, int sid, int dataOffset, int dataSize, long expire) {
    super.writeIndex(ibPtr, ptr, keyPtr, keySize, valuePtr, valueSize, sid, dataOffset, dataSize,
      expire);
    ibPtr += this.superIndexBlockHeaderSize;
    this.expireSupport.setExpire(ibPtr, ptr + this.expireOffset, expire);

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
