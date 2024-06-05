
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

import com.carrotdata.cache.expire.AbstractExpireSupport;
import com.carrotdata.cache.expire.ExpireSupport;
import com.carrotdata.cache.util.CacheConfig;
import com.carrotdata.cache.util.UnsafeAccess;

public abstract class AbstractIndexFormat implements IndexFormat {

  int indexBlockHeaderSize;

  // Must be initialized by subclass
  int superIndexBlockHeaderSize;

  int indexEntrySize;

  boolean isFixedSize = true;

  boolean isExpirationSupported = false;

  boolean isSizeSupported = true;

  /**
   * @deprecated
   */
  int embeddedOffset = 0;

  int hashOffset;

  int expireOffset;

  int sidOffset;

  int dataOffsetOffset;

  int sizeOffset;

  /*
   * Expire support section
   */
  public AbstractExpireSupport expireSupport;

  public int expireMetaSize;

  public int expireFieldSize;

  public AbstractIndexFormat() {
    this.indexBlockHeaderSize = getIndexBlockHeaderSize();
    this.indexEntrySize = indexEntrySize();
    this.isFixedSize = isFixedSize();
    this.isExpirationSupported = isExpirationSupported();
    this.isSizeSupported = isSizeSupported();
    this.embeddedOffset = getEmbeddedOffset();
    this.hashOffset = hashOffset();
    this.expireOffset = expireOffset();
    this.sidOffset = sidOffset();
    this.dataOffsetOffset = dataOffsetOffset();
    this.sizeOffset = sizeOffset();
  }

  /**
   * Are all entries of the same size?
   * @return true - yes, false - otherwise
   */
  public boolean isFixedSize() {
    return true;
  }

  /**
   * Cache name for this index format
   * @param cacheName
   */
  public void setCacheName(String cacheName) {
    if (this.isExpirationSupported) {
      try {
        this.expireSupport =
            (AbstractExpireSupport) CacheConfig.getInstance().getExpireSupport(cacheName);
        this.hashOffset = hashOffset();
        this.expireOffset = expireOffset();
        this.sidOffset = sidOffset();
        this.dataOffsetOffset = dataOffsetOffset();
        this.sizeOffset = sizeOffset();
        this.indexEntrySize = indexEntrySize();
        this.indexBlockHeaderSize = getIndexBlockHeaderSize();
        this.embeddedOffset = getEmbeddedOffset();
        this.expireMetaSize = this.expireSupport.metaSectionSize;
        this.expireFieldSize = this.expireSupport.fieldSize;

      } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
        // TODO Auto-generated catch block
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * For testing
   */
  public void setExpireSupport(ExpireSupport support) {
    if (this.isExpirationSupported) {
      this.expireSupport = (AbstractExpireSupport) support;
      this.hashOffset = hashOffset();
      this.expireOffset = expireOffset();
      this.dataOffsetOffset = dataOffsetOffset();
      this.sidOffset = sidOffset();
      this.sizeOffset = sizeOffset();
      this.indexEntrySize = indexEntrySize();
      this.indexBlockHeaderSize = getIndexBlockHeaderSize();
      this.embeddedOffset = getEmbeddedOffset();
      this.expireMetaSize = this.expireSupport.metaSectionSize;
      this.expireFieldSize = this.expireSupport.fieldSize;
    }
  }

  /**
   * For embedded into index key-value returns offset where data starts
   * @return embedded data offset
   * @deprecated
   */
  public int getEmbeddedOffset() {
    return 0;
  }

  /**
   * Is expiration supported by this index format
   * @return true/ false
   */
  public boolean isExpirationSupported() {
    return false;
  }

  /**
   * Get expiration time
   * @param ibPtr index block address
   * @param buffer buffer contains entry data
   * @return expiration time (0 - no expire, -1 - not supported)
   */
  public long getExpire(long ibPtr, long buffer) {
    return -1; // expiration is not supported by default
  }

  /**
   * Get current expiration time and set new one
   * @param ibPtr index block address
   * @param expPtr pointer to expiration field
   * @return old expiration time in ms
   */
  public long getAndSetExpire(long ibPtr, long expPtr, long expire) {
    return -1; // expiration is not supported by default
  }

  /**
   * Get hash bit value value for a given index entry address
   * @param ptr address
   * @param n bit number
   * @return n-th bit of a hash value
   */
  public int getHashBit(long ptr, int n) {
    return (int) (UnsafeAccess.toLong(ptr) >>> 64 - n) & 1;
  }

  @Override
  public int fullEntrySize(long ptr) {
    return indexEntrySize();
  }

  @Override
  public int fullEntrySize(int keySize, int valueSize) {
    return indexEntrySize();
  }

  @Override
  public long advance(long current) {
    return current + indexEntrySize();
  }

  /**
   * Begin index block access
   * @param ibPtr index block address
   * @param force forces scan operations
   * @return true if full index block scan requested, false - otherwise when force = true, always
   *         returns true
   */
  @Override
  public boolean begin(long ibPtr, boolean force) {
    return false;
  }

  /**
   * Begins index block access operation
   * @param ibPtr index block address
   * @return true if scan is requested, false - otherwise
   */
  @Override
  public boolean begin(long ibPtr) {
    return begin(ibPtr, false);
  }

  /**
   * End index block access
   * @param ibPtr index block address
   */
  @Override
  public void end(long ibPtr) {
  }

  /**
   * Update meta section after index block split (if needed)
   * @param ibPtr new index block pointer
   */
  @Override
  public void updateMetaSection(long ibPtr) {
    // do nothing by default
  }

  /**
   * Does this format keeps K-V size?
   * @return true or false
   */
  @Override
  public boolean isSizeSupported() {
    return true;
  }

  public abstract int hashOffset();

  public abstract int sidOffset();

  public abstract int dataOffsetOffset();

  public abstract int expireOffset();

  public abstract int sizeOffset();

}
