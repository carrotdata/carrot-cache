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

import com.carrotdata.cache.util.Persistent;

public interface IndexFormat extends Persistent {

  /**
   * Are all entries of the same size?
   * @return true - yes, false - otherwise
   */
  public boolean isFixedSize();

  /**
   * Cache name for this index format
   * @param cacheName
   */
  public void setCacheName(String cacheName);

  /**
   * Compares current index item with a given hash
   * @param ptr current index item address
   * @param hash hash of a key
   * @return true or false
   */
  public boolean equals(long ptr, long hash);

  /**
   * Returns index entry size (only index part)
   * @return size
   */
  public int indexEntrySize();

  /**
   * Returns full entry size (index + data)
   * @param ptr index entry address (can contain data as well)
   * @return full entry size
   */
  public int fullEntrySize(long ptr);

  /**
   * Get full index entry size for a given key size and value size (key-value can be embedded for
   * some implementations) Segment ID is always 2 bytes Data segment offset is always 4 bytes
   * @param keySize key size
   * @param valueSize value size
   * @return full index entry size
   */
  public int fullEntrySize(int keySize, int valueSize);

  /**
   * Advance index segment by one entry
   * @param current current entry address
   * @return next index entry address or -1 (last one)
   */
  public long advance(long current);

  /**
   * Returns key-value size
   * @param buffer buffer contains entry data
   * @return size or -1 if format does not store this value
   */
  public int getKeyValueSize(long buffer);

  /**
   * Returns segment id
   * @param buffer buffer contains entry data
   * @return segment id
   */
  public int getSegmentId(long buffer);

  /**
   * Returns offset of a cached entry in a segment
   * @param buffer buffer contains entry data
   * @return offset
   */
  public long getOffset(long buffer);

  /**
   * For embedded into index key-value returns offset where data starts
   * @return embedded data offset
   */
  public int getEmbeddedOffset();

  /**
   * Is expiration supported by this index format
   * @return true/ false
   */
  public boolean isExpirationSupported();

  /**
   * Get expiration time
   * @param ibPtr index block address
   * @param buffer buffer contains entry data
   * @return expiration time (0 - no expire, -1 - not supported)
   */
  public long getExpire(long ibPtr, long buffer);

  /**
   * Get current expiration time and set new one
   * @param ibPtr index block address
   * @param expPtr pointer to expiration field
   * @return old expiration time in ms
   */
  public long getAndSetExpire(long ibPtr, long expPtr, long expire);

  /**
   * Get hash bit value value for a given index entry address
   * @param ptr address
   * @param n bit number
   * @return n-th bit of a hash value
   */
  public int getHashBit(long ptr, int n);

  /**
   * Gets index block header size
   * @return size
   */
  public int getIndexBlockHeaderSize();

  /**
   * get hit count for a given index
   * @param buffer index data
   * @return count
   */
  public int getHitCount(long buffer);

  /**
   * Records hit for a given index (address)
   * @param ptr address of an index item
   */
  public void hit(long ptr);

  /**
   * Write index in place
   * @param ibPtr index block pointer
   * @param ptr address to write
   * @param key key buffer
   * @param keyOffset key offset in the buffer
   * @param keySize key size
   * @param value value buffer
   * @param valueOffset value offset in the buffer
   * @param valueSize value size
   * @param sid segment id
   * @param dataOffset offset in a block segment
   * @param dataSize data size
   * @param expire expiration time in ms since 01/01/1970 (if supported)
   */
  public void writeIndex(long ibPtr, long ptr, byte[] key, int keyOffset, int keySize, byte[] value,
      int valueOffset, int valueSize, int sid, int dataOffset, int dataSize, long expire);

  /**
   * Write index in place
   * @param ibPtr index block pointer
   * @param ptr address to write
   * @param keyPtr key address
   * @param keySize key size
   * @param valuePtr value address
   * @param valueSize value size
   * @param sid segment id
   * @param dataOffset offset in a block segment
   * @param dataSize data size
   * @param expire expiration time
   */
  public void writeIndex(long ibPtr, long ptr, long keyPtr, int keySize, long valuePtr,
      int valueSize, int sid, int dataOffset, int dataSize, long expire);

  /**
   * Update index in place ( segment ID and offset
   * @param ptr address to write
   * @param sid segment id
   * @param dataOffset offset in a block segment
   */
  public void updateIndex(long ptr, int sid, int dataOffset);

  /**
   * Begin index block access
   * @param ibPtr index block address
   * @param force forces scan operations
   * @return true if full index block scan requested, false - otherwise when force = true, always
   *         returns true
   */
  public boolean begin(long ibPtr, boolean force);

  /**
   * Begins index block access operation
   * @param ibPtr index block address
   * @return true if scan is requested, false - otherwise
   */
  public boolean begin(long ibPtr);

  /**
   * End index block access
   * @param ibPtr index block address
   */
  public default void end(long ibPtr) {
  }

  /**
   * Update meta section after index block split (if needed)
   * @param ibPtr new index block pointer
   */
  public void updateMetaSection(long ibPtr);

  /**
   * Does this format keeps K-V size?
   * @return true or false
   */
  public boolean isSizeSupported();

  public int hashOffset();

  public int sidOffset();

  public int dataOffsetOffset();

  public int expireOffset();

  public int sizeOffset();
}
