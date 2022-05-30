/**
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
package com.carrot.cache.index;

import com.carrot.cache.util.Persistent;
import com.carrot.cache.util.UnsafeAccess;

public interface IndexFormat extends Persistent {
    
  /**
   * Cache name for this index format
   * @param cacheName
   */
  public default void setCacheName(String cacheName) {
    
  }
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
  public int indexEntrySize() ;
  
  /**
   * Returns full entry size (index + data)
   * @param ptr index entry address (can contain data as well)
   * @return full entry size
   */
  public int fullEntrySize(long ptr);
  
  /**
   * Get full index entry size for a given key size and value size
   * (key-value can be embedded for some implementations)
   * 
   * Segment ID is always 2 bytes
   * Data segment offset is always 4 bytes 
   * 
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
   * Find key in the index and loads index data into a provided buffer
   * @param ibPtr index block pointer
   * @param keyPtr key address
   * @param keySize key size
   * @param hit true - if hit, false - otherwise
   * @param buffer memory buffer pointer
   * @return size of an index entry or -1 (if not found)
   */
  public int find(long ibPtr, long keyPtr, int keySize, boolean hit, long buffer);
  
  /**
   * Find key in the index and loads index data into a provided buffer
   * 
   * @param ibPtr index block pointer
   * @param key key buffer
   * @param keyOffset key offset
   * @param keySize key size
   * @param hit true - if hit, false - otherwise
   * @param buffer memory buffer pointer
   * @return size of an index entry or -1 (if not found)
   */
  public int find(long ibPtr, byte[] key, int keyOffset, int keySize, boolean hit, long buffer);
  
  /**
   * Returns key-value size 
   * @param buffer buffer contains entry data 
   * @return size
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
  public default int getEmbeddedOffset() {
    return 0;
  }
  
  /**
   * Get expiration time
   * @param ibPtr index block address
   * @param buffer buffer contains entry data
   * @return expiration time (0 - no expire)
   */
  public long getExpire(long ibPtr, long buffer);
  
  
  /**
   * Get hash value for a given index entry address
   * @param ptr address
   * @return hash value
   */
  public default long getHash(long ptr) {
    return UnsafeAccess.toLong(ptr);
  }
  
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
   * 
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
   */
  public void writeIndex(long ptr, byte[] key, int keyOffset, int keySize, byte[] value, 
      int valueOffset, int valueSize, short sid, int dataOffset, int dataSize);
  
  /**
   * Write index in place
   * 
   * @param ptr address to write
   * @param keyPtr key address
   * @param keySize key size
   * @param valuePtr value address
   * @param valueSize value size
   * @param sid segment id
   * @param dataOffset offset in a block segment
   * @param dataSize data size
   */
  public void writeIndex(long ptr, long keyPtr, int keySize, long valuePtr, 
      int valueSize, short sid, int dataOffset, int dataSize);
  
  
}