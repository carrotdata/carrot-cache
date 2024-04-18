/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.onecache.core.io;

/**
 * Segment data appender (writer). 
 * Implementation MUST be stateless hence - thread - safe
 */

public interface DataWriter {
  
  /**
   * When IOEngine receives with reply code it MUST
   * skip updating MemoryIndex. This code is used by batching 
   * compressed block writer
   */
  public final static long IGNORE = Long.MIN_VALUE;
  /**
   * Is block based data writer
   * @return true false
   */
  public default boolean isBlockBased() {
    return false;
  }
  
  /**
   * Is write batch supported
   * @return true if supported, false - otherwise
   */
  public default boolean isWriteBatchSupported() {
    return false;
  }
  
  /**
   * This method must be called after init()
   * @return
   */
  public default WriteBatch newWriteBatch() {
    if (!isWriteBatchSupported()) {
      throw new UnsupportedOperationException("append write batch");
    }
    return null;
  }
  
  /**
   * For data writers with batch supports
   * @param s data segment
   * @param batch write batch
   * @return total bytes written
   */
  public default long append(Segment s, WriteBatch batch) {
    if (!isWriteBatchSupported()) {
      throw new UnsupportedOperationException("append write batch");
    }
    return 0;
  }
  
  /**
   * Get block size
   * @return block size
   */
  public default int getBlockSize() {
    return 0;
  }
  
  /**
   * Initialize after creation
   * @param cacheName
   */
  public void init(String cacheName);
  
  /**
   * Appends entry to a segment
   * @param keyPtr key address
   * @param keySize key size
   * @param itemPtr value address
   * @param itemSize value size
   * @param s data segment
   * @return offset at a segment for a new entry or -1 (can not append) 
   */
  public long append(Segment s, long keyPtr, int keySize, long itemPtr, int itemSize);
  
  
  /**
   * Appends entry to a segment
   * @param key key buffer
   * @param keyOffset offset
   * @param keySize key size
   * @param value value buffer
   * @param valueOffset offset
   * @param valueSize value size
   * @param s data segment
   * @return offset at a segment for a new entry or -1 (can not append) 
   */
  public long append(Segment s, byte[] key, int keyOffset, int keySize, byte[] value, int valueOffset, int valueSize);
  
  /**
   * Appends single entry to a segment (batch mode)
   * @param keyPtr key address
   * @param keySize key size
   * @param itemPtr value address
   * @param itemSize value size
   * @param s data segment
   * @return offset at a segment for a new entry or -1 (can not append) 
   */
  public default long appendSingle(Segment s, long keyPtr, int keySize, long itemPtr, int itemSize) {
    if (!isWriteBatchSupported()) {
      throw new UnsupportedOperationException("append single write batch");
    }
    return -1;
  }
  
  
  /**
   * Appends entry to a segment (batch mode)
   * @param key key buffer
   * @param keyOffset offset
   * @param keySize key size
   * @param value value buffer
   * @param valueOffset offset
   * @param valueSize value size
   * @param s data segment
   * @return offset at a segment for a new entry or -1 (can not append) 
   */
  public default long appendSingle(Segment s, byte[] key, int keyOffset, int keySize, byte[] value, int valueOffset, int valueSize) {
    if (!isWriteBatchSupported()) {
      throw new UnsupportedOperationException("append single write batch");
    }
    return -1;
  }

}
