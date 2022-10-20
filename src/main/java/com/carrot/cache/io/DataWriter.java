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
package com.carrot.cache.io;

/**
 * Segment data appender (writer). 
 * Implementation MUST be stateless hence - thread - safe
 */

public interface DataWriter {
  
  /**
   * Is block based data writer
   * @return true false
   */
  public default boolean isBlockBased() {
    return false;
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

}
