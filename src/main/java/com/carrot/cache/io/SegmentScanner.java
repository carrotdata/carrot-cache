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
package com.carrot.cache.io;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

public interface SegmentScanner extends Closeable {
  
  /**
   * Has next element
   * @return true/false
   */
  public boolean hasNext() throws IOException;
  
  /**
   * Advance to next
   * @return true if succeeded, false - otherwise
   */
  public boolean next() throws IOException;
  
  /**
   * Current key length
   * @return key length
   */
  public int keyLength() throws IOException;
  
  /**
   * Current value length
   * @return
   */
  public int valueLength() throws IOException;
  
  /**
   * Current key address
   * @return key address
   */
  public long keyAddress();
  
  /**
   * Current value address
   * @return
   */
  public long valueAddress();
  
  /**
   * Expiration time
   * @return expiration time
   * @deprecated
   */
  public long getExpire();
  
  /**
   * Read key into a buffer
   * @param b byte buffer
   * @return total bytes read
   */
  public int getKey(ByteBuffer b) throws IOException;
  
  /**
   * Read value into a buffer
   * @param b byte buffer
   * @return total bytes read
   */
  public int getValue(ByteBuffer b) throws IOException;
  
  /**
   * Read key into a given buffer
   * @param buffer memory buffer
   * @param offset buffer offset 
   * @return total byte read
   * @throws IOException
   */
  public int getKey(byte[] buffer, int offset) throws IOException;
  
  /**
   * Read value into a given buffer
   * @param buffer memory buffer
   * @param offset buffer offset
   * @return total bytes read
   * @throws IOException
   */
  public int getValue(byte[] buffer, int offset) throws IOException;
  
  /**
   * Direct access to a key and value data (only off-heap IOEngine supports it)
   * @return true if keyAddress() and valueAddress() are supported 
   */
  public default boolean isDirect() {
    return true;
  }
  
  /**
   * Get data segment for the scanner
   * @return data segment
   */
  public Segment getSegment();
  
  /**
   * Get current offset in the segment
   * @return offset
   */
  public long getOffset();
}
