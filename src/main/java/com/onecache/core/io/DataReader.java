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

import java.io.IOException;
import java.nio.ByteBuffer;

public interface DataReader {
  
  /**
   * Initialize after creation
   * @param cacheName
   */
  public void init(String cacheName);
  
  /**
   * Read key - value pair into a given buffer
   * @param engine I/O engine
   * @param sid - ID of a segment to read from
   * @param offset - offset at a segment to start reading from
   * @param size - size of a K-V pair (can be -1 - unknown)
   * @param key key buffer
   * @param keyOffset keyOffset
   * @param keySize keySize
   * @param buffer buffer
   * @param bufferOffset buffer offset
   * @return full size of K-V pair including 
   */
  public int read(IOEngine engine, byte[] key, int keyOffset, int keySize, 
      int sid, long offset, int size, byte[] buffer, int bufferOffset) throws IOException;
  
  /**
   * Read key - value pair into a given buffer
   * @param engine I/O engine
   * @param key key
   * @param keyOffset key offset
   * @param keySize key size
   * @param sid segment id
   * @param offset offset at a segment to start reading from
   * @param size size of K-V pair (can be -1)
   * @param buffer buffer
   * @return full size of a K-V pair
   */
  public int read(IOEngine engine, byte[] key, int keyOffset, int keySize, int sid,
      long offset, int size, ByteBuffer buffer) throws IOException;
  
  /**
   * Read key - value pair into a given buffer
   * @param engine I/O engine
   * @param keyPtr key address
   * @param keySize keySize
   * @param sid segment id
   * @param offset offset at a segment to start reading from
   * @param size size of K-V pair (can be -1)
   * @param buffer buffer
   * @param bufferOffset buffer offset
   * @return full size of K-V pair including 
   */
  public int read(IOEngine engine, long keyPtr, int keySize, int sid, long offset, 
      int size, byte[] buffer, int bufferOffset) throws IOException;
  
  /**
   * Read key - value pair into a given buffer
   * @param engine I/O engine
   * @param keyPtr key address
   * @param keySize key size
   * @param sid segment id
   * @param offset offset at a segment to start reading from
   * @param size size of K-V pair (can be -1)
   * @param buffer buffer
   * @return full size of a K-V pair
   */
  public int read(IOEngine engine, long keyPtr, int keySize, 
      int sid, long offset, int size, ByteBuffer buffer) throws IOException;

  
  
  /**
   * Read value range into a given buffer
   * @param engine I/O engine
   * @param sid - ID of a segment to read from
   * @param offset - offset at a segment to start reading from
   * @param size - size of a K-V pair (can be -1 - unknown)
   * @param key key buffer
   * @param keyOffset keyOffset
   * @param keySize keySize
   * @param buffer buffer
   * @param bufferOffset buffer offset
   * @param rangeStart range start
   * @param rangeSize range size
   * @return full size of K-V pair including 
   */
  public default int readValueRange(IOEngine engine, byte[] key, int keyOffset, int keySize, 
      int sid, long offset, int size, byte[] buffer, int bufferOffset, int rangeStart, int rangeSize) throws IOException {
    throw new UnsupportedOperationException("read value range");
  }
  
  /**
   * Read value range into a given buffer
   * @param engine I/O engine
   * @param key key
   * @param keyOffset key offset
   * @param keySize key size
   * @param sid segment id
   * @param offset offset at a segment to start reading from
   * @param size size of K-V pair (can be -1)
   * @param buffer buffer
   * @param rangeStart range start
   * @param rangeSize range size
   * @return full size of a K-V pair
   */
  public default int readValueRange(IOEngine engine, byte[] key, int keyOffset, int keySize, int sid,
      long offset, int size, ByteBuffer buffer, int rangeStart, int rangeSize) throws IOException {
    throw new UnsupportedOperationException("read value range");
  }
  
  /**
   * Read value range into a given buffer
   * @param engine I/O engine
   * @param keyPtr key address
   * @param keySize keySize
   * @param sid segment id
   * @param offset offset at a segment to start reading from
   * @param size size of K-V pair (can be -1)
   * @param buffer buffer
   * @param bufferOffset buffer offset
   * @param rangeStart range start
   * @param rangeSize range size
   * @return full size of K-V pair including 
   */
  public default int readValueRange(IOEngine engine, long keyPtr, int keySize, int sid, long offset, 
      int size, byte[] buffer, int bufferOffset, int rangeStart, int rangeSize) throws IOException {
    throw new UnsupportedOperationException("read value range");

  }
  
  /**
   * Read value range into a given buffer
   * @param engine I/O engine
   * @param keyPtr key address
   * @param keySize key size
   * @param sid segment id
   * @param offset offset at a segment to start reading from
   * @param size size of K-V pair (can be -1)
   * @param buffer buffer
   * @param rangeStart offset to read from
   * @param rangeSize size of a value range
   * @return full size of a K-V pair
   */
  public default int readValueRange(IOEngine engine, long keyPtr, int keySize, 
      int sid, long offset, int size, ByteBuffer buffer, int rangeStart, int rangeSize) throws IOException{
    throw new UnsupportedOperationException("read value range");
  }

  
  /**
   * Get segment scanner
   * @param engine I/O engine
   * @param s segment
   * @return segment scanner
   */
  public SegmentScanner getSegmentScanner(IOEngine engine, Segment s) throws IOException;
  
}
