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
package com.carrotdata.cache.io;

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
  public int read(IOEngine engine, byte[] key, int keyOffset, int keySize, int sid, long offset,
      int size, byte[] buffer, int bufferOffset) throws IOException;

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
  public int read(IOEngine engine, byte[] key, int keyOffset, int keySize, int sid, long offset,
      int size, ByteBuffer buffer) throws IOException;

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
  public int read(IOEngine engine, long keyPtr, int keySize, int sid, long offset, int size,
      byte[] buffer, int bufferOffset) throws IOException;

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
  public int read(IOEngine engine, long keyPtr, int keySize, int sid, long offset, int size,
      ByteBuffer buffer) throws IOException;

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
      int sid, long offset, int size, byte[] buffer, int bufferOffset, int rangeStart,
      int rangeSize) throws IOException {
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
  public default int readValueRange(IOEngine engine, byte[] key, int keyOffset, int keySize,
      int sid, long offset, int size, ByteBuffer buffer, int rangeStart, int rangeSize)
      throws IOException {
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
  public default int readValueRange(IOEngine engine, long keyPtr, int keySize, int sid, long offset,
      int size, ByteBuffer buffer, int rangeStart, int rangeSize) throws IOException {
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
