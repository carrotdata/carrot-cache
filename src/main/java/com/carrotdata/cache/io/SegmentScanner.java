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
