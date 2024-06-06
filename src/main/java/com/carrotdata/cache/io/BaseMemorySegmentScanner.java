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

import com.carrotdata.cache.util.UnsafeAccess;
import com.carrotdata.cache.util.Utils;

/**
 * Segment scanner Usage: while(scanner.hasNext()){ // do job // ... // next() scanner.next(); }
 */
public final class BaseMemorySegmentScanner implements SegmentScanner {
  /*
   * Data segment
   */
  Segment segment;
  /*
   * Current scanner index
   */
  int currentIndex = 0;

  /**
   * Current offset in a parent segment
   */
  int offset = 0;

  /*
   * Private constructor
   */
  BaseMemorySegmentScanner(Segment s) {
    // Make sure it is sealed
    if (s.isSealed() == false) {
      throw new RuntimeException("segment is not sealed");
    }
    this.segment = s;
    s.readLock();
  }

  public boolean hasNext() {
    return currentIndex < segment.getTotalItems();
  }

  public boolean next() {
    long ptr = segment.getAddress();

    int keySize = Utils.readUVInt(ptr + offset);
    int keySizeSize = Utils.sizeUVInt(keySize);
    offset += keySizeSize;
    int valueSize = Utils.readUVInt(ptr + offset);
    int valueSizeSize = Utils.sizeUVInt(valueSize);
    offset += valueSizeSize;
    offset += keySize + valueSize;
    currentIndex++;
    // TODO
    return true;
  }

  /**
   * Get expiration time of a current cached entry
   * @return expiration time
   * @deprecated use memory index to retrieve expiration time
   */
  public final long getExpire() {
    return -1;
  }

  /**
   * Get key size of a current cached entry
   * @return key size
   */
  public final int keyLength() {
    long ptr = segment.getAddress();
    return Utils.readUVInt(ptr + offset);
  }

  /**
   * Get current value size
   * @return value size
   */

  public final int valueLength() {
    long ptr = segment.getAddress();
    int off = offset;
    int keySize = Utils.readUVInt(ptr + off);
    int keySizeSize = Utils.sizeUVInt(keySize);
    off += keySizeSize;
    return Utils.readUVInt(ptr + off);
  }

  /**
   * Get current key's address
   * @return keys address
   */
  public final long keyAddress() {
    long ptr = segment.getAddress();
    int off = offset;
    int keySize = Utils.readUVInt(ptr + off);
    int keySizeSize = Utils.sizeUVInt(keySize);
    off += keySizeSize;
    int valueSize = Utils.readUVInt(ptr + off);
    int valueSizeSize = Utils.sizeUVInt(valueSize);
    off += valueSizeSize;
    return ptr + off;
  }

  /**
   * Get current value's address
   * @return values address
   */
  public final long valueAddress() {
    long ptr = segment.getAddress();
    int off = offset;
    int keySize = Utils.readUVInt(ptr + off);
    int keySizeSize = Utils.sizeUVInt(keySize);
    off += keySizeSize;
    int valueSize = Utils.readUVInt(ptr + off);
    int valueSizeSize = Utils.sizeUVInt(valueSize);
    off += valueSizeSize + keySize;
    return ptr + off;
  }

  @Override
  public void close() throws IOException {
    segment.readUnlock();
  }

  @Override
  public int getKey(ByteBuffer b) {
    int keySize = keyLength();
    long keyAddress = keyAddress();
    if (keySize <= b.remaining()) {
      UnsafeAccess.copy(keyAddress, b, keySize);
    }
    return keySize;
  }

  @Override
  public int getValue(ByteBuffer b) {
    int valueSize = valueLength();
    long valueAddress = valueAddress();
    if (valueSize <= b.remaining()) {
      UnsafeAccess.copy(valueAddress, b, valueSize);
    }
    return valueSize;
  }

  @Override
  public int getKey(byte[] buffer, int offset) throws IOException {
    int keySize = keyLength();
    if (keySize > buffer.length - offset) {
      return keySize;
    }
    long keyAddress = keyAddress();
    UnsafeAccess.copy(keyAddress, buffer, offset, keySize);
    return keySize;
  }

  @Override
  public int getValue(byte[] buffer, int offset) throws IOException {
    int valueSize = valueLength();
    if (valueSize > buffer.length - offset) {
      return valueSize;
    }
    long valueAddress = valueAddress();
    UnsafeAccess.copy(valueAddress, buffer, offset, valueSize);
    return valueSize;
  }

  @Override
  public Segment getSegment() {
    return this.segment;
  }

  @Override
  public long getOffset() {
    return this.offset;
  }
}
