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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotdata.cache.util.CacheConfig;

public class MemoryIOEngine extends IOEngine {

  /** Logger */
  @SuppressWarnings("unused")
  private static final Logger LOG = LoggerFactory.getLogger(MemoryIOEngine.class);

  public MemoryIOEngine(String cacheName) {
    super(cacheName);
  }

  public MemoryIOEngine(CacheConfig conf) {
    super(conf);
  }

  @Override
  protected int getInternal(int sid, long offset, int size, byte[] key, int keyOffset, int keySize,
      byte[] buffer, int bufOffset) {
    try {
      return this.memoryDataReader.read(this, key, keyOffset, keySize, sid, offset, size, buffer,
        bufOffset);
    } catch (IOException e) {
      // never happens
    }
    return NOT_FOUND;
  }

  @Override
  protected int getInternal(int sid, long offset, int size, byte[] key, int keyOffset, int keySize,
      ByteBuffer buffer) throws IOException {
    try {
      return this.memoryDataReader.read(this, key, keyOffset, keySize, sid, offset, size, buffer);
    } catch (IOException e) {
      // never happens
    }
    return NOT_FOUND;
  }

  @Override
  protected int getInternal(int sid, long offset, int size, long keyPtr, int keySize, byte[] buffer,
      int bufOffset) {
    try {
      return this.memoryDataReader.read(this, keyPtr, keySize, sid, offset, size, buffer,
        bufOffset);
    } catch (IOException e) {
      // never happens
    }
    return NOT_FOUND;
  }

  @Override
  protected int getInternal(int sid, long offset, int size, long keyPtr, int keySize,
      ByteBuffer buffer) throws IOException {
    try {
      return this.memoryDataReader.read(this, keyPtr, keySize, sid, offset, size, buffer);
    } catch (IOException e) {
      // never happens
    }
    return NOT_FOUND;
  }

  @Override
  public SegmentScanner getScanner(Segment s) throws IOException {
    return this.memoryDataReader.getSegmentScanner(this, s);
  }

  @Override
  protected void saveInternal(Segment data) throws IOException {
    data.seal();
  }

  @Override
  protected int getRangeInternal(int sid, long offset, int size, byte[] key, int keyOffset,
      int keySize, int rangeStart, int rangeSize, byte[] buffer, int bufOffset) throws IOException {
    try {
      return this.memoryDataReader.readValueRange(this, key, keyOffset, keySize, sid, offset, size,
        buffer, bufOffset, rangeStart, rangeSize);
    } catch (IOException e) {
      // never happens
    }
    return NOT_FOUND;
  }

  @Override
  protected int getRangeInternal(int sid, long offset, int size, long keyPtr, int keySize,
      int rangeStart, int rangeSize, byte[] buffer, int bufOffset) throws IOException {
    try {
      return this.memoryDataReader.readValueRange(this, keyPtr, keySize, sid, offset, size, buffer,
        bufOffset, rangeStart, rangeSize);
    } catch (IOException e) {
      // never happens
    }
    return NOT_FOUND;
  }

  @Override
  protected int getRangeInternal(int sid, long offset, int size, byte[] key, int keyOffset,
      int keySize, int rangeStart, int rangeSize, ByteBuffer buffer) throws IOException {
    try {
      return this.memoryDataReader.readValueRange(this, key, keyOffset, keySize, sid, offset, size,
        buffer, rangeStart, rangeSize);
    } catch (IOException e) {
      // never happens
    }
    return NOT_FOUND;
  }

  @Override
  protected int getRangeInternal(int sid, long offset, int size, long keyPtr, int keySize,
      int rangeStart, int rangeSize, ByteBuffer buffer) throws IOException {
    try {
      return this.memoryDataReader.readValueRange(this, keyPtr, keySize, sid, offset, size, buffer,
        rangeStart, rangeSize);
    } catch (IOException e) {
      // never happens
    }
    return NOT_FOUND;
  }

  @Override
  protected boolean isMemory() {
    return true;
  }
}
