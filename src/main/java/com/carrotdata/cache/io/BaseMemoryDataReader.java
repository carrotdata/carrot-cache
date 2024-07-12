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

import static com.carrotdata.cache.util.Utils.getItemSize;
import static com.carrotdata.cache.util.Utils.getKeyOffset;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.carrotdata.cache.util.UnsafeAccess;
import com.carrotdata.cache.util.Utils;

public class BaseMemoryDataReader implements DataReader {

  public BaseMemoryDataReader() {
  }

  @Override
  public void init(String cacheName) {
    // TODO Auto-generated method stub
  }

  @Override
  public int read(IOEngine engine, byte[] key, int keyOffset, int keySize, int sid, long offset,
      int size, // TODO size can be -1
      byte[] buffer, int bufOffset) {

    int avail = buffer.length - bufOffset;

    // Segment read lock is already held by this thread
    Segment s = engine.getSegmentById(sid);
    if (s == null) {
      // TODO: error
      return IOEngine.NOT_FOUND;
    }

    if (!s.isMemory()) {
      return IOEngine.NOT_FOUND;
    }
    long ptr = s.getAddress();

    if (size < 0) {
      size = getItemSize(ptr + offset);
    }
    if (size > avail) {
      return size;
    }
    if (s.getSegmentDataSize() < offset + size) {
      // Rare situation - wrong segment - hash collision
      return IOEngine.NOT_FOUND;
    }
    UnsafeAccess.copy(ptr + offset, buffer, bufOffset, size);
    // Now buffer contains both: key and value, we need to compare keys
    // Format of a key-value pair in a buffer: key-size, value-size, key, value

    bufOffset += getKeyOffset(buffer, bufOffset);

    // Now compare keys
    if (Utils.compareTo(buffer, bufOffset, keySize, key, keyOffset, keySize) == 0) {
      // If key is the same
      return size;
    } else {
      return IOEngine.NOT_FOUND;
    }
  }

  @Override
  public int read(IOEngine engine, byte[] key, int keyOffset, int keySize, int sid, long offset,
      int size, // can be < 0 - unknown
      ByteBuffer buffer) {
    int avail = buffer.remaining();
    // Segment read lock is already held by this thread
    Segment s = engine.getSegmentById(sid);
    if (s == null) {
      // TODO: error
      return IOEngine.NOT_FOUND;
    }

    if (!s.isMemory()) {
      return IOEngine.NOT_FOUND;
    }
    long ptr = s.getAddress();

    if (size < 0) {
      size = getItemSize(ptr + offset);
    }
    if (size > avail) {
      return size;
    }
    if (s.getSegmentDataSize() < offset + size) {
      // Rare situation - wrong segment - hash collision
      return IOEngine.NOT_FOUND;
    }
    int pos = buffer.position();
    UnsafeAccess.copy(ptr + offset, buffer, size);
    buffer.position(pos);
    // Now buffer contains both: key and value, we need to compare keys
    // Format of a key-value pair in a buffer: key-size, value-size, key, value

    int $off = getKeyOffset(buffer);
    buffer.position(pos + $off);
    // Now compare keys
    if (Utils.compareTo(buffer, keySize, key, keyOffset, keySize) == 0) {
      // If key is the same
      buffer.position(pos);
      return size;
    } else {
      return IOEngine.NOT_FOUND;
    }
  }

  @Override
  public int read(IOEngine engine, long keyPtr, int keySize, int sid, long offset, int size,
      byte[] buffer, int bufOffset) {
    int avail = buffer.length - bufOffset;
    // Sanity check
    if (size > avail) {
      return size;
    }
    // Segment read lock is already held by this thread
    Segment s = engine.getSegmentById(sid);
    if (s == null) {
      // TODO: error
      return IOEngine.NOT_FOUND;
    }

    if (!s.isMemory()) {
      return IOEngine.NOT_FOUND;
    }
    long ptr = s.getAddress();
    if (size < 0) {
      size = getItemSize(ptr + offset);
    }
    if (size > avail) {
      return size;
    }
    if (s.getSegmentDataSize() < offset + size) {
      // Rare situation - wrong segment - hash collision
      return IOEngine.NOT_FOUND;
    }

    UnsafeAccess.copy(ptr + offset, buffer, bufOffset, size);
    // Now buffer contains both: key and value, we need to compare keys
    // Format of a key-value pair in a buffer: key-size, value-size, key, value

    bufOffset += getKeyOffset(buffer, bufOffset);
    // Now compare keys
    if (Utils.compareTo(buffer, bufOffset, keySize, keyPtr, keySize) == 0) {
      // If key is the same
      return size;
    } else {
      return IOEngine.NOT_FOUND;
    }
  }

  @Override
  public int read(IOEngine engine, long keyPtr, int keySize, int sid, long offset, int size,
      ByteBuffer buffer) {

    int avail = buffer.remaining();
    // Segment read lock is already held by this thread
    Segment s = engine.getSegmentById(sid);
    if (s == null) {
      // TODO: error
      return IOEngine.NOT_FOUND;
    }
    if (!s.isMemory()) {
      return IOEngine.NOT_FOUND;
    }
    long ptr = s.getAddress();
    if (size < 0) {
      size = getItemSize(ptr + offset);
    }
    if (size > avail) {
      return size;
    }
    if (s.getSegmentDataSize() < offset + size) {
      // Rare situation - wrong segment - hash collision
      return IOEngine.NOT_FOUND;
    }
    int pos = buffer.position();
    UnsafeAccess.copy(ptr + offset, buffer, size);
    // Now buffer contains both: key and value, we need to compare keys
    // Format of a key-value pair in a buffer: key-size, value-size, key, value
    // rewind position back
    buffer.position(pos);
    int $off = getKeyOffset(buffer);
    buffer.position(pos + $off);
    // Now compare keys
    if (Utils.compareTo(buffer, keySize, keyPtr, keySize) == 0) {
      // If key is the same
      buffer.position(pos);
      return size;
    } else {
      return IOEngine.NOT_FOUND;
    }
  }

  @Override
  public int readValueRange(IOEngine engine, byte[] key, int keyOffset, int keySize, int sid,
      long offset, int size, byte[] buffer, int bufOffset, int rangeStart, int rangeSize)
      throws IOException {
    int avail = buffer.length - bufOffset;

    if (rangeSize > avail) {
      rangeSize = avail;
    }

    // Segment read lock is already held by this thread
    Segment s = engine.getSegmentById(sid);
    if (s == null) {
      // TODO: error
      return IOEngine.NOT_FOUND;
    }

    if (!s.isMemory()) {
      return IOEngine.NOT_FOUND;
    }
    long ptr = s.getAddress();

    int valueSize = Utils.getValueSize(ptr + offset);

    if (valueSize < rangeStart) {
      // TODO: better handling
      return IOEngine.NOT_FOUND;
    }

    if (valueSize < rangeStart + rangeSize) {
      rangeSize = valueSize - rangeStart;
    }

    int valueOffset = Utils.getValueOffset(ptr + offset);

    valueOffset += rangeStart;

    if (s.getSegmentDataSize() < offset + valueOffset + rangeSize) {
      // Rare situation - wrong segment - hash collision
      return IOEngine.NOT_FOUND;
    }
    UnsafeAccess.copy(ptr + offset + valueOffset, buffer, bufOffset, rangeSize);
    // Now buffer contains both: key and value, we need to compare keys
    // Format of a key-value pair in a buffer: key-size, value-size, key, value

    int kSize = Utils.getKeySize(ptr + offset);
    if (kSize != keySize) {
      return IOEngine.NOT_FOUND;
    }

    int kOffset = Utils.getKeyOffset(ptr + offset);
    // Now compare keys
    if (Utils.compareTo(key, keyOffset, keySize, ptr + offset + kOffset, kSize) == 0) {
      // If key is the same
      return rangeSize;
    } else {
      return IOEngine.NOT_FOUND;
    }

  }

  @Override
  public int readValueRange(IOEngine engine, byte[] key, int keyOffset, int keySize, int sid,
      long offset, int size, ByteBuffer buffer, int rangeStart, int rangeSize) throws IOException {
    int avail = buffer.remaining();
    if (rangeSize > avail) {
      rangeSize = avail;
    }

    // Segment read lock is already held by this thread
    Segment s = engine.getSegmentById(sid);
    if (s == null) {
      // TODO: error
      return IOEngine.NOT_FOUND;
    }

    if (!s.isMemory()) {
      return IOEngine.NOT_FOUND;
    }
    long ptr = s.getAddress();

    int valueSize = Utils.getValueSize(ptr + offset);

    if (valueSize < rangeStart) {
      // TODO: better handling
      return IOEngine.NOT_FOUND;
    }

    if (valueSize < rangeStart + rangeSize) {
      rangeSize = valueSize - rangeStart;
    }

    int valueOffset = Utils.getValueOffset(ptr + offset);

    valueOffset += rangeStart;

    if (s.getSegmentDataSize() < offset + valueOffset + rangeSize) {
      // Rare situation - wrong segment - hash collision
      return IOEngine.NOT_FOUND;
    }
    int pos = buffer.position();

    UnsafeAccess.copy(ptr + offset + valueOffset, buffer, rangeSize);
    buffer.position(pos);

    int kSize = Utils.getKeySize(ptr + offset);

    if (kSize != keySize) {
      return IOEngine.NOT_FOUND;
    }

    int kOffset = Utils.getKeyOffset(ptr + offset);

    // Now compare keys
    if (Utils.compareTo(key, keyOffset, keySize, ptr + offset + kOffset, keySize) == 0) {
      // If key is the same
      return rangeSize;
    } else {
      return IOEngine.NOT_FOUND;
    }
  }

  @Override
  public int readValueRange(IOEngine engine, long keyPtr, int keySize, int sid, long offset,
      int size, byte[] buffer, int bufOffset, int rangeStart, int rangeSize) throws IOException {
    int avail = buffer.length - bufOffset;

    if (rangeSize > avail) {
      rangeSize = avail;
    }

    // Segment read lock is already held by this thread
    Segment s = engine.getSegmentById(sid);
    if (s == null) {
      // TODO: error
      return IOEngine.NOT_FOUND;
    }

    if (!s.isMemory()) {
      return IOEngine.NOT_FOUND;
    }
    long ptr = s.getAddress();

    int valueSize = Utils.getValueSize(ptr + offset);

    if (valueSize < rangeStart) {
      // TODO: better handling
      return IOEngine.NOT_FOUND;
    }

    if (valueSize < rangeStart + rangeSize) {
      rangeSize = valueSize - rangeStart;
    }

    int valueOffset = Utils.getValueOffset(ptr + offset);

    valueOffset += rangeStart;

    if (s.getSegmentDataSize() < offset + valueOffset + rangeSize) {
      // Rare situation - wrong segment - hash collision
      return IOEngine.NOT_FOUND;
    }

    UnsafeAccess.copy(ptr + offset + valueOffset, buffer, bufOffset, rangeSize);
    // Now buffer contains both: key and value, we need to compare keys
    // Format of a key-value pair in a buffer: key-size, value-size, key, value

    int kSize = Utils.getKeySize(ptr + offset);
    if (kSize != keySize) {
      return IOEngine.NOT_FOUND;
    }

    int kOffset = Utils.getKeyOffset(ptr + offset);

    // Now compare keys
    if (Utils.compareTo(ptr + offset + kOffset, kSize, keyPtr, keySize) == 0) {
      // If key is the same
      return rangeSize;
    } else {
      return IOEngine.NOT_FOUND;
    }
  }

  @Override
  public int readValueRange(IOEngine engine, long keyPtr, int keySize, int sid, long offset,
      int size, ByteBuffer buffer, int rangeStart, int rangeSize) throws IOException {
    int avail = buffer.remaining();

    if (rangeSize > avail) {
      rangeSize = avail;
    }

    // Segment read lock is already held by this thread
    Segment s = engine.getSegmentById(sid);
    if (s == null) {
      // TODO: error
      return IOEngine.NOT_FOUND;
    }
    if (!s.isMemory()) {
      return IOEngine.NOT_FOUND;
    }
    long ptr = s.getAddress();

    int valueSize = Utils.getValueSize(ptr + offset);

    if (valueSize < rangeStart) {
      // TODO: better handling
      return IOEngine.NOT_FOUND;
    }

    if (valueSize < rangeStart + rangeSize) {
      rangeSize = valueSize - rangeStart;
    }

    int valueOffset = Utils.getValueOffset(ptr + offset);

    valueOffset += rangeStart;

    if (s.getSegmentDataSize() < offset + valueOffset + rangeSize) {
      // Rare situation - wrong segment - hash collision
      return IOEngine.NOT_FOUND;
    }

    int pos = buffer.position();
    UnsafeAccess.copy(ptr + offset + valueOffset, buffer, rangeSize);

    buffer.position(pos);

    int kSize = Utils.getKeySize(ptr + offset);

    if (kSize != keySize) {
      return IOEngine.NOT_FOUND;
    }
    int kOffset = Utils.getKeyOffset(ptr + offset);
    // Now compare keys
    if (Utils.compareTo(ptr + offset + kOffset, kSize, keyPtr, keySize) == 0) {
      // If key is the same
      return rangeSize;
    } else {
      return IOEngine.NOT_FOUND;
    }
  }

  @Override
  public SegmentScanner getSegmentScanner(IOEngine engine, Segment s) throws IOException {
    return new BaseMemorySegmentScanner(s);
  }

}
