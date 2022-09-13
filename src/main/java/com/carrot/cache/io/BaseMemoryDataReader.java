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

import java.io.IOException;
import java.nio.ByteBuffer;

import com.carrot.cache.util.UnsafeAccess;
import com.carrot.cache.util.Utils;
import static com.carrot.cache.util.Utils.getItemSize;
import static com.carrot.cache.util.Utils.getKeyOffset;


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

    if (!s.isOffheap()) {
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

    if (!s.isOffheap()) {
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

    if (!s.isOffheap()) {
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
    if (!s.isOffheap()) {
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
  public SegmentScanner getSegmentScanner(IOEngine engine, Segment s) throws IOException {
    return new BaseMemorySegmentScanner(s);
  }
  
}
