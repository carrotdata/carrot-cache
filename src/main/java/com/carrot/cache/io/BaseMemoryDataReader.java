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

import java.nio.ByteBuffer;

import com.carrot.cache.util.UnsafeAccess;
import com.carrot.cache.util.Utils;

public class BaseMemoryDataReader implements DataReader {

  public BaseMemoryDataReader() {
    
  }

  @Override
  public void init(String cacheName) {
    // TODO Auto-generated method stub
    
  }

  @Override
  public int read(
      IOEngine engine,
      byte[] key,
      int keyOffset,
      int keySize,
      int sid,
      long offset,
      int size, //TODO size can be -1
      byte[] buffer,
      int bufOffset) {
    // Segment read lock is already held by this thread
    Segment s = engine.getSegmentById(sid);
    if (s == null) {
      // TODO: error
      return IOEngine.NOT_FOUND; 
    }
    long ptr = s.getAddress(); 
 
    if ( s.dataSize() < offset + size) {
      // Rare situation - wrong segment - hash collision
      return IOEngine.NOT_FOUND;
    }
    
    //TODO: size can be unknown
    UnsafeAccess.copy(ptr + offset, buffer, bufOffset, size);
    // Now buffer contains both: key and value, we need to compare keys
    // Format of a key-value pair in a buffer: key-size, value-size, key, value
    int kSize = Utils.readUVInt(buffer, bufOffset);
    if (kSize != keySize) {
      return IOEngine.NOT_FOUND;
    }
    int kSizeSize = Utils.sizeUVInt(kSize);
    bufOffset += kSizeSize;
    int vSize = Utils.readUVInt(buffer, bufOffset);
    int vSizeSize = Utils.sizeUVInt(vSize);
    bufOffset += vSizeSize;

    // Now compare keys
    if (Utils.compareTo(buffer, bufOffset, keySize, key, keyOffset, keySize) == 0) {
      // If key is the same
      return size;
    } else {
      return IOEngine.NOT_FOUND;
    }
  }

  @Override
  public int read(
      IOEngine engine,
      byte[] key,
      int keyOffset,
      int keySize,
      int sid,
      long offset,
      int size,
      ByteBuffer buffer) {
    // Segment read lock is already held by this thread

    Segment s = engine.getSegmentById(sid);
    if (s == null) {
      // TODO: error
      return IOEngine.NOT_FOUND; 
    }
    long ptr = s.getAddress(); 
    if ( s.dataSize() < offset + size) {
      // Rare situation - wrong segment - hash collision
      return IOEngine.NOT_FOUND;
    }
    int off = buffer.position();
    UnsafeAccess.copy(ptr + offset, buffer,  size);
    buffer.position(off);
    // Now buffer contains both: key and value, we need to compare keys
    // Format of a key-value pair in a buffer: key-size, value-size, key, value
    int kSize = Utils.readUVInt(buffer);
    if (kSize != keySize) {
      return IOEngine.NOT_FOUND;
    }
    int kSizeSize = Utils.sizeUVInt(kSize);
    buffer.position(off + kSizeSize);

    int vSize = Utils.readUVInt(buffer);
    int vSizeSize = Utils.sizeUVInt(vSize);
    buffer.position(off + kSizeSize + vSizeSize);
    // Now compare keys
    if (Utils.compareTo(buffer, keySize, key, keyOffset, keySize) == 0) {
      // If key is the same
      buffer.position(off + size);
      return size;
    } else {
      return IOEngine.NOT_FOUND;
    }
  }

  @Override
  public int read(
      IOEngine engine,
      long keyPtr,
      int keySize,
      int sid,
      long offset,
      int size,
      byte[] buffer,
      int bufOffset) {
    // Segment read lock is already held by this thread
    Segment s = engine.getSegmentById(sid);
    if (s == null) {
      // TODO: error
      return IOEngine.NOT_FOUND; 
    }
    long ptr = s.getAddress(); 
    if ( s.dataSize() < offset + size) {
      // Rare situation - wrong segment - hash collision
      return IOEngine.NOT_FOUND;
    }
    
    UnsafeAccess.copy(ptr + offset, buffer, bufOffset, size);
    // Now buffer contains both: key and value, we need to compare keys
    // Format of a key-value pair in a buffer: key-size, value-size, key, value
    int kSize = Utils.readUVInt(buffer, bufOffset);
    if (kSize != keySize) {
      return IOEngine.NOT_FOUND;
    }
    int kSizeSize = Utils.sizeUVInt(kSize);
    bufOffset += kSizeSize;
    int vSize = Utils.readUVInt(buffer, bufOffset);
    int vSizeSize = Utils.sizeUVInt(vSize);
    bufOffset += vSizeSize;

    // Now compare keys
    if (Utils.compareTo(buffer, bufOffset, keySize, keyPtr, keySize) == 0) {
      // If key is the same
      return size;
    } else {
      return IOEngine.NOT_FOUND;
    }
  }

  @Override
  public int read(
      IOEngine engine, long keyPtr, int keySize, int sid, long offset, int size, ByteBuffer buffer) {
    // Segment read lock is already held by this thread
    Segment s = engine.getSegmentById(sid);
    if (s == null) {
      // TODO: error
      return IOEngine.NOT_FOUND; 
    }
    long ptr = s.getAddress(); 
    if ( s.dataSize() < offset + size) {
      // Rare situation - wrong segment - hash collision
      return IOEngine.NOT_FOUND;
    }
    int off = buffer.position();
    UnsafeAccess.copy(ptr + offset, buffer,  size);
    // Now buffer contains both: key and value, we need to compare keys
    // Format of a key-value pair in a buffer: key-size, value-size, key, value
    int kSize = Utils.readUVInt(buffer);
    if (kSize != keySize) {
      return IOEngine.NOT_FOUND;
    }
    int kSizeSize = Utils.sizeUVInt(kSize);
    buffer.position(off + kSizeSize);

    int vSize = Utils.readUVInt(buffer);
    int vSizeSize = Utils.sizeUVInt(vSize);
    buffer.position(off + kSizeSize + vSizeSize);
    // Now compare keys
    if (Utils.compareTo(buffer, keySize, keyPtr, keySize) == 0) {
      // If key is the same
      buffer.position(off + size);
      return size;
    } else {
      return IOEngine.NOT_FOUND;
    }
  }
}
