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

import static com.carrotdata.cache.io.BlockReaderWriterSupport.findInBlock;
import static com.carrotdata.cache.io.BlockReaderWriterSupport.getFullDataSize;
import static com.carrotdata.cache.util.Utils.getItemSize;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.carrotdata.cache.util.CacheConfig;
import com.carrotdata.cache.util.UnsafeAccess;

public class BlockMemoryDataReader implements DataReader {

  private int blockSize;

  public BlockMemoryDataReader() {
  }

  @Override
  public void init(String cacheName) {
    this.blockSize = CacheConfig.getInstance().getBlockWriterBlockSize(cacheName);
  }

  @Override
  public int read(IOEngine engine, byte[] key, int keyOffset, int keySize, int sid, long offset,
      int size, /* can be unknown -1 */
      byte[] buffer, int bufOffset) {

    int avail = buffer.length - bufOffset;
    // sanity check
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
    long dataSize = getFullDataSize(s, blockSize);
    if (size > 0 && dataSize < offset + size) {
      // Rare situation - wrong segment - hash collision
      return IOEngine.NOT_FOUND;
    }
    long ptr = s.getAddress() + offset;
    ptr = findInBlock(ptr, key, keyOffset, keySize);
    if (ptr < 0) {
      return IOEngine.NOT_FOUND;
    } else {
      int requiredSize = getItemSize(ptr);
      if (requiredSize > avail) {
        return requiredSize;
      }
      UnsafeAccess.copy(ptr, buffer, bufOffset, requiredSize);
      return requiredSize;
    }
  }

  @Override
  public int read(IOEngine engine, byte[] key, int keyOffset, int keySize, int sid, long offset,
      int size, ByteBuffer buffer) {
    // Segment read lock is already held by this thread
    int avail = buffer.remaining();
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
    long dataSize = getFullDataSize(s, blockSize);
    if (size > 0 && dataSize < offset + size) {
      // Rare situation - wrong segment - hash collision
      return IOEngine.NOT_FOUND;
    }
    long ptr = s.getAddress() + offset;
    ptr = findInBlock(ptr, key, keyOffset, keySize);
    if (ptr < 0) {
      return IOEngine.NOT_FOUND;
    } else {
      int requiredSize = getItemSize(ptr);
      if (requiredSize > avail) {
        return requiredSize;
      }
      int pos = buffer.position();
      UnsafeAccess.copy(ptr, buffer, requiredSize);
      buffer.position(pos);
      return requiredSize;
    }
  }

  @Override
  public int read(IOEngine engine, long keyPtr, int keySize, int sid, long offset, int size,
      byte[] buffer, int bufOffset) {
    // Segment read lock is already held by this thread
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
    long dataSize = getFullDataSize(s, blockSize);
    if (size > 0 && dataSize < offset + size) {
      // Rare situation - wrong segment - hash collision
      return IOEngine.NOT_FOUND;
    }
    long ptr = s.getAddress() + offset;
    ptr = findInBlock(ptr, keyPtr, keySize);
    if (ptr < 0) {
      return IOEngine.NOT_FOUND;
    } else {
      int requiredSize = getItemSize(ptr);
      if (requiredSize > avail) {
        return requiredSize;
      }
      UnsafeAccess.copy(ptr, buffer, bufOffset, requiredSize);
      return requiredSize;
    }
  }

  @Override
  public int read(IOEngine engine, long keyPtr, int keySize, int sid, long offset, int size,
      ByteBuffer buffer) {
    // Segment read lock is already held by this thread
    int avail = buffer.remaining();
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
    long dataSize = getFullDataSize(s, blockSize);
    if (size > 0 && dataSize < offset + size) {
      // Rare situation - wrong segment - hash collision
      return IOEngine.NOT_FOUND;
    }
    long ptr = s.getAddress() + offset;
    ptr = findInBlock(ptr, keyPtr, keySize);
    if (ptr < 0) {
      return IOEngine.NOT_FOUND;
    } else {
      int requiredSize = getItemSize(ptr);
      if (requiredSize > avail) {
        return requiredSize;
      }
      int pos = buffer.position();
      UnsafeAccess.copy(ptr, buffer, requiredSize);
      buffer.position(pos);
      return requiredSize;
    }
  }

  @Override
  public SegmentScanner getSegmentScanner(IOEngine engine, Segment s) throws IOException {
    CacheConfig config = CacheConfig.getInstance();
    String cacheName = engine.getCacheName();
    int blockSize = config.getBlockWriterBlockSize(cacheName);
    return new BlockMemorySegmentScanner(s, blockSize);
  }
}
