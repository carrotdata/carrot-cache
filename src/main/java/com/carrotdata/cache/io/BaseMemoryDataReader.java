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


import static com.carrotdata.cache.io.BlockReaderWriterSupport.META_SIZE;
import static com.carrotdata.cache.io.BlockReaderWriterSupport.OPT_META_SIZE;

import static com.carrotdata.cache.util.Utils.getItemSize;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotdata.cache.util.UnsafeAccess;
import com.carrotdata.cache.util.Utils;

public class BaseMemoryDataReader implements DataReader {
  @SuppressWarnings("unused")
  private static final Logger LOG = LoggerFactory.getLogger(BaseMemoryDataReader.class);

  public BaseMemoryDataReader() {
  }

  @Override
  public void init(String cacheName) {
  }

  @Override
  public int read(IOEngine engine, byte[] key, int keyOffset, int keySize, int sid, long offset,
      int size, // TODO size can be -1
      byte[] buffer, int bufOffset) {

    final int avail = buffer.length - bufOffset;
    if (size > avail) {
      return size;
    }
    // Segment read lock is already held by this thread
    final Segment s = engine.getSegmentById(sid);
    if (s == null) {
      // TODO: error
      return IOEngine.NOT_FOUND;
    }
    if (!s.isMemory()) {
      return IOEngine.NOT_FOUND;
    }
    final long ptr = s.getAddress();
    final int size1 = UnsafeAccess.toInt(ptr + offset);
    final int id = UnsafeAccess.toInt(ptr + offset + Utils.SIZEOF_INT);
    final int size2 = UnsafeAccess.toInt(ptr + offset + 2 * Utils.SIZEOF_INT);
    // sanity check
    // TODO: add to corrupted reads
    if (size1 <= 0 || size2 < 0 || size1 != size2) {
      return IOEngine.READ_ERROR;
    }
    final long segSize = s.getSegmentDataSize();
    if (size1 > segSize || size2 > segSize) {
      return IOEngine.READ_ERROR;
    }
    if (id != -1) {
      return IOEngine.READ_ERROR;
    } else  { // Block is not compressed
      if (segSize < offset + OPT_META_SIZE + size1) {
        return IOEngine.READ_ERROR;
      }
      // Find key-value in the buffer
      int offAdj = OPT_META_SIZE - META_SIZE;

      long addr = BlockReaderWriterSupport.findInBlock(ptr + offset + offAdj, size1, key,
        keyOffset, keySize);

      if (addr < 0) {
        return IOEngine.NOT_FOUND;
      }
      size = getItemSize(addr);
      if (size > avail) {
        return size;
      }
      if (segSize < offset + size) {
        // Rare situation - wrong segment - hash collision
        return IOEngine.READ_ERROR;
      }
      UnsafeAccess.copy(addr, buffer, bufOffset, size);
    } 
    return size;
  }
  
  @Override
  public int read(IOEngine engine, byte[] key, int keyOffset, int keySize, int sid, long offset,
      int size, // can be < 0 - unknown
      ByteBuffer buffer) {
    // Race condition. Get location from index, get segment (can be different)
    final int avail = buffer.remaining();
    if (size > avail) {
      return size;
    }
    // Segment read lock is already held by this thread
    final Segment s = engine.getSegmentById(sid);
    if (s == null) {
      // TODO: error
      return IOEngine.NOT_FOUND;
    }
    if (!s.isMemory()) {
      return IOEngine.NOT_FOUND;
    }
    final long ptr = s.getAddress();
    final int size1 = UnsafeAccess.toInt(ptr + offset);
    final int id = UnsafeAccess.toInt(ptr + offset + Utils.SIZEOF_INT);
    final int size2 = UnsafeAccess.toInt(ptr + offset + 2 * Utils.SIZEOF_INT);
    // sanity check
    if (size1 <= 0 || size2 < 0 || size1 != size2) {
      return IOEngine.READ_ERROR;
    }
    final long segSize = s.getSegmentDataSize();
    if (size1 > segSize || size2 > segSize) {
      return IOEngine.READ_ERROR;
    }
    if (id != -1) {
      return IOEngine.READ_ERROR;
    } else { // Block is not compressed
      if (segSize < offset + OPT_META_SIZE + size1) {
        return IOEngine.READ_ERROR;
      }
      // Find key-value in the buffer
      int offAdj = OPT_META_SIZE - META_SIZE;
      long addr = BlockReaderWriterSupport.findInBlock(ptr + offset + offAdj, size1, key,
        keyOffset, keySize);
      if (addr < 0) {
        return IOEngine.NOT_FOUND;
      }
      size = getItemSize(addr);
      if (size > avail) {
        return size;
      }
      // TODO: remove this?
      if (segSize < offset + size) {
        // Rare situation - wrong segment - hash collision
        return IOEngine.READ_ERROR;
      }
      int pos = buffer.position();
      UnsafeAccess.copy(addr, buffer, size);
      buffer.position(pos);
    } 
    return size;
  }

  @Override
  public int read(IOEngine engine, long keyPtr, int keySize, int sid, long offset, int size,
      byte[] buffer, int bufOffset) {
    final int avail = buffer.length - bufOffset;
    if (size > avail) {
      return size;
    }
    // Segment read lock is already held by this thread
    final Segment s = engine.getSegmentById(sid);
    if (s == null) {
      // TODO: error
      return IOEngine.NOT_FOUND;
    }
    if (!s.isMemory()) {
      return IOEngine.NOT_FOUND;
    }
    final long ptr = s.getAddress();
    final int size1 = UnsafeAccess.toInt(ptr + offset);
    final int id = UnsafeAccess.toInt(ptr + offset + Utils.SIZEOF_INT);
    final int size2 = UnsafeAccess.toInt(ptr + offset + 2 * Utils.SIZEOF_INT);
    // sanity check
    if (size1 <= 0 || size2 < 0 || size1 != size2) {
      return IOEngine.READ_ERROR;
    }
    final long segSize = s.getSegmentDataSize();
    if (size1 > segSize || size2 > segSize) {
      return IOEngine.READ_ERROR;
    }
    if (id != -1) {
      // TODO: sanity check on values
      return IOEngine.READ_ERROR;
    } else { // Block is not compressed
      if (segSize < offset + OPT_META_SIZE + size1) {
        return IOEngine.READ_ERROR;
      }
      // Find key-value in the buffer
      int offAdj = OPT_META_SIZE - META_SIZE;
      long addr = BlockReaderWriterSupport.findInBlock(ptr + offset + offAdj, size1,
        keyPtr, keySize);
      if (addr < 0) {
        return IOEngine.NOT_FOUND;
      }
      size = getItemSize(addr);
      if (size > avail) {
        return size;
      }
      // TODO: remove this?
      if (segSize < offset + size) {
        // Rare situation - wrong segment - hash collision
        return IOEngine.READ_ERROR;
      }
      UnsafeAccess.copy(addr, buffer, bufOffset, size);
    } 
    return size;
  }

  @Override
  public int read(IOEngine engine, long keyPtr, int keySize, int sid, long offset, int size,
      ByteBuffer buffer) {

    // Race condition. Get location from index, get segment (can be different)
    final int avail = buffer.remaining();
    if (size > avail) {
      return size;
    }
    // Segment read lock is already held by this thread
    final Segment s = engine.getSegmentById(sid);
    if (s == null) {
      // TODO: error
      return IOEngine.NOT_FOUND;
    }
    if (!s.isMemory()) {
      return IOEngine.NOT_FOUND;
    }
    final long ptr = s.getAddress();
    final int size1 = UnsafeAccess.toInt(ptr + offset);
    final int id = UnsafeAccess.toInt(ptr + offset + Utils.SIZEOF_INT);
    final int size2 = UnsafeAccess.toInt(ptr + offset + 2 * Utils.SIZEOF_INT);
    // sanity check
    if (size1 <= 0 || size2 < 0 || size1 != size2) {
      return IOEngine.READ_ERROR;
    }
    final long segSize = s.getSegmentDataSize();
    if (size1 > segSize || size2 > segSize) {
      return IOEngine.READ_ERROR;
    }
    if (id != -1) {
      // TODO: sanity check on values
      return IOEngine.READ_ERROR;
    } else  { // Block is not compressed
      // TODO: sanity check on values
      if (segSize < offset + OPT_META_SIZE + size1) {
        return IOEngine.READ_ERROR;
      }
      // Find key-value in the buffer
      int offAdj = OPT_META_SIZE - META_SIZE;
      long addr = BlockReaderWriterSupport.findInBlock(ptr + offset + offAdj, size1, keyPtr, keySize);
      if (addr < 0) {
        return IOEngine.NOT_FOUND;
      }
      size = getItemSize(addr);
      if (size > avail) {
        return size;
      }
      // TODO: remove this?
      if (s.getSegmentDataSize() < offset + size) {
        // Rare situation - wrong segment - hash collision
        return IOEngine.READ_ERROR;
      }
      int pos = buffer.position();
      UnsafeAccess.copy(addr, buffer, size);
      buffer.position(pos);
    } 
    return size;
  }

  @Override
  public int readValueRange(IOEngine engine, byte[] key, int keyOffset, int keySize, int sid,
      long offset, int size, byte[] buffer, int bufOffset, int rangeStart, int rangeSize)
      throws IOException {

    final int avail = buffer.length - bufOffset;
    if (rangeSize > avail) {
      rangeSize = avail;
    }
    // Segment read lock is already held by this thread
    final Segment s = engine.getSegmentById(sid);
    if (s == null) {
      // TODO: error
      return IOEngine.NOT_FOUND;
    }
    if (!s.isMemory()) {
      return IOEngine.NOT_FOUND;
    }
    final long ptr = s.getAddress();
    final int size1 = UnsafeAccess.toInt(ptr + offset);
    final int id = UnsafeAccess.toInt(ptr + offset + Utils.SIZEOF_INT);
    final int size2 = UnsafeAccess.toInt(ptr + offset + 2 * Utils.SIZEOF_INT);
    // sanity check
    // TODO: add to corrupted reads
    if (size1 <= 0 || size2 < 0 || size1 != size2) {
      return IOEngine.READ_ERROR;
    }
    final long segSize = s.getSegmentDataSize();
    if (size1 > segSize || size2 > segSize) {
      return IOEngine.READ_ERROR;
    }
    if (id != -1) {
      // TODO: sanity check on values
      return IOEngine.READ_ERROR;
    } else {
      // TODO: sanity check on values
      if (segSize < offset + OPT_META_SIZE + size1) {
        return IOEngine.READ_ERROR;
      }
      // Find key-value in the buffer
      int offAdj = OPT_META_SIZE - META_SIZE;
      long addr = BlockReaderWriterSupport.findInBlock(ptr + offset + offAdj, size1, key,
        keyOffset, keySize);
      if (addr < 0) {
        return IOEngine.NOT_FOUND;
      }
      int valueSize = Utils.getValueSize(addr);
      if (valueSize < rangeStart) {
        // TODO: better handling
        return IOEngine.NOT_FOUND;
      }
      if (valueSize < rangeStart + rangeSize) {
        rangeSize = valueSize - rangeStart;
      }
      int valueOffset = Utils.getValueOffset(addr);
      valueOffset += rangeStart;
      if (segSize < offset + valueOffset + rangeSize) {
        // Rare situation - wrong segment - hash collision
        return IOEngine.READ_ERROR;
      }
      UnsafeAccess.copy(addr + valueOffset, buffer, bufOffset, rangeSize);
      // Now buffer contains both: key and value, we need to compare keys
      // Format of a key-value pair in a buffer: key-size, value-size, key, value
    } 
    return rangeSize;
  }

  @Override
  public int readValueRange(IOEngine engine, byte[] key, int keyOffset, int keySize, int sid,
      long offset, int size, ByteBuffer buffer, int rangeStart, int rangeSize) throws IOException {
    final int avail = buffer.remaining();
    if (rangeSize > avail) {
      rangeSize = avail;
    }
    // Segment read lock is already held by this thread
    final Segment s = engine.getSegmentById(sid);
    if (s == null) {
      // TODO: error
      return IOEngine.NOT_FOUND;
    }
    if (!s.isMemory()) {
      return IOEngine.NOT_FOUND;
    }
    final long ptr = s.getAddress();
    final int size1 = UnsafeAccess.toInt(ptr + offset);
    final int id = UnsafeAccess.toInt(ptr + offset + Utils.SIZEOF_INT);
    final int size2 = UnsafeAccess.toInt(ptr + offset + 2 * Utils.SIZEOF_INT);
    int pos = buffer.position();
    // TODO: add to corrupted reads
    if (size1 <= 0 || size2 < 0 || size1 != size2) {
      return IOEngine.READ_ERROR;
    }
    final long segSize = s.getSegmentDataSize();
    if (size1 > segSize || size2 > segSize) {
      return IOEngine.READ_ERROR;
    }
    if (id != -1) {
      // TODO: sanity check on values
      return IOEngine.READ_ERROR;
    } else {
      if (segSize < offset + OPT_META_SIZE + size1) {
        return IOEngine.READ_ERROR;
      }
      // Find key-value in the buffer
      int offAdj = OPT_META_SIZE - META_SIZE;
      long addr = BlockReaderWriterSupport.findInBlock(ptr + offset + offAdj, size1, key,
        keyOffset, keySize);
      if (addr < 0) {
        return IOEngine.NOT_FOUND;
      }
      int valueSize = Utils.getValueSize(addr);
      if (valueSize < rangeStart) {
        // TODO: better handling
        return IOEngine.NOT_FOUND;
      }
      if (valueSize < rangeStart + rangeSize) {
        rangeSize = valueSize - rangeStart;
      }
      int valueOffset = Utils.getValueOffset(addr);
      valueOffset += rangeStart;
      if (segSize < offset + valueOffset + rangeSize) {
        // Rare situation - wrong segment - hash collision
        return IOEngine.READ_ERROR;
      }
      UnsafeAccess.copy(addr + valueOffset, buffer, rangeSize);
    }
    buffer.position(pos);
    return rangeSize;
  }

  @Override
  public int readValueRange(IOEngine engine, long keyPtr, int keySize, int sid, long offset,
      int size, byte[] buffer, int bufOffset, int rangeStart, int rangeSize) throws IOException {
    final int avail = buffer.length - bufOffset;
    if (rangeSize > avail) {
      rangeSize = avail;
    }
    // Segment read lock is already held by this thread
    final Segment s = engine.getSegmentById(sid);
    if (s == null) {
      // TODO: error
      return IOEngine.NOT_FOUND;
    }
    if (!s.isMemory()) {
      return IOEngine.NOT_FOUND;
    }
    final long ptr = s.getAddress();
    final int size1 = UnsafeAccess.toInt(ptr + offset);
    final int id = UnsafeAccess.toInt(ptr + offset + Utils.SIZEOF_INT);
    final int size2 = UnsafeAccess.toInt(ptr + offset + 2 * Utils.SIZEOF_INT);
    // TODO: add to corrupted reads
    if (size1 <= 0 || size2 < 0 || size1 != size2) {
      return IOEngine.READ_ERROR;
    }
    final long segSize = s.getSegmentDataSize();
    if (size1 > segSize || size2 > segSize) {
      return IOEngine.READ_ERROR;
    }
    if (id != -1) {
      // TODO: sanity check on values
      return IOEngine.READ_ERROR;
    } else {
      // TODO: sanity check on values
      if (segSize < offset + OPT_META_SIZE + size1) {
        return IOEngine.READ_ERROR;
      }
      // Find key-value in the buffer
      int offAdj = OPT_META_SIZE - META_SIZE;
      long addr = BlockReaderWriterSupport.findInBlock(ptr + offset + offAdj, size1,
        keyPtr, keySize);
      if (addr < 0) {
        return IOEngine.NOT_FOUND;
      }
      int valueSize = Utils.getValueSize(addr);
      if (valueSize < rangeStart) {
        // TODO: better handling
        return IOEngine.NOT_FOUND;
      }
      if (valueSize < rangeStart + rangeSize) {
        rangeSize = valueSize - rangeStart;
      }
      int valueOffset = Utils.getValueOffset(addr);
      valueOffset += rangeStart;
      if (segSize < offset + valueOffset + rangeSize) {
        // Rare situation - wrong segment - hash collision
        return IOEngine.READ_ERROR;
      }
      UnsafeAccess.copy(addr + valueOffset, buffer, bufOffset, rangeSize);
      // Now buffer contains both: key and value, we need to compare keys
      // Format of a key-value pair in a buffer: key-size, value-size, key, value
    } 
    return rangeSize;
  }

  @Override
  public int readValueRange(IOEngine engine, long keyPtr, int keySize, int sid, long offset,
      int size, ByteBuffer buffer, int rangeStart, int rangeSize) throws IOException {
    final int avail = buffer.remaining();
    if (rangeSize > avail) {
      rangeSize = avail;
    }
    // Segment read lock is already held by this thread
    final Segment s = engine.getSegmentById(sid);
    if (s == null) {
      // TODO: error
      return IOEngine.NOT_FOUND;
    }
    if (!s.isMemory()) {
      return IOEngine.NOT_FOUND;
    }
    final long ptr = s.getAddress();
    final int size1 = UnsafeAccess.toInt(ptr + offset);
    final int id = UnsafeAccess.toInt(ptr + offset + Utils.SIZEOF_INT);
    final int size2 = UnsafeAccess.toInt(ptr + offset + 2 * Utils.SIZEOF_INT);
    int pos = buffer.position();
    // TODO: add to corrupted reads
    if (size1 <= 0 || size2 < 0 || size1 != size2) {
      return IOEngine.READ_ERROR;
    }
    long segSize = s.getSegmentDataSize();
    if (size1 > segSize || size2 > segSize) {
      return IOEngine.READ_ERROR;
    }
    if (id != -1) {
      // TODO: sanity check on values
      return IOEngine.READ_ERROR;
    } else {
      // TODO: sanity check on values
      if (segSize < offset + OPT_META_SIZE + size1) {
        return IOEngine.READ_ERROR;
      }
      // Find key-value in the buffer
      int offAdj = OPT_META_SIZE - META_SIZE;
      long addr = BlockReaderWriterSupport.findInBlock(ptr + offset + offAdj, size1,
        keyPtr, keySize);
      if (addr < 0) {
        return IOEngine.NOT_FOUND;
      }
      int valueSize = Utils.getValueSize(addr);
      if (valueSize < rangeStart) {
        // TODO: better handling
        return IOEngine.NOT_FOUND;
      }
      if (valueSize < rangeStart + rangeSize) {
        rangeSize = valueSize - rangeStart;
      }
      int valueOffset = Utils.getValueOffset(addr);
      valueOffset += rangeStart;
      if (segSize < offset + valueOffset + rangeSize) {
        // Rare situation - wrong segment - hash collision
        return IOEngine.READ_ERROR;
      }
      UnsafeAccess.copy(addr + valueOffset, buffer, rangeSize);
    } 
    buffer.position(pos);
    return rangeSize;
  }

  @Override
  public SegmentScanner getSegmentScanner(IOEngine engine, Segment s) throws IOException {
    return new BaseMemorySegmentScanner(s);
  }
}
