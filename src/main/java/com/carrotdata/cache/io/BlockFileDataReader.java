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
package com.carrotdata.cache.io;

import static com.carrotdata.cache.io.BlockReaderWriterSupport.META_SIZE;
import static com.carrotdata.cache.io.BlockReaderWriterSupport.findInBlock;
import static com.carrotdata.cache.io.IOUtils.readFully;
import static com.carrotdata.cache.util.Utils.getItemSize;
import static com.carrotdata.cache.util.Utils.getKeyOffset;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.carrotdata.cache.util.CacheConfig;
import com.carrotdata.cache.util.UnsafeAccess;
import com.carrotdata.cache.util.Utils;

public class BlockFileDataReader implements DataReader {
  
  private ConcurrentLinkedQueue<byte[]> buffers = new ConcurrentLinkedQueue<>();
  
  private int blockSize;
  
  @Override
  public void init(String cacheName) {
    this.blockSize = CacheConfig.getInstance().getBlockWriterBlockSize(cacheName);      
  }

  @Override
  public int read(
      IOEngine engine,
      byte[] key,
      int keyOffset,
      int keySize,
      int sid,
      long offset,
      int size, // can be -1 (unknown)
      byte[] buffer,
      int bufOffset)
      throws IOException {

    // FIXME: Dirty hack
    offset += Segment.META_SIZE; // add 8 bytes to the file offset
    // every segment in a file system has 8 bytes meta prefix

    int avail = buffer.length - bufOffset;
    // sanity check
    if (size > avail) {
      return (size / blockSize + 1) * blockSize;
    }

    if (avail < blockSize) {
      // We need at least blockSize available space 
      return blockSize;
    }
    // TODO prevent file from being closed/deleted
    FileIOEngine fileEngine = (FileIOEngine) engine;
    RandomAccessFile file = fileEngine.getFileFor(sid);
    if (file == null) {
      // TODO: what kind of error is it?
      return IOEngine.NOT_FOUND;
    }
    if (size > 0 && file.length() < offset + size) {
      // Rare situation - wrong segment - hash collision
      return IOEngine.NOT_FOUND;
    }

    int off = 0;
    // Read first block
    int toRead =(int) Math.min(blockSize, file.length() - offset);
    readFully(file, offset, buffer, bufOffset, toRead);

    int dataSize = UnsafeAccess.toInt(buffer, bufOffset);
    if (dataSize > blockSize - META_SIZE) {
      // means that this is a single item larger than a block
      if (dataSize + META_SIZE > avail) {
        return dataSize + META_SIZE;
      }
      readFully(file, offset + blockSize, buffer, bufOffset + blockSize, dataSize - blockSize + META_SIZE);
    }

    off = (int) findInBlock(buffer, bufOffset, key, keyOffset, keySize);
    if (off < 0) {
      return IOEngine.NOT_FOUND;
    }
    int itemSize = getItemSize(buffer, off);
    System.arraycopy(buffer, off, buffer, bufOffset, itemSize);
    return itemSize;
  }

  // TODO: tests
  // TODO: handle IOException upstream
  @Override
  public int read(
      IOEngine engine,
      byte[] key,
      int keyOffset,
      int keySize,
      int sid,
      long offset,
      int size,
      ByteBuffer buffer)
      throws IOException {
    // FIXME: Dirty hack
    offset += Segment.META_SIZE; // add 8 bytes to

    int avail = buffer.remaining();
    // sanity check
    if (size > avail) {
      return size;
    }
    // TODO prevent file from being closed/deleted
    FileIOEngine fileEngine = (FileIOEngine) engine;
    RandomAccessFile file = fileEngine.getFileFor(sid);
    if (file == null) {
      // TODO: what kind of error is it?
      return IOEngine.NOT_FOUND;
    }
    if (size > 0 && file.length() < offset + size) {
      // Rare situation - wrong segment - hash collision
      return IOEngine.NOT_FOUND;
    }

    byte[] buf = getBuffer();
    int pos = buffer.position();
    int off = pos;
    boolean releaseBuffer = true;
    try {
      // TODO: make file read a separate method
      int toRead = (int) Math.min(buf.length, file.length() - offset);
      readFully(file, offset, buf, 0, toRead);

      int dataSize = UnsafeAccess.toInt(buf, 0);
      if (dataSize > blockSize - META_SIZE) {
        // means that this is a single item larger than a block
        if (dataSize + META_SIZE > avail) {
          return dataSize + META_SIZE;
        }

        // couple bloats
        byte[] bbuf = new byte[dataSize + META_SIZE];

        System.arraycopy(buf, 0, bbuf, 0, blockSize);
        releaseBuffer(buf);
        releaseBuffer = false;
        buf = bbuf;
        readFully(file, offset + blockSize, buf, blockSize, dataSize - blockSize + META_SIZE);
      }
      // Now buffer contains both: key and value, we need to compare keys
      // Format of a key-value pair in a buffer: key-size, value-size, key, value
      off = (int) findInBlock(buf, 0, key, keyOffset, keySize);
      if (off < 0) {
        return IOEngine.NOT_FOUND;
      }

      int itemSize = getItemSize(buf, off);
      int $off = getKeyOffset(buf, off);
      off += $off;

      // Now compare keys
      if (Utils.compareTo(buf, off, keySize, key, keyOffset, keySize) == 0) {
        // If key is the same copy K-V to buffer
        off -= $off;
        buffer.put(buf, off, itemSize);
        // TODO:rewind to start?
        return itemSize;
      } else {
        return IOEngine.NOT_FOUND;
      }

    } finally {
      if (buf != null && releaseBuffer) {
        releaseBuffer(buf);
      }
      buffer.position(pos);
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
      int bufOffset) throws IOException {
    // FIXME: Dirty hack
    offset += Segment.META_SIZE; // add 8 bytes to the file offset
    // every segment in a file system has 8 bytes meta prefix

    int avail = buffer.length - bufOffset;
    // sanity check
    if (size > avail) {
      return (size / blockSize + 1) * blockSize;
    }

    if (avail < blockSize) {
      // We need at least blockSize available space 
      return blockSize;
    }
    // TODO prevent file from being closed/deleted
    FileIOEngine fileEngine = (FileIOEngine) engine;
    RandomAccessFile file = fileEngine.getFileFor(sid);
    if (file == null) {
      // TODO: what kind of error is it?
      return IOEngine.NOT_FOUND;
    }
    if (size > 0 && file.length() < offset + size) {
      // Rare situation - wrong segment - hash collision
      return IOEngine.NOT_FOUND;
    }

    int off = 0;
    // Read first block
    int toRead = (int) Math.min(blockSize, file.length() - offset);
    readFully(file, offset, buffer, bufOffset, toRead);

    int dataSize = UnsafeAccess.toInt(buffer, bufOffset);
    if (dataSize > blockSize - META_SIZE) {
      // means that this is a single item larger than a block
      if (dataSize + META_SIZE > avail) {
        return dataSize + META_SIZE;
      }
      readFully(file, offset + blockSize, buffer, bufOffset + blockSize, dataSize - blockSize + META_SIZE);
    }

    off = (int) findInBlock(buffer, bufOffset, keyPtr, keySize);
    if (off < 0) {
      return IOEngine.NOT_FOUND;
    }
    int itemSize = getItemSize(buffer, off);
    System.arraycopy(buffer, off, buffer, bufOffset, itemSize);
    return itemSize;

  }

  @Override
  public int read(
      IOEngine engine, long keyPtr, int keySize, int sid, long offset, int size, ByteBuffer buffer)
      throws IOException {
    // FIXME: Dirty hack
    offset += Segment.META_SIZE; // add 8 bytes to
    int avail = buffer.remaining();
    // sanity check
    if (size > avail) {
      return size;
    }
    // TODO prevent file from being closed/deleted
    FileIOEngine fileEngine = (FileIOEngine) engine;
    RandomAccessFile file = fileEngine.getFileFor(sid);
    if (file == null) {
      // TODO: what kind of error is it?
      return IOEngine.NOT_FOUND;
    }
    if (file.length() < offset + size) {
      // Rare situation - wrong segment - hash collision
      return IOEngine.NOT_FOUND;
    }

    byte[] buf = getBuffer();
    int pos = buffer.position();
    int off = pos;
    boolean releaseBuffer = true;
    try {
      // TODO: make file read a separate method
      int toRead = (int) Math.min(buf.length, file.length() - offset);
      readFully(file, offset, buf, 0, toRead);
      int dataSize = UnsafeAccess.toInt(buf, 0);
      if (dataSize > blockSize - META_SIZE) {
        // means that this is a single item larger than a block
        if (dataSize + META_SIZE > avail) {
          return dataSize + META_SIZE;
        }
        // couple bloats
        byte[] bbuf = new byte[dataSize + META_SIZE];
        System.arraycopy(buf, 0, bbuf, 0, blockSize);
        releaseBuffer(buf);
        releaseBuffer = false;
        buf = bbuf;
        readFully(file, offset + blockSize, buf, blockSize, dataSize - blockSize + META_SIZE);
      }
      // Now buffer contains both: key and value, we need to compare keys
      // Format of a key-value pair in a buffer: key-size, value-size, key, value
      off = (int) findInBlock(buf, 0, keyPtr, keySize);
      if (off < 0) {
        return IOEngine.NOT_FOUND;
      }

      int itemSize = getItemSize(buf, off);
      int $off = getKeyOffset(buf, off);
      off += $off;

      // Now compare keys
      if (Utils.compareTo(buf, off, keySize, keyPtr, keySize) == 0) {
        // If key is the same copy K-V to buffer
        off -= $off;
        buffer.put(buf, off, itemSize);
        // TODO:rewind to start?
        return itemSize;
      } else {
        return IOEngine.NOT_FOUND;
      }

    } finally {
      if (buf != null && releaseBuffer) {
        releaseBuffer(buf);
      }
      buffer.position(pos);
    }
  }

  private byte[] getBuffer() {
    byte[] buffer = buffers.poll();
    if (buffer == null) {
      buffer = new byte[blockSize];
    }
    return buffer;
  }
  
  private void releaseBuffer(byte[] buffer) {
    buffers.offer(buffer);
  }

  @Override
  public SegmentScanner getSegmentScanner(IOEngine engine, Segment s) throws IOException {
    String cacheName = engine.getCacheName();
    int blockSize = CacheConfig.getInstance().getBlockWriterBlockSize(cacheName);
    return new BlockFileSegmentScanner(s, (FileIOEngine) engine, blockSize);
  }
}
