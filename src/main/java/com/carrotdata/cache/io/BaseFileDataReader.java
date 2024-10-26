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


import static com.carrotdata.cache.io.BlockReaderWriterSupport.OPT_META_SIZE;
import static com.carrotdata.cache.io.BlockReaderWriterSupport.META_SIZE;
import static com.carrotdata.cache.io.BlockReaderWriterSupport.findInBlock;
import static com.carrotdata.cache.io.IOUtils.readFully;
import static com.carrotdata.cache.util.Utils.getItemSize;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotdata.cache.util.UnsafeAccess;
import com.carrotdata.cache.util.Utils;

public class BaseFileDataReader implements DataReader {
  @SuppressWarnings("unused")
  private static Logger LOG = LoggerFactory.getLogger(BaseFileDataReader.class);

  private static int INIT_BUFFER_SIZE = 1 << 16;

  private static ThreadLocal<byte[]> readBuffers = new ThreadLocal<byte[]>() {
    @Override
    protected byte[] initialValue() {
      return new byte[INIT_BUFFER_SIZE];
    }
  };

  private static void checkReadBuffer(int required) {
    byte[] buf = readBuffers.get();
    if (buf.length < required) {
      buf = new byte[required];
      readBuffers.set(buf);
    }
  }

  private int blockSize = 4096;

  public BaseFileDataReader() {
  }

  @Override
  public void init(String cacheName) {
    // Can be null on initialization
  }
  
  @Override
  public int read(IOEngine engine, byte[] key, int keyOffset, int keySize, int sid, long offset,
      int size, // can be -1 (unknown)
      byte[] buffer, int bufOffset) throws IOException {
    // FIXME: Dirty hack
    offset += Segment.META_SIZE; // add 8 bytes to the file offset
    // every segment in a file system has 8 bytes meta prefix
    final int avail = buffer.length - bufOffset;
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
    int off = 0;
    // Read first block
    // TODO: we can improve read speed if we do 4K aligned reads
    int toRead = (int) Math.min(blockSize, file.length() - offset);
    // Check buffers
    checkReadBuffer(toRead);
    byte[] readBuffer = readBuffers.get();
    readFully(file, offset, readBuffer, 0, toRead);
    final int size1 = UnsafeAccess.toInt(readBuffer, 0);
    final int id = UnsafeAccess.toInt(readBuffer, Utils.SIZEOF_INT);
    final int size2 = UnsafeAccess.toInt(readBuffer, 2 * Utils.SIZEOF_INT);

    if (id != -1 || size1 != size2 || size1 < 0) {
      // sanity check - possible wrong segment request
      return IOEngine.NOT_FOUND;
    } 
    int boff = OPT_META_SIZE;
    int sizeToRead = size1;
    if (sizeToRead > toRead - OPT_META_SIZE) {
      // means that this is a single item larger than a block
      checkReadBuffer(sizeToRead);
      readBuffer = readBuffers.get();
      readFully(file, offset + OPT_META_SIZE, readBuffer, 0, sizeToRead);
      boff = 0;
    }
    int offAdj = boff - META_SIZE;
    off = (int) findInBlock(readBuffer, offAdj, size1, key, keyOffset, keySize);
    if (off < 0) {
      return IOEngine.NOT_FOUND;
    }
    int itemSize = getItemSize(readBuffer, off);
    if (itemSize > avail) {
      return itemSize;
    }
    System.arraycopy(readBuffer, off, buffer, bufOffset, itemSize);
    return itemSize;
  }

  // TODO: tests
  // TODO: handle IOException upstream
  @Override
  public int read(IOEngine engine, byte[] key, int keyOffset, int keySize, int sid, long offset,
      int size, ByteBuffer buffer) throws IOException {
    // FIXME: Dirty hack
    offset += Segment.META_SIZE; // add 8 bytes to
    final int avail = buffer.remaining();
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
    int pos = buffer.position();
    // int off = pos;
    try {
      // TODO: make file read a separate method
      int toRead = (int) Math.min(blockSize, file.length() - offset);
      checkReadBuffer(toRead);
      byte[] readBuffer = readBuffers.get();
      readFully(file, offset, readBuffer, 0, toRead);
      final int size1 = UnsafeAccess.toInt(readBuffer, 0);
      final int id = UnsafeAccess.toInt(readBuffer, Utils.SIZEOF_INT);
      final int size2 = UnsafeAccess.toInt(readBuffer, 2 * Utils.SIZEOF_INT);
      if (id != -1 || size1 != size2 || size1 < 0) {
        return IOEngine.NOT_FOUND;
      } 
      int boff = OPT_META_SIZE;
      int sizeToRead = size1;
      if (sizeToRead > toRead - OPT_META_SIZE) {
        // means that this is a single item larger than a block
        checkReadBuffer(sizeToRead);
        readBuffer = readBuffers.get();
        readFully(file, offset + OPT_META_SIZE, readBuffer, 0, sizeToRead);
        boff = 0;
      }
      int offAdj = boff - META_SIZE;
      int off = (int) findInBlock(readBuffer, offAdj, size1, key, keyOffset, keySize);
      if (off < 0) {
        return IOEngine.NOT_FOUND;
      }
      int itemSize = getItemSize(readBuffer, off);
      if (itemSize > avail) {
        return itemSize;
      }
      buffer.put(readBuffer, off, itemSize);
      return itemSize;
    } finally {
      buffer.position(pos);
    }
  }

  @Override
  public int read(IOEngine engine, long keyPtr, int keySize, int sid, long offset, int size,
      byte[] buffer, int bufOffset) throws IOException {
    // FIXME: Dirty hack
    offset += Segment.META_SIZE; // add 8 bytes to the file offset
    // every segment in a file system has 8 bytes meta prefix
    final int avail = buffer.length - bufOffset;
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
    int off = 0;
    // Read first block
    int toRead = (int) Math.min(blockSize, file.length() - offset);
    // Check buffers
    checkReadBuffer(toRead);
    byte[] readBuffer = readBuffers.get();
    readFully(file, offset, readBuffer, 0, toRead);
    final int size1 = UnsafeAccess.toInt(readBuffer, 0);
    final int id = UnsafeAccess.toInt(readBuffer, Utils.SIZEOF_INT);
    final int size2 = UnsafeAccess.toInt(readBuffer, 2 * Utils.SIZEOF_INT);
    if (id != -1 || size1 != size2 || size1 < 0) {
      return IOEngine.NOT_FOUND;
    } 
    int boff = OPT_META_SIZE;
    int sizeToRead = size1;
    if (sizeToRead > toRead - OPT_META_SIZE) {
      // means that this is a single item larger than a block
      checkReadBuffer(sizeToRead);
      readBuffer = readBuffers.get();
      readFully(file, offset + OPT_META_SIZE, readBuffer, 0, sizeToRead);
      boff = 0;
    }
    int offAdj = boff - META_SIZE;
    off = (int) findInBlock(readBuffer, offAdj, size1, keyPtr, keySize);
    if (off < 0) {
      return IOEngine.NOT_FOUND;
    }
    int itemSize = getItemSize(readBuffer, off);
    if (itemSize > avail) {
      return itemSize;
    }
    System.arraycopy(readBuffer, off, buffer, bufOffset, itemSize);
    return itemSize;
  }

  @Override
  public int read(IOEngine engine, long keyPtr, int keySize, int sid, long offset, int size,
      ByteBuffer buffer) throws IOException {
    // FIXME: Dirty hack
    offset += Segment.META_SIZE; // add 8 bytes to
    final int avail = buffer.remaining();
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
    int pos = buffer.position();
    try {
      // TODO: make file read a separate method
      int toRead = (int) Math.min(blockSize, file.length() - offset);
      checkReadBuffer(toRead);
      byte[] readBuffer = readBuffers.get();
      readFully(file, offset, readBuffer, 0, toRead);
      final int size1 = UnsafeAccess.toInt(readBuffer, 0);
      final int id = UnsafeAccess.toInt(readBuffer, Utils.SIZEOF_INT);
      final int size2 = UnsafeAccess.toInt(readBuffer, 2 * Utils.SIZEOF_INT);
      if (id != -1 || size1 != size2 || size1 < 0) {
        return IOEngine.NOT_FOUND;
      } 
      int boff = OPT_META_SIZE;
      int sizeToRead = size1;
      if (sizeToRead > toRead - OPT_META_SIZE) {
        // means that this is a single item larger than a block
        checkReadBuffer(sizeToRead);
        readBuffer = readBuffers.get();
        readFully(file, offset + OPT_META_SIZE, readBuffer, 0, sizeToRead);
        boff = 0;
      }
      int offAdj = boff - META_SIZE;
      int off = (int) findInBlock(readBuffer, offAdj, size1, keyPtr, keySize);
      if (off < 0) {
        return IOEngine.NOT_FOUND;
      }
      int itemSize = getItemSize(readBuffer, off);
      if (itemSize > avail) {
        return itemSize;
      }
      buffer.put(readBuffer, off, itemSize);
      return itemSize;
    } finally {
      buffer.position(pos);
    }
  }

  @Override
  public int readValueRange(IOEngine engine, byte[] key, int keyOffset, int keySize, int sid,
      long offset, int size, byte[] buffer, int bufferOffset, int rangeStart, int rangeSize)
      throws IOException {
    // FIXME: Dirty hack
    offset += Segment.META_SIZE; // add 8 bytes to the file offset
    // every segment in a file system has 8 bytes meta prefix
    final int avail = buffer.length - bufferOffset;
    if (rangeSize > avail) {
      rangeSize = avail;
    }
    // TODO prevent file from being closed/deleted
    FileIOEngine fileEngine = (FileIOEngine) engine;
    RandomAccessFile file = fileEngine.getFileFor(sid);
    if (file == null) {
      // TODO: what kind of error is it?
      return IOEngine.NOT_FOUND;
    }
    int off = 0;
    // Read first block
    // TODO: we can improve read speed if we do 4K aligned reads
    int toRead = (int) Math.min(blockSize, file.length() - offset);
    // Check buffers
    checkReadBuffer(toRead);
    byte[] readBuffer = readBuffers.get();
    readFully(file, offset, readBuffer, 0, toRead);
    final int size1 = UnsafeAccess.toInt(readBuffer, 0);
    final int id = UnsafeAccess.toInt(readBuffer, Utils.SIZEOF_INT);
    final int size2 = UnsafeAccess.toInt(readBuffer, 2 * Utils.SIZEOF_INT);

    if (id != -1 || size1 != size2 || size1 < 0) {
      // sanity check - possible wrong segment request
      return IOEngine.NOT_FOUND;
    } 
    int boff = OPT_META_SIZE;
    int sizeToRead = size1;
    if (sizeToRead > toRead - OPT_META_SIZE) {
      // means that this is a single item larger than a block
      checkReadBuffer(sizeToRead);
      readBuffer = readBuffers.get();
      readFully(file, offset + OPT_META_SIZE, readBuffer, 0, sizeToRead);
      boff = 0;
    }
    int offAdj = boff - META_SIZE;
    off = (int) findInBlock(readBuffer, offAdj, size1, key, keyOffset, keySize);
    if (off < 0) {
      return IOEngine.NOT_FOUND;
    }
    int valueSize = Utils.getValueSize(readBuffer, off);
    if (valueSize < rangeStart) {
      // TODO: better handling
      return IOEngine.NOT_FOUND;
    }
    if (valueSize < rangeStart + rangeSize) {
      rangeSize = valueSize - rangeStart;
    }
    int valueOffset = Utils.getValueOffset(readBuffer, off);
    valueOffset += rangeStart;
    long fileSize = file.length();
    if (fileSize < offset + valueOffset + rangeSize) {
      // Rare situation - wrong segment - hash collision
      return IOEngine.READ_ERROR;
    }
    UnsafeAccess.copy(readBuffer, off + valueOffset, buffer, 0, rangeSize);
    return rangeSize;
  }

  @Override
  public int readValueRange(IOEngine engine, byte[] key, int keyOffset, int keySize, int sid,
      long offset, int size, ByteBuffer buffer, int rangeStart, int rangeSize) throws IOException {
    // FIXME: Dirty hack
    offset += Segment.META_SIZE; // add 8 bytes to
    final int avail = buffer.remaining();
    // sanity check
    if (rangeSize > avail) {
      rangeSize = avail;
    }
    // TODO prevent file from being closed/deleted
    FileIOEngine fileEngine = (FileIOEngine) engine;
    RandomAccessFile file = fileEngine.getFileFor(sid);
    if (file == null) {
      // TODO: what kind of error is it?
      return IOEngine.NOT_FOUND;
    }
    int pos = buffer.position();
    // int off = pos;
    try {
      // TODO: make file read a separate method
      int toRead = (int) Math.min(blockSize, file.length() - offset);
      checkReadBuffer(toRead);
      byte[] readBuffer = readBuffers.get();
      readFully(file, offset, readBuffer, 0, toRead);
      final int size1 = UnsafeAccess.toInt(readBuffer, 0);
      final int id = UnsafeAccess.toInt(readBuffer, Utils.SIZEOF_INT);
      final int size2 = UnsafeAccess.toInt(readBuffer, 2 * Utils.SIZEOF_INT);
      if (id != -1 || size1 != size2 || size1 < 0) {
        return IOEngine.NOT_FOUND;
      } 
      int boff = OPT_META_SIZE;
      int sizeToRead = size1;
      if (sizeToRead > toRead - OPT_META_SIZE) {
        // means that this is a single item larger than a block
        checkReadBuffer(sizeToRead);
        readBuffer = readBuffers.get();
        readFully(file, offset + OPT_META_SIZE, readBuffer, 0, sizeToRead);
        boff = 0;
      }
      int offAdj = boff - META_SIZE;
      int off = (int) findInBlock(readBuffer, offAdj, size1, key, keyOffset, keySize);
      if (off < 0) {
        return IOEngine.NOT_FOUND;
      }
      int valueSize = Utils.getValueSize(readBuffer, off);
      if (valueSize < rangeStart) {
        // TODO: better handling
        return IOEngine.NOT_FOUND;
      }
      if (valueSize < rangeStart + rangeSize) {
        rangeSize = valueSize - rangeStart;
      }
      int valueOffset = Utils.getValueOffset(readBuffer, off);
      valueOffset += rangeStart;
      long fileLength = file.length();
      if (fileLength < offset + valueOffset + rangeSize) {
        // Rare situation - wrong segment - hash collision
        return IOEngine.READ_ERROR;
      }
      UnsafeAccess.copy(readBuffer, off + valueOffset, buffer, pos, rangeSize);
      return rangeSize;
    } finally {
      buffer.position(pos);
    }
  }

  @Override
  public int readValueRange(IOEngine engine, long keyPtr, int keySize, int sid, long offset,
      int size, byte[] buffer, int bufferOffset, int rangeStart, int rangeSize) throws IOException {
    // FIXME: Dirty hack
    offset += Segment.META_SIZE; // add 8 bytes to the file offset
    // every segment in a file system has 8 bytes meta prefix
    final int avail = buffer.length - bufferOffset;
    if (rangeSize > avail) {
      rangeSize = avail;
    }
    // TODO prevent file from being closed/deleted
    FileIOEngine fileEngine = (FileIOEngine) engine;
    RandomAccessFile file = fileEngine.getFileFor(sid);
    if (file == null) {
      // TODO: what kind of error is it?
      return IOEngine.NOT_FOUND;
    }
    int off = 0;
    // Read first block
    // TODO: we can improve read speed if we do 4K aligned reads
    int toRead = (int) Math.min(blockSize, file.length() - offset);
    // Check buffers
    checkReadBuffer(toRead);
    byte[] readBuffer = readBuffers.get();
    readFully(file, offset, readBuffer, 0, toRead);
    final int size1 = UnsafeAccess.toInt(readBuffer, 0);
    final int id = UnsafeAccess.toInt(readBuffer, Utils.SIZEOF_INT);
    final int size2 = UnsafeAccess.toInt(readBuffer, 2 * Utils.SIZEOF_INT);

    if (id != -1 || size1 != size2 || size1 < 0) {
      // sanity check - possible wrong segment request
      return IOEngine.NOT_FOUND;
    } 
    int boff = OPT_META_SIZE;
    int sizeToRead = size1;
    if (sizeToRead > toRead - OPT_META_SIZE) {
      // means that this is a single item larger than a block
      checkReadBuffer(sizeToRead);
      readBuffer = readBuffers.get();
      readFully(file, offset + OPT_META_SIZE, readBuffer, 0, sizeToRead);
      boff = 0;
    }
    int offAdj = boff - META_SIZE;
    off = (int) findInBlock(readBuffer, offAdj, size1, keyPtr, keySize);
    if (off < 0) {
      return IOEngine.NOT_FOUND;
    }
    int valueSize = Utils.getValueSize(readBuffer, off);
    if (valueSize < rangeStart) {
      // TODO: better handling
      return IOEngine.NOT_FOUND;
    }
    if (valueSize < rangeStart + rangeSize) {
      rangeSize = valueSize - rangeStart;
    }
    int valueOffset = Utils.getValueOffset(readBuffer, off);
    valueOffset += rangeStart;
    long fileSize = file.length();
    if (fileSize < offset + valueOffset + rangeSize) {
      // Rare situation - wrong segment - hash collision
      return IOEngine.READ_ERROR;
    }
    UnsafeAccess.copy(readBuffer, off + valueOffset, buffer, 0, rangeSize);
    return rangeSize;
  }

  @Override
  public int readValueRange(IOEngine engine, long keyPtr, int keySize, int sid, long offset,
      int size, ByteBuffer buffer, int rangeStart, int rangeSize) throws IOException {
    // FIXME: Dirty hack
    offset += Segment.META_SIZE; // add 8 bytes to
    final int avail = buffer.remaining();
    // sanity check
    if (rangeSize > avail) {
      rangeSize = avail;
    }
    // TODO prevent file from being closed/deleted
    FileIOEngine fileEngine = (FileIOEngine) engine;
    RandomAccessFile file = fileEngine.getFileFor(sid);
    if (file == null) {
      // TODO: what kind of error is it?
      return IOEngine.NOT_FOUND;
    }
    int pos = buffer.position();
    // int off = pos;
    try {
      // TODO: make file read a separate method
      int toRead = (int) Math.min(blockSize, file.length() - offset);
      checkReadBuffer(toRead);
      byte[] readBuffer = readBuffers.get();
      readFully(file, offset, readBuffer, 0, toRead);
      final int size1 = UnsafeAccess.toInt(readBuffer, 0);
      final int id = UnsafeAccess.toInt(readBuffer, Utils.SIZEOF_INT);
      final int size2 = UnsafeAccess.toInt(readBuffer, 2 * Utils.SIZEOF_INT);
      if (id != -1 || size1 != size2 || size1 < 0) {
        return IOEngine.NOT_FOUND;
      } 
      int boff = OPT_META_SIZE;
      int sizeToRead = size1;
      if (sizeToRead > toRead - OPT_META_SIZE) {
        // means that this is a single item larger than a block
        checkReadBuffer(sizeToRead);
        readBuffer = readBuffers.get();
        readFully(file, offset + OPT_META_SIZE, readBuffer, 0, sizeToRead);
        boff = 0;
      }
      int offAdj = boff - META_SIZE;
      int off = (int) findInBlock(readBuffer, offAdj, size1, keyPtr, keySize);
      if (off < 0) {
        return IOEngine.NOT_FOUND;
      }
      int valueSize = Utils.getValueSize(readBuffer, off);
      if (valueSize < rangeStart) {
        // TODO: better handling
        return IOEngine.NOT_FOUND;
      }
      if (valueSize < rangeStart + rangeSize) {
        rangeSize = valueSize - rangeStart;
      }
      int valueOffset = Utils.getValueOffset(readBuffer, off);
      valueOffset += rangeStart;
      long fileLength = file.length();
      if (fileLength < offset + valueOffset + rangeSize) {
        // Rare situation - wrong segment - hash collision
        return IOEngine.READ_ERROR;
      }
      UnsafeAccess.copy(readBuffer, off + valueOffset, buffer, pos, rangeSize);
      return rangeSize;
    } finally {
      buffer.position(pos);
    }
  }

  @Override
  public SegmentScanner getSegmentScanner(IOEngine engine, Segment s) throws IOException {
    RandomAccessFile file = ((FileIOEngine) engine).getFileFor(s.getId());
    int prefetchBuferSize = ((FileIOEngine) engine).getFilePrefetchBufferSize();
    return new BaseFileSegmentScanner(s, file, prefetchBuferSize);
  }
}
