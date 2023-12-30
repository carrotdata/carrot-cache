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

import static com.carrot.cache.compression.CompressionCodec.COMP_META_SIZE;
import static com.carrot.cache.compression.CompressionCodec.COMP_SIZE_OFFSET;
import static com.carrot.cache.compression.CompressionCodec.DICT_VER_OFFSET;
import static com.carrot.cache.compression.CompressionCodec.SIZE_OFFSET;
import static com.carrot.cache.io.BlockReaderWriterSupport.META_SIZE;
import static com.carrot.cache.io.BlockReaderWriterSupport.findInBlock;
import static com.carrot.cache.io.IOUtils.readFully;
import static com.carrot.cache.util.Utils.getItemSize;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

import com.carrot.cache.compression.CodecFactory;
import com.carrot.cache.compression.CompressionCodec;
import com.carrot.cache.util.UnsafeAccess;

public class CompressedBlockFileDataReader implements DataReader {
  
 private static int INIT_BUFFER_SIZE = 1 << 16;
  
  private static ThreadLocal<byte[]> compBuffers = new ThreadLocal<byte[]>() {
    @Override
    protected byte[] initialValue() {
      return new byte[INIT_BUFFER_SIZE];
    }
  };
  
  private static ThreadLocal<byte[]> readBuffers = new ThreadLocal<byte[]>() {
    @Override
    protected byte[] initialValue() {
      return new byte[INIT_BUFFER_SIZE];
    }
  };
  
  private static void checkCompBuffer(int required) {
    byte[] buf = compBuffers.get();
    if (buf.length < required) {
      buf = new byte[required];
      compBuffers.set(buf);
    }
  }
  
  private static void checkReadBuffer(int required) {
    byte[] buf = readBuffers.get();
    if (buf.length < required) {
      buf = new byte[required];
      readBuffers.set(buf);
    }
  }
  
  private String cacheName;
  
  private CompressionCodec codec;
  
  private int blockSize = 4096;
  
  @Override
  public void init(String cacheName) {
    // Can be null on initialization
    this.codec = CodecFactory.getInstance().getCompressionCodecForCache(cacheName);
    this.cacheName = cacheName;
  }
  
  private void checkCodec() {
    if (this.codec == null) {
      this.codec = CodecFactory.getInstance().getCompressionCodecForCache(cacheName);
      if (this.codec == null) {
        throw new RuntimeException(String.format("Codec type is undefined for cache \'%s'", cacheName));
      }
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
      int size, // can be -1 (unknown)
      byte[] buffer,
      int bufOffset)
      throws IOException {
    checkCodec();
    // FIXME: Dirty hack
    offset += Segment.META_SIZE; // add 8 bytes to the file offset
    // every segment in a file system has 8 bytes meta prefix

    int avail = buffer.length - bufOffset;
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
    int toRead =(int) Math.min(blockSize, file.length() - offset);
    // Check buffers
    checkReadBuffer(toRead);
    byte[] readBuffer = readBuffers.get();
    readFully(file, offset, readBuffer, 0, toRead);
    int decompressedSize = UnsafeAccess.toInt(readBuffer, SIZE_OFFSET);
    int compSize = UnsafeAccess.toInt(readBuffer, COMP_SIZE_OFFSET);
    int dictId = UnsafeAccess.toInt(readBuffer, DICT_VER_OFFSET);
    int boff = COMP_META_SIZE;
    if (compSize > toRead - COMP_META_SIZE) {
      // means that this is a single item larger than a block
      checkReadBuffer(compSize);
      readBuffer = readBuffers.get();
      readFully(file, offset + COMP_META_SIZE, readBuffer, 0, compSize);
      boff = 0;
    }
    
    checkCompBuffer(decompressedSize);
    byte[] compBuffer = compBuffers.get();
    
    if (dictId >= 0) {
    int s = codec.decompress(readBuffer, boff, compSize, compBuffer, dictId);
      if (s == 0) {
        return IOEngine.NOT_FOUND;
      }
    } else {
      UnsafeAccess.copy(readBuffer, boff, compBuffer, 0, decompressedSize);
    }
    int offAdj = -META_SIZE;
    off = (int) findInBlock(compBuffer, offAdj, decompressedSize, key, keyOffset, keySize);
    if (off < 0) {
      return IOEngine.NOT_FOUND;
    }
    int itemSize = getItemSize(compBuffer, off);
    if (itemSize > avail) {
      return itemSize;
    }
    System.arraycopy(compBuffer, off, buffer, bufOffset, itemSize);
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
    checkCodec();
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

    int pos = buffer.position();
    //int off = pos;
    try {
      // TODO: make file read a separate method
      int toRead = (int) Math.min(blockSize, file.length() - offset);
      checkReadBuffer(toRead);
      byte[] readBuffer = readBuffers.get();
      readFully(file, offset, readBuffer, 0, toRead);

      int decompressedSize = UnsafeAccess.toInt(readBuffer, SIZE_OFFSET);
      int compSize = UnsafeAccess.toInt(readBuffer, COMP_SIZE_OFFSET);
      int dictId = UnsafeAccess.toInt(readBuffer, DICT_VER_OFFSET);
      int boff = COMP_META_SIZE;
      if (compSize > toRead - COMP_META_SIZE) {
        // means that this is a single item larger than a block
        checkReadBuffer(compSize);
        readBuffer = readBuffers.get();
        readFully(file, offset + COMP_META_SIZE, readBuffer, 0, compSize);
        boff = 0;
      }
      
      checkCompBuffer(decompressedSize);
      byte[] compBuffer = compBuffers.get();
      
      if (dictId >= 0) {
        int s = codec.decompress(readBuffer, boff, compSize, compBuffer, dictId);
        if (s == 0) {
          return IOEngine.NOT_FOUND;
        }
      } else {
        UnsafeAccess.copy(readBuffer, boff, compBuffer, 0, decompressedSize);
      }
      int offAdj = -META_SIZE;

      int off = (int) findInBlock(compBuffer, offAdj, decompressedSize, key, keyOffset, keySize);
      if (off < 0) {
        return IOEngine.NOT_FOUND;
      }
      int itemSize = getItemSize(compBuffer, off);
      if (itemSize > avail) {
        return itemSize;
      }
      buffer.put(compBuffer, off, itemSize);
      return itemSize;
    } finally {
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
    checkCodec();
    // FIXME: Dirty hack
    offset += Segment.META_SIZE; // add 8 bytes to the file offset
    // every segment in a file system has 8 bytes meta prefix

    int avail = buffer.length - bufOffset;
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
    int toRead =(int) Math.min(blockSize, file.length() - offset);
    // Check buffers
    checkReadBuffer(toRead);
    byte[] readBuffer = readBuffers.get();
    readFully(file, offset, readBuffer, 0, toRead);
    int decompressedSize = UnsafeAccess.toInt(readBuffer, SIZE_OFFSET);
    int compSize = UnsafeAccess.toInt(readBuffer, COMP_SIZE_OFFSET);
    int dictId = UnsafeAccess.toInt(readBuffer, DICT_VER_OFFSET);
    int boff = COMP_META_SIZE;
    if (compSize > toRead - COMP_META_SIZE) {
      // means that this is a single item larger than a block
      checkReadBuffer(compSize);
      readBuffer = readBuffers.get();
      readFully(file, offset + COMP_META_SIZE, readBuffer, 0, compSize);
      boff = 0;
    }
    
    checkCompBuffer(decompressedSize);
    byte[] compBuffer = compBuffers.get();
    
    if (dictId >= 0) {
    int s = codec.decompress(readBuffer, boff, compSize, compBuffer, dictId);
      if (s == 0) {
        return IOEngine.NOT_FOUND;
      }
    } else {
      UnsafeAccess.copy(readBuffer, boff, compBuffer, 0, decompressedSize);
    }
    int offAdj = -META_SIZE;
    off = (int) findInBlock(compBuffer, offAdj, decompressedSize, keyPtr, keySize);
    if (off < 0) {
      return IOEngine.NOT_FOUND;
    }
    int itemSize = getItemSize(compBuffer, off);
    if (itemSize > avail) {
      return itemSize;
    }
    System.arraycopy(compBuffer, off, buffer, bufOffset, itemSize);
    return itemSize;

  }

  @Override
  public int read(
      IOEngine engine, long keyPtr, int keySize, int sid, long offset, int size, ByteBuffer buffer)
      throws IOException {
    checkCodec();
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

    int pos = buffer.position();
    //int off = pos;
    try {
      // TODO: make file read a separate method
      int toRead = (int) Math.min(blockSize, file.length() - offset);
      checkReadBuffer(toRead);
      byte[] readBuffer = readBuffers.get();
      readFully(file, offset, readBuffer, 0, toRead);

      int decompressedSize = UnsafeAccess.toInt(readBuffer, SIZE_OFFSET);
      int compSize = UnsafeAccess.toInt(readBuffer, COMP_SIZE_OFFSET);
      int dictId = UnsafeAccess.toInt(readBuffer, DICT_VER_OFFSET);
      int boff = COMP_META_SIZE;
      if (compSize > toRead - COMP_META_SIZE) {
        // means that this is a single item larger than a block
        checkReadBuffer(compSize);
        readBuffer = readBuffers.get();
        readFully(file, offset + COMP_META_SIZE, readBuffer, 0, compSize);
        boff = 0;
      }
      
      checkCompBuffer(decompressedSize);
      byte[] compBuffer = compBuffers.get();
      
      if (dictId >= 0) {
        int s = codec.decompress(readBuffer, boff, compSize, compBuffer, dictId);
        if (s == 0) {
          return IOEngine.NOT_FOUND;
        }
      } else {
        UnsafeAccess.copy(readBuffer, boff, compBuffer, 0, decompressedSize);
      }
      int offAdj = -META_SIZE;

      int off = (int) findInBlock(compBuffer, offAdj, decompressedSize, keyPtr, keySize);
      if (off < 0) {
        return IOEngine.NOT_FOUND;
      }
      int itemSize = getItemSize(compBuffer, off);
      if (itemSize > avail) {
        return itemSize;
      }
      buffer.put(compBuffer, off, itemSize);
      return itemSize;
    } finally {
      buffer.position(pos);
    }
  }


  @Override
  public SegmentScanner getSegmentScanner(IOEngine engine, Segment s) throws IOException {
    return new CompressedBlockFileSegmentScanner(s, (FileIOEngine) engine, this.codec);
  }
}
