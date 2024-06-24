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

import static com.carrotdata.cache.compression.CompressionCodec.COMP_META_SIZE;
import static com.carrotdata.cache.compression.CompressionCodec.COMP_SIZE_OFFSET;
import static com.carrotdata.cache.compression.CompressionCodec.DICT_VER_OFFSET;
import static com.carrotdata.cache.compression.CompressionCodec.SIZE_OFFSET;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotdata.cache.compression.CompressionCodec;
import com.carrotdata.cache.util.UnsafeAccess;
import com.carrotdata.cache.util.Utils;

/**
 * Segment scanner Usage: while(scanner.hasNext()){ // do job // ... // next() scanner.next(); }
 */
public final class CompressedBlockFileSegmentScanner implements SegmentScanner {
  private static final Logger LOG =
      LoggerFactory.getLogger(CompressedBlockFileSegmentScanner.class);

  /*
   * Data segment
   */
  Segment segment;

  /*
   * Prefetch buffer
   */
  PrefetchBuffer prefetch;

  /*
   * Current scanner index
   */
  int currentIndex = 0;

  /**
   * Current offset in the current block
   */
  int blockOffset = 0;

  /**
   * Current block size (decompressed)
   */
  int blockSize = 4096;

  /**
   * Internal buffer
   */
  byte[] buf;

  /**
   * Compression codec
   */
  private CompressionCodec codec;

  /*
   * Private constructor
   */
  CompressedBlockFileSegmentScanner(Segment s, FileIOEngine engine, CompressionCodec codec)
      throws IOException {
    // Make sure it is sealed
    if (s.isSealed() == false) {
      throw new RuntimeException("segment is not sealed");
    }
    this.segment = s;
    s.readLock();
    // Allocate internal buffer
    int bufferSize = 1 << 16;
    buf = new byte[bufferSize];
    this.codec = codec;
    RandomAccessFile file = engine.getFileFor(s.getId());
    if (file == null) {
      String fileName = engine.getSegmentFileName(s.getId());
      throw new IOException(String.format("File %s does not exists", fileName));
    }
    int bufSize = engine.getFilePrefetchBufferSize();
    this.prefetch = new PrefetchBuffer(file, bufSize);
    nextBlock();

  }

  private void checkBuffer(int requiredSize) {
    if (requiredSize <= buf.length) {
      return;
    }
    this.buf = new byte[requiredSize];
  }

  private void nextBlock() throws IOException {
    if (currentIndex >= segment.getTotalItems()) {
      return;
    }
    byte[] buffer = prefetch.getBuffer();
    int bufferOffset = prefetch.getBufferOffset();
    // next blockSize
    if (this.prefetch.available() <= COMP_META_SIZE) {
      this.prefetch.prefetch();
      bufferOffset = 0;
    }
    this.blockSize = UnsafeAccess.toInt(buffer, bufferOffset + SIZE_OFFSET);
    int dictId = UnsafeAccess.toInt(buffer, bufferOffset + DICT_VER_OFFSET);
    int compSize = UnsafeAccess.toInt(buffer, bufferOffset + COMP_SIZE_OFFSET);
    if (this.prefetch.available() < compSize + COMP_META_SIZE) {
      this.prefetch.prefetch();
      bufferOffset = 0;
    }

    checkBuffer(this.blockSize);
    if (dictId >= 0) {
      this.codec.decompress(buffer, bufferOffset + COMP_META_SIZE, compSize, this.buf, dictId);
    } else if (dictId == -1) {
      UnsafeAccess.copy(buffer, bufferOffset + COMP_META_SIZE, this.buf, 0, this.blockSize);
    } else {
      // PANIC - memory corruption
      LOG.error(
        "Segment size={} offset={} uncompressed={} compressed={} dictId={} index={} total items={}",
        segment.getSegmentDataSize(), this.prefetch.getFileOffset(), this.blockSize, compSize,
        dictId, currentIndex, segment.getTotalItems());
      Thread.dumpStack();
      throw new RuntimeException();
      //System.exit(-1);
    }
    // Advance segment offset
    this.prefetch.advance(compSize + COMP_META_SIZE);

    this.blockOffset = 0;
  }

  public boolean hasNext() {
    return currentIndex < segment.getTotalItems();
  }

  public boolean next() throws IOException {

    int keySize = Utils.readUVInt(buf, this.blockOffset);
    int keySizeSize = Utils.sizeUVInt(keySize);
    this.blockOffset += keySizeSize;
    int valueSize = Utils.readUVInt(buf, this.blockOffset);
    int valueSizeSize = Utils.sizeUVInt(valueSize);
    this.blockOffset += valueSizeSize;
    this.blockOffset += keySize + valueSize;
    this.currentIndex++;
    if (this.blockOffset >= this.blockSize) {
      nextBlock();
    }
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
    return Utils.readUVInt(this.buf, this.blockOffset);
  }

  /**
   * Get current value size
   * @return value size
   */

  public final int valueLength() {
    int off = this.blockOffset;
    int keySize = Utils.readUVInt(this.buf, off);
    int keySizeSize = Utils.sizeUVInt(keySize);
    off += keySizeSize;
    return Utils.readUVInt(this.buf, off);
  }

  /**
   * Get current key's address
   * @return keys address or 0 (if not supported)
   */
  public final long keyAddress() {
    return 0;
  }

  private final int keyOffset() {
    int off = this.blockOffset;
    int keySize = Utils.readUVInt(this.buf, off);
    int keySizeSize = Utils.sizeUVInt(keySize);
    off += keySizeSize;
    int valueSize = Utils.readUVInt(this.buf, off);
    int valueSizeSize = Utils.sizeUVInt(valueSize);
    off += valueSizeSize;
    return off;
  }

  /**
   * Get current value's address
   * @return values address
   */
  public final long valueAddress() {
    return 0;
  }

  private final int valueOffset() {
    int off = this.blockOffset;
    int keySize = Utils.readUVInt(this.buf, off);
    int keySizeSize = Utils.sizeUVInt(keySize);
    off += keySizeSize;
    int valueSize = Utils.readUVInt(this.buf, off);
    int valueSizeSize = Utils.sizeUVInt(valueSize);
    off += valueSizeSize + keySize;
    return off;
  }

  @Override
  public void close() throws IOException {
    segment.readUnlock();
  }

  @Override
  public int getKey(ByteBuffer b) {
    int keySize = keyLength();
    int keyOffset = keyOffset();
    if (keySize <= b.remaining()) {
      b.put(this.buf, keyOffset, keySize);
    }
    return keySize;
  }

  @Override
  public int getValue(ByteBuffer b) {
    int valueSize = valueLength();
    int valueOffset = valueOffset();
    if (valueSize <= b.remaining()) {
      b.put(this.buf, valueOffset, valueSize);
    }
    return valueSize;
  }

  @Override
  public int getKey(byte[] buffer, int offset) throws IOException {
    int keySize = keyLength();
    if (keySize > buffer.length - offset) {
      return keySize;
    }
    int keyOffset = keyOffset();
    System.arraycopy(this.buf, keyOffset, buffer, offset, keySize);
    return keySize;
  }

  @Override
  public int getValue(byte[] buffer, int offset) throws IOException {
    int valueSize = valueLength();
    if (valueSize > buffer.length - offset) {
      return valueSize;
    }
    int valueOffset = valueOffset();
    System.arraycopy(this.buf, valueOffset, buffer, offset, valueSize);
    return valueSize;
  }

  @Override
  public Segment getSegment() {
    return this.segment;
  }

  @Override
  public long getOffset() {
    return this.prefetch.getFileOffset();
  }

  @Override
  public boolean isDirect() {
    return false;
  }
}
