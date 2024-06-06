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
import static com.carrotdata.cache.io.BlockReaderWriterSupport.getBlockDataSize;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

import com.carrotdata.cache.util.Utils;

public final class BlockFileSegmentScanner implements SegmentScanner {

  /** Data segment */
  Segment segment;
  /** File */
  RandomAccessFile file;

  /** Number of entries in the segment */
  int numEntries;

  /** Current entry */
  int currentEntry = 0;

  /** Prefetch buffer */
  PrefetchBuffer pBuffer;

  /** Block size */
  int blockSize;

  /** Current block data size */
  int currentBlockDataSize = 0;

  /* Offset in a current block */
  int currentBlockOffset = 0;

  public BlockFileSegmentScanner(Segment s, FileIOEngine engine, int blockSize) throws IOException {
    this.segment = s;
    this.file = engine.getOrCreateFileFor(s.getId());
    this.numEntries = s.getInfo().getTotalItems();
    int bufSize = engine.getFilePrefetchBufferSize();
    this.pBuffer = new PrefetchBuffer(file, bufSize);
    this.blockSize = blockSize;
    initNextBlock();

  }

  private void initNextBlock() throws IOException {
    long fileOffset = this.pBuffer.getFileOffset(); // includes META (8 bytes)
    fileOffset -= Segment.META_SIZE;
    if (fileOffset > 0) {
      // When fileOffset % blockSize == 0, skip == 0, that is why we subtract 1
      int skip = (int) (((fileOffset - 1) / blockSize + 1) * blockSize - fileOffset);
      this.pBuffer.skip(skip);
    }
    byte[] buffer = this.pBuffer.getBuffer();
    int bufOffset = this.pBuffer.getBufferOffset();
    this.currentBlockDataSize = getBlockDataSize(buffer, bufOffset);
    this.pBuffer.skip(META_SIZE);
    this.currentBlockOffset = 0;
  }

  @Override
  public boolean hasNext() throws IOException {
    // TODO Auto-generated method stub
    if (currentEntry < numEntries) {
      return true;
    }
    return false;
  }

  @Override
  public boolean next() throws IOException {
    this.currentEntry++;
    int kSize = this.pBuffer.keyLength();
    int vSize = this.pBuffer.valueLength();
    int adv = Utils.kvSize(kSize, vSize);
    this.currentBlockOffset += adv;
    boolean result = this.pBuffer.next();
    if (!result) {
      return result;
    }
    if (this.currentBlockOffset == this.currentBlockDataSize) {
      initNextBlock();
    }
    return true;
  }

  @Override
  public int keyLength() throws IOException {
    return this.pBuffer.keyLength();
  }

  @Override
  public int valueLength() throws IOException {
    // Caller must check return value
    return this.pBuffer.valueLength();
  }

  @Override
  public long keyAddress() {
    // Caller must check return value
    return 0;
  }

  @Override
  public long valueAddress() {
    return 0;
  }

  @Override
  public long getExpire() {
    return -1;
  }

  @Override
  public void close() throws IOException {
    // FIXME
    file.close();
  }

  @Override
  public int getKey(ByteBuffer b) throws IOException {
    return this.pBuffer.getKey(b);
  }

  @Override
  public int getValue(ByteBuffer b) throws IOException {
    return this.pBuffer.getValue(b);
  }

  @Override
  public boolean isDirect() {
    return false;
  }

  @Override
  public int getKey(byte[] buffer, int offset) throws IOException {
    return this.pBuffer.getKey(buffer, offset);
  }

  @Override
  public int getValue(byte[] buffer, int offset) throws IOException {
    return this.pBuffer.getValue(buffer, offset);
  }

  @Override
  public Segment getSegment() {
    return this.segment;
  }

  @Override
  public long getOffset() {
    return this.pBuffer.getOffset();
  }
}
