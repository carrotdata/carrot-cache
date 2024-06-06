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

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

public final class BaseFileSegmentScanner implements SegmentScanner {

  // RandomAccessFile file;
  Segment segment;
  int numEntries;
  int currentEntry = 0;
  PrefetchBuffer pBuffer;

  public BaseFileSegmentScanner(Segment s, RandomAccessFile file, int prefetchBufferSize)
      throws IOException {
    this.segment = s;
    this.numEntries = s.getInfo().getTotalItems();
    int bufSize = prefetchBufferSize;
    this.pBuffer = new PrefetchBuffer(file, bufSize);
  }

  @Override
  public boolean hasNext() throws IOException {
    if (currentEntry <= numEntries - 1) {
      return true;
    }
    ;
    return false;
  }

  @Override
  public boolean next() throws IOException {
    this.currentEntry++;
    return this.pBuffer.next();
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
    // file.close();
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
