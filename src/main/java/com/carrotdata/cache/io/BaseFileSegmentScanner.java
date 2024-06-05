/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
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