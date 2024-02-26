/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.onecache.core.io;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.onecache.core.util.CarrotConfig;

public class OffheapIOEngine extends IOEngine {
  
  /** Logger */
  @SuppressWarnings("unused")
  private static final Logger LOG = LogManager.getLogger(OffheapIOEngine.class);
    
  public OffheapIOEngine(String cacheName) {
    super(cacheName);
  }

  public OffheapIOEngine (CarrotConfig conf) {
    super(conf);
  }
  
  @Override
  protected int getInternal(int sid, long offset, int size, byte[] key, 
      int keyOffset, int keySize, byte[] buffer, int bufOffset)  {
    try {
      return this.memoryDataReader.read(this, key, keyOffset, keySize, sid, offset, size, buffer, bufOffset);
    } catch (IOException e) {
      // never happens
    }
    return NOT_FOUND;
  }

  @Override
  protected int getInternal(int sid, long offset, int size, byte[] key, 
      int keyOffset, int keySize, ByteBuffer buffer) throws IOException {
    try {
      return this.memoryDataReader.read(this, key, keyOffset, keySize, sid, offset, size, buffer);
    } catch (IOException e) {
      // never happens
    }
    return NOT_FOUND;
  }

  @Override
  protected int getInternal(int sid, long offset, int size, long keyPtr, 
      int keySize, byte[] buffer, int bufOffset)  {
    try {
      return this.memoryDataReader.read(this, keyPtr, keySize, sid, offset, size, buffer, bufOffset);
    } catch (IOException e) {
      // never happens
    }
    return NOT_FOUND;
 }

  @Override
  protected int getInternal(int sid, long offset, int size, long keyPtr, 
      int keySize, ByteBuffer buffer) throws IOException {
    try {
      return this.memoryDataReader.read(this, keyPtr, keySize, sid, offset, size, buffer);
    } catch (IOException e) {
      // never happens
    }
    return NOT_FOUND;
  }
  
  @Override
  public SegmentScanner getScanner(Segment s) throws IOException {
    return this.memoryDataReader.getSegmentScanner(this, s);
  }

  @Override
  protected void saveInternal(Segment data) throws IOException {
    data.seal();
  }

  @Override
  protected int getRangeInternal(int sid, long offset, int size, byte[] key, int keyOffset,
      int keySize, int rangeStart, int rangeSize, byte[] buffer, int bufOffset) throws IOException {
    try {
      return this.memoryDataReader.readValueRange(this, key, keyOffset, keySize, sid, offset, size,
        buffer, bufOffset, rangeStart, rangeSize);
    } catch (IOException e) {
      // never happens
    }
    return NOT_FOUND;
  }

  @Override
  protected int getRangeInternal(int sid, long offset, int size, long keyPtr, int keySize,
      int rangeStart, int rangeSize, byte[] buffer, int bufOffset) throws IOException {
    try {
      return this.memoryDataReader.readValueRange(this, keyPtr, keySize, sid, offset, size,
        buffer, bufOffset, rangeStart, rangeSize);
    } catch (IOException e) {
      // never happens
    }
    return NOT_FOUND;
  }

  @Override
  protected int getRangeInternal(int sid, long offset, int size, byte[] key, int keyOffset,
      int keySize, int rangeStart, int rangeSize, ByteBuffer buffer) throws IOException {
    try {
      return this.memoryDataReader.readValueRange(this, key, keyOffset, keySize, sid, offset, size,
        buffer, rangeStart, rangeSize);
    } catch (IOException e) {
      // never happens
    }
    return NOT_FOUND;
  }

  @Override
  protected int getRangeInternal(int sid, long offset, int size, long keyPtr, int keySize,
      int rangeStart, int rangeSize, ByteBuffer buffer) throws IOException {
    try {
      return this.memoryDataReader.readValueRange(this, keyPtr, keySize, sid, offset, size,
        buffer,  rangeStart, rangeSize);
    } catch (IOException e) {
      // never happens
    }
    return NOT_FOUND;
  }
  
  @Override
  protected boolean isOffheap() {
    return true;
  }
}
