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
package com.carrot.cache.io;

import java.io.IOException;
import java.nio.ByteBuffer;

import com.carrot.cache.util.UnsafeAccess;
import com.carrot.cache.util.Utils;
import static com.carrot.cache.util.BlockReaderWriterSupport.getBlockDataSize;
import static com.carrot.cache.util.BlockReaderWriterSupport.META_SIZE;


/**
   * Segment scanner
   * 
   * Usage:
   * 
   * while(scanner.hasNext()){
   *   // do job
   *   // ...
   *   // next()
   *   scanner.next();
   * }
   */
public class BlockMemorySegmentScanner implements SegmentScanner {
  /*
   * Parent segment
   */
  Segment parent;
  /*
   * Current scanner index 
   */
  int currentItemIndex = 0;
  
  /**
   * Total number of items in a segment
   */
  int totalItems = 0;
  /**
   * Current offset in a parent segment
   */
  int segmentOffset = 0;
  
  /**
   * Offset in a current block
   */
  int blockOffset = 0;
  
  /**
   * Block data size
   */
  int blockDataSize = 0;
  
  /**
   * Block size
   */
  int blockSize;
  
  /**
   * Current block number
   */
  int currentBlockIndex = 0;
  
  /*
   * Private constructor
   */
  BlockMemorySegmentScanner(Segment s, int blockSize){
    // Make sure it is sealed
    if (s.isSealed() == false) {
      throw new RuntimeException("segment is not sealed");
    }
    this.parent = s;
    this.blockSize = blockSize;
    s.readLock();
    this.currentBlockIndex = 0;
    initNextBlock();
  }
  
  
  private void initNextBlock() {
    this.currentBlockIndex++;
    this.segmentOffset = this.currentBlockIndex * this.blockSize;
    this.blockDataSize = getBlockDataSize(this.segmentOffset);
    this.blockOffset = META_SIZE;
    
  }
  public boolean hasNext() {
    return this.currentItemIndex < this.totalItems;
  }
  
  public boolean next() {
    if (this.currentItemIndex == this.totalItems - 1) {
      return false;
    }
    long ptr = parent.getAddress();
    int off = this.segmentOffset + this.blockOffset;
    int keySize = Utils.readUVInt(off);
    int keySizeSize = Utils.sizeUVInt(keySize);
    off += keySizeSize;
    int valueSize = Utils.readUVInt(ptr + segmentOffset);
    int valueSizeSize = Utils.sizeUVInt(valueSize);
    off += valueSizeSize;
    off += keySize + valueSize;
    this.currentItemIndex++;
    if (off - this.segmentOffset == this.blockDataSize + META_SIZE) {
      initNextBlock();
    } else {
      this.blockOffset = off - this.segmentOffset;
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
    long ptr = parent.getAddress();
    return Utils.readUVInt(ptr + this.segmentOffset + this.blockOffset);
  }
  
  /**
   * Get current value size
   * @return value size
   */
  
  public final int valueLength() {
    long ptr = parent.getAddress();
    int off = this.segmentOffset + this.blockOffset;
    int keySize = Utils.readUVInt(ptr + off);
    int keySizeSize = Utils.sizeUVInt(keySize);
    off += keySizeSize;
    return Utils.readUVInt(ptr + off);
  }
  
  /**
   * Get current key's address
   * @return keys address
   */
  public final long keyAddress() {
    long ptr = parent.getAddress();
    int off = this.segmentOffset + this.blockOffset;
    int keySize = Utils.readUVInt(ptr + off);
    int keySizeSize = Utils.sizeUVInt(keySize);
    off += keySizeSize;
    int valueSize = Utils.readUVInt(ptr + off);
    int valueSizeSize = Utils.sizeUVInt(valueSize);
    off += valueSizeSize;
    return ptr + off;
  }
  
  /**
   * Get current value's address
   * @return values address
   */
  public final long valueAddress() {
    long ptr = parent.getAddress();
    int off = this.segmentOffset + this.blockOffset;
    int keySize = Utils.readUVInt(ptr + off);
    int keySizeSize = Utils.sizeUVInt(keySize);
    off += keySizeSize;
    int valueSize = Utils.readUVInt(ptr + off);
    int valueSizeSize = Utils.sizeUVInt(valueSize);
    off += valueSizeSize + keySize;
    return ptr + off;
  }

  @Override
  public void close() throws IOException {
    parent.readUnlock();
  }

  @Override
  public int getKey(ByteBuffer b) {
    int keySize = keyLength();
    long keyAddress = keyAddress();
    if (keySize <= b.remaining()) {
      UnsafeAccess.copy(keyAddress, b, keySize);
    }
    return keySize;
  }

  @Override
  public int getValue(ByteBuffer b) {
    int valueSize = valueLength();
    long valueAddress = valueAddress();
    if (valueSize <= b.remaining()) {
      UnsafeAccess.copy(valueAddress, b, valueSize);
    }
    return valueSize;
  }

  @Override
  public int getKey(byte[] buffer, int offset) throws IOException {
    int keySize = keyLength();
    if (keySize > buffer.length - offset) {
      return keySize;
    }
    long keyAddress = keyAddress();
    UnsafeAccess.copy(keyAddress, buffer, offset, keySize); 
    return keySize;
  }

  @Override
  public int getValue(byte[] buffer, int offset) throws IOException {
    int valueSize = valueLength();
    if (valueSize > buffer.length - offset) {
      return valueSize;
    }
    long valueAddress = valueAddress();
    UnsafeAccess.copy(valueAddress, buffer, offset, valueSize);
    return valueSize;
  }
}