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
  public final class BaseMemorySegmentScanner implements SegmentScanner {
    /*
     * Data segment
     */
    Segment segment;
    /*
     * Current scanner index 
     */
    int currentIndex = 0;
    
    /**
     * Current offset in a parent segment
     */
    int offset = 0;
    
    /*
     * Private constructor
     */
    BaseMemorySegmentScanner(Segment s){
      // Make sure it is sealed
      if (s.isSealed() == false) {
        throw new RuntimeException("segment is not sealed");
      }
      this.segment = s;
      s.readLock();
    }
    
    public boolean hasNext() {
      return currentIndex < segment.numberOfEntries();
    }
    
    public boolean next() {
      long ptr = segment.getAddress();
      
      int keySize = Utils.readUVInt(ptr + offset);
      int keySizeSize = Utils.sizeUVInt(keySize);
      offset += keySizeSize;
      int valueSize = Utils.readUVInt(ptr + offset);
      int valueSizeSize = Utils.sizeUVInt(valueSize);
      offset += valueSizeSize;
      offset += keySize + valueSize;
      currentIndex++;
      //TODO
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
      long ptr = segment.getAddress();
      return Utils.readUVInt(ptr + offset);
    }
    
    /**
     * Get current value size
     * @return value size
     */
    
    public final int valueLength() {
      long ptr = segment.getAddress();
      int off = offset;
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
      long ptr = segment.getAddress();
      int off = offset;
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
      long ptr = segment.getAddress();
      int off = offset;
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
      segment.readUnlock();
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

    @Override
    public Segment getSegment() {
      return this.segment;
    }

    @Override
    public long getOffset() {
      return this.offset;
    }
  }