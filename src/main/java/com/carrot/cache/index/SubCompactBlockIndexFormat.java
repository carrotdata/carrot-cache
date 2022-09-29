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
package com.carrot.cache.index;

import com.carrot.cache.util.CarrotConfig;
import com.carrot.cache.util.UnsafeAccess;
import com.carrot.cache.util.Utils;

/**
 * 
 * Compact index format takes only 6 bytes per cached entry:
 * 
 * First 2 bytes:
 * Bits 15 - 0 keep 16 bits of a hashed key 8 byte value starting with a bit L + 1
 * L is defined in configuration file as 'index.slots.power', default value is 10
 * 
 * Memory index hash table size = 2**L
 * 
 * 2 bytes - segment id (lowest 15 bits only, bit 15 is used for 1-bit hit counter)
 * 2 - bytes data block number which "potentially" stores this key-value
 * 
 * real offset is data block * block size. With 64K blocks addressed and 4K block size 
 * We can address maximum 64K * 4K = 256MB size segments. With 32K maximum segments (15 bits)
 * Total maximum cache size supported is 32K * 256MB = 8TB. 
 * 
 */
public class SubCompactBlockIndexFormat implements IndexFormat {
  
  int blockSize;
  int  L; // index.slots.power from configuration
  
  /**
   * Cache name for this index format
   * @param cacheName
   */
  public void setCacheName(String cacheName) {
    CarrotConfig config = CarrotConfig.getInstance();
    this.blockSize = config.getBlockWriterBlockSize(cacheName);
    this.L = config.getStartIndexNumberOfSlotsPower(cacheName);
  }
  
  @Override
  public boolean equals(long ptr, long hash) {
    int off = hashOffset();
    int v = UnsafeAccess.toShort(ptr + off) & 0xffff;
    hash = ((hash >>> (64 - L - 16)) & 0xffff);
    return v == hash;
  }

  @Override
  public int indexEntrySize() {
    return  3 * Utils.SIZEOF_SHORT;
  }

  @Override
  public int fullEntrySize(long ptr) {
    return indexEntrySize();
  }

  @Override
  public int fullEntrySize(int keySize, int valueSize) {
    return indexEntrySize();
  }

  @Override
  public long advance(long current) {
    return current + indexEntrySize();
  }


  @Override
  public int getKeyValueSize(long buffer) {
    // this index format does not store k-v size
    return -1;
  }

  @Override
  public int getSegmentId(long buffer) {
    int off = sidOffset();
    return UnsafeAccess.toShort(buffer + off) & 0x7fff;
  }

  @Override
  public long getOffset(long buffer) {
    int off = dataOffsetOffset();
    int blockNumber = UnsafeAccess.toShort(buffer + off) & 0xffff;
    return blockNumber * this.blockSize;
  }

  @Override
  public int getIndexBlockHeaderSize() {
    return 3 * Utils.SIZEOF_SHORT;
  }

  public int getHitCount(long buffer) {
    int off = sidOffset();
    return (UnsafeAccess.toShort(buffer + off) & 0x8000) >>> 15;
  }

  @Override
  public void hit(long ptr) {
    int off = sidOffset();
    int v = UnsafeAccess.toShort(ptr + off) & 0xffff;
    v |= 0x8000;
    UnsafeAccess.putShort(ptr + off, (short) v);
  }

  @Override
  public int getEmbeddedOffset() {
    return Utils.SIZEOF_SHORT;
  }

  @Override
  public int getHashBit(long ptr, int n) {
    int off = hashOffset();
    // TODO:test
    return ((UnsafeAccess.toShort(ptr + off) & 0xffff) >>> (16 - n + L)) & 1;
  }

  @Override
  public void writeIndex(
      long ibPtr,
      long ptr,
      byte[] key,
      int keyOffset,
      int keySize,
      byte[] value,
      int valueOffset,
      int valueSize,
      int sid,
      int dataOffset,
      int dataSize,
      long expire) {
    long hash = Utils.hash64(key, keyOffset, keySize);
    short $hash = (short)(hash >>> 64 - L - 16);
    UnsafeAccess.putShort(ptr + hashOffset(), $hash);
    UnsafeAccess.putShort(ptr + sidOffset(), (short) sid);
    UnsafeAccess.putShort(ptr + dataOffsetOffset(), (short) (dataOffset / this.blockSize));     
  }

  @Override
  public void writeIndex(
      long ibPtr,
      long ptr,
      long keyPtr,
      int keySize,
      long valuePtr,
      int valueSize,
      int sid,
      int dataOffset,
      int dataSize,
      long expire) {
    long hash = Utils.hash64(keyPtr, keySize);
    short $hash = (short)(hash >>> 64 - L - 16);
    UnsafeAccess.putShort(ptr + hashOffset(), $hash);
    UnsafeAccess.putShort(ptr + sidOffset(), (short) sid);
    UnsafeAccess.putShort(ptr + dataOffsetOffset(), (short) (dataOffset / this.blockSize));     
  }
  
  /**
   * Offsets in index field sections
   * @return offset
   */
  int hashOffset() {
    return 0;
  }
  /**
   * Offsets in index field sections
   * @return offset
   */
  int sidOffset() {
    return Utils.SIZEOF_SHORT;
  }
  /**
   * Offsets in index field sections
   * @return offset
   */
  int dataOffsetOffset() {
    return  2 * Utils.SIZEOF_SHORT;
  }
  /**
   * Offsets in index field sections
   * @return offset
   */
  int expireOffset() {
    // Not supported
    return -1;
  }
  
  @Override
  public boolean isSizeSupported() {
    return false;
  }
}
