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

import static com.carrot.cache.util.BlockReaderWriterSupport.META_SIZE;
import static com.carrot.cache.util.BlockReaderWriterSupport.SIZE_OFFSET;
import static com.carrot.cache.util.BlockReaderWriterSupport.getFullDataSize;

import com.carrot.cache.util.CacheConfig;
import com.carrot.cache.util.UnsafeAccess;
import com.carrot.cache.util.Utils;

/**
 * 
 * This data writer combines cached items into blocks. Default size of a block - 4096 bytes.
 * Block size can be configured in the configuration file
 * All cached item locations are rounded (floor) to a block boundary.
 * 
 * All items in a first block have location - 0
 * All items in a second block have location - 4096
 * ...
 * All items in a block N have location - 4096 * (N -1)
 * 
 * Blocking allows to reduce location requirement by 12 bit (for 4096 byte block) and allows to create 
 * compact meta representation (like 8 bytes per cached item including expiration time). 
 * 
 * Each block starts with 6 bytes meta where block data size is stored (4) and number of items in the block (2)
 * Upon opening new block, writer must guarantee that meta section is clear (all 0)  
 */
public class BlockDataWriter implements DataWriter {
  
  private int blockSize;
      
  public BlockDataWriter() {
  }
  
  @Override
  public long append(Segment s, long keyPtr, int keySize, long valuePtr, int valueSize) {
    
    processEmptySegment(s);
    
    long addr = getAppendAddress(s, keySize, valueSize);
    if (addr < 0) {
      return IOEngine.NOT_FOUND;
    }
    int currentBlock = (int)(addr - s.getAddress()) / this.blockSize;
    //TODO: check this code
    boolean crossedBlockBoundary = addr > s.getAddress() + getFullDataSize(s, blockSize);
    
    if (crossedBlockBoundary) {
      zeroBlockMeta(s, currentBlock);
    }
    
    // Key size
    Utils.writeUVInt(addr, keySize);
    int kSizeSize = Utils.sizeUVInt(keySize);
    addr += kSizeSize;
    // Value size
    Utils.writeUVInt(addr, valueSize);
    int vSizeSize = Utils.sizeUVInt(valueSize);
    addr += vSizeSize;
    // Copy key
    UnsafeAccess.copy(keyPtr, addr, keySize);
    addr += keySize;
    // Copy value (item)
    UnsafeAccess.copy(valuePtr, addr, valueSize);   
    int requiredSize = Utils.requiredSize(keySize, valueSize);
    incrBlockDataSize(s, currentBlock, requiredSize);
    //incrNumberBlockItems(s, currentBlock);
    
    return crossedBlockBoundary? this.blockSize: 0;
  }

  private long getAppendAddress(Segment s, int keySize, int valueSize) {
    int requiredSize = Utils.requiredSize(keySize, valueSize);
    long dataSize = getFullDataSize(s, blockSize);
    if (requiredSize + dataSize > s.size()) {
      return IOEngine.NOT_FOUND;
    }
    boolean crossedBlockBoundary = dataSize / this.blockSize < (dataSize + requiredSize) /this.blockSize ;
    
    if (crossedBlockBoundary) {
      // next block starts with
      int off = (int)(dataSize / this.blockSize) * this.blockSize + this.blockSize;
      // and must have space for blockSize
      off += this.blockSize;
      if (off > s.size()) {
        // Not enough space in this segment for additional block
        return IOEngine.NOT_FOUND;
      }
    }
    long addr = crossedBlockBoundary? s.getAddress() + (dataSize / this.blockSize) * this.blockSize 
        + this.blockSize + META_SIZE: s.getAddress() + dataSize;
    return addr;
  }

  @Override
  public long append(
      Segment s,
      byte[] key,
      int keyOffset,
      int keySize,
      byte[] value,
      int valueOffset,
      int valueSize) {

    processEmptySegment(s);
    long addr = getAppendAddress(s, keySize, valueSize);
    if (addr < 0) {
      return IOEngine.NOT_FOUND;
    }
    int currentBlock = (int) (addr - s.getAddress()) / this.blockSize;
    // TODO: check this code
    boolean crossedBlockBoundary = addr > s.getAddress() + getFullDataSize(s, blockSize);

    if (crossedBlockBoundary) {
      zeroBlockMeta(s, currentBlock);
    }

    // Key size
    Utils.writeUVInt(addr, keySize);
    int kSizeSize = Utils.sizeUVInt(keySize);
    addr += kSizeSize;
    // Value size
    Utils.writeUVInt(addr, valueSize);
    int vSizeSize = Utils.sizeUVInt(valueSize);
    addr += vSizeSize;
    // Copy key
    UnsafeAccess.copy(key, keyOffset, addr, keySize);
    addr += keySize;
    // Copy value (item)
    UnsafeAccess.copy(value, valueOffset, addr, valueSize);

    int requiredSize = Utils.requiredSize(keySize, valueSize);

    incrBlockDataSize(s, currentBlock, requiredSize);
    //incrNumberBlockItems(s, currentBlock);

    return crossedBlockBoundary ? this.blockSize : 0;
  }
  
  /**
   * Increment block data size
   * @param s segment
   * @param n block number
   * @param incr increment value
   */
  private void incrBlockDataSize(Segment s, int n, int incr) {
    long ptr = s.getAddress() + n * blockSize + SIZE_OFFSET;
    int size = UnsafeAccess.toInt(ptr);
    UnsafeAccess.putInt(ptr, size + incr);
  }
  
  /**
   * Increment number of block items
   * @param s segment
   * @param n block number (0 - based)
   */
//  private void incrNumberBlockItems(Segment s, int n) {
//    long ptr = s.getAddress() + n * blockSize;
//    int num = UnsafeAccess.toShort(ptr);
//    UnsafeAccess.putShort(ptr, (short)(num + 1));
//  }
  
  /**
   * Zero first 6 (actually - 8) bytes of a given block
   * @param s segment
   * @param n block number
   */
  private void zeroBlockMeta(Segment s, int n) {
    long ptr = s.getAddress() + n * blockSize;
    UnsafeAccess.putLong(ptr, 0L);
  }
  
  /**
   * Processes empty segment
   * @param s segment
   */
  private void processEmptySegment(Segment s) {
    if (s.numberOfEntries() == 0) {
      long ptr = s.getAddress();
      UnsafeAccess.putShort(ptr, (short) 0);
    }
  }
  
  @Override
  public void init(String cacheName) {
    this.blockSize = CacheConfig.getInstance().getBlockWriterBlockSize(cacheName);
  }
}
