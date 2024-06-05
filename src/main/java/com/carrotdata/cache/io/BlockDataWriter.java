/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.carrotdata.cache.io;

import static com.carrotdata.cache.io.BlockReaderWriterSupport.META_SIZE;
import static com.carrotdata.cache.io.BlockReaderWriterSupport.SIZE_OFFSET;
import static com.carrotdata.cache.io.BlockReaderWriterSupport.getFullDataSize;

import com.carrotdata.cache.util.CacheConfig;
import com.carrotdata.cache.util.UnsafeAccess;
import com.carrotdata.cache.util.Utils;

/**
 * This data writer combines cached items into blocks. Default size of a block - 4096 bytes. Block
 * size can be configured in the configuration file All cached item locations are rounded (floor) to
 * a block boundary. All items in a first block have location - 0 All items in a second block have
 * location - 4096 ... All items in a block N have location - 4096 * (N -1) Blocking allows to
 * reduce location requirement by 12 bit (for 4096 byte block) and allows to create compact meta
 * representation (like 8 bytes per cached item including expiration time). Each block starts with 6
 * bytes meta where block data size is stored (4) and number of items in the block (2) Upon opening
 * new block, writer must guarantee that meta section is clear (all 0)
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

    long fullDataSize = getFullDataSize(s, blockSize);
    boolean notEmptySegment = s.getTotalItems() > 0;
    boolean crossedBlockBoundary = notEmptySegment && addr > s.getAddress() + fullDataSize;
    long retValue =
        crossedBlockBoundary ? addr - META_SIZE - s.getSegmentBlockDataSize() - s.getAddress() : 0;
    int currentBlock = crossedBlockBoundary ? (int) (addr - s.getAddress()) / this.blockSize
        : (int) (s.getSegmentBlockDataSize() / this.blockSize);

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
    s.incrBlockDataSize((int) retValue);
    return s.getSegmentBlockDataSize();// retValue;
  }

  private long getAppendAddress(Segment s, int keySize, int valueSize) {
    int requiredSize = Utils.requiredSize(keySize, valueSize);
    long fullDataSize = getFullDataSize(s, blockSize);
    if (requiredSize + fullDataSize > s.size()) {
      return IOEngine.NOT_FOUND;
    }

    // corner case: fullDataSize / blockSize * blockSize == fullDataSize
    boolean fullBlocks = fullDataSize > 0 && (fullDataSize / blockSize * blockSize == fullDataSize);
    if (fullBlocks) {
      // Zero meta section
      UnsafeAccess.setMemory(s.getAddress() + fullDataSize, META_SIZE, (byte) 0);
      return s.getAddress() + fullDataSize + META_SIZE;
    }

    boolean crossedBlockBoundary = fullDataSize > 0 && !fullBlocks
        && fullDataSize / this.blockSize < (fullDataSize + requiredSize) / this.blockSize;

    if (crossedBlockBoundary) {
      // next block starts with
      int off = (int) (fullDataSize / this.blockSize) * this.blockSize + this.blockSize;
      // and must have space for requiredSize
      if (off + META_SIZE + requiredSize > s.size()) {
        // Not enough space in this segment for additional block
        return IOEngine.NOT_FOUND;
      }
      // Zero meta section
      UnsafeAccess.setMemory(s.getAddress() + off, META_SIZE, (byte) 0);
    }
    // This is equivalent to :
    // if (fullDataSize == 0) {
    // fullDataSize += META_SIZE
    // }
    long incr = ((fullDataSize - 1) & 0x8000000000000000L) >>> 63;
    fullDataSize += META_SIZE * incr;

    long addr =
        crossedBlockBoundary
            ? s.getAddress() + (fullDataSize / this.blockSize) * this.blockSize + this.blockSize
                + META_SIZE
            : s.getAddress() + fullDataSize;
    return addr;
  }

  @Override
  public long append(Segment s, byte[] key, int keyOffset, int keySize, byte[] value,
      int valueOffset, int valueSize) {

    processEmptySegment(s);
    long addr = getAppendAddress(s, keySize, valueSize);
    if (addr < 0) {
      return IOEngine.NOT_FOUND;
    }
    // TODO: check this code
    long fullDataSize = getFullDataSize(s, blockSize);
    boolean notEmptySegment = s.getTotalItems() > 0;
    boolean crossedBlockBoundary = notEmptySegment && addr > s.getAddress() + fullDataSize;
    long retValue =
        crossedBlockBoundary ? addr - META_SIZE - s.getSegmentBlockDataSize() - s.getAddress() : 0;

    int currentBlock = crossedBlockBoundary ? (int) (addr - s.getAddress()) / this.blockSize
        : (int) (s.getSegmentBlockDataSize() / this.blockSize);

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
    s.incrBlockDataSize((int) retValue);
    return s.getSegmentBlockDataSize();
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
    s.incrDataSize(incr);
  }

  /**
   * Processes empty segment
   * @param s segment
   */
  private void processEmptySegment(Segment s) {
    if (s.getTotalItems() == 0) {
      long ptr = s.getAddress();
      UnsafeAccess.setMemory(ptr, META_SIZE, (byte) 0);
    }
  }

  /**
   * Sets block size
   * @param size block size
   */
  public void setBlockSize(int size) {
    this.blockSize = size;
  }

  @Override
  public void init(String cacheName) {
    this.blockSize = CacheConfig.getInstance().getBlockWriterBlockSize(cacheName);
  }

  /**
   * Is block based data writer
   * @return true false
   */
  @Override
  public boolean isBlockBased() {
    return true;
  }

  /**
   * Get block size
   * @return block size
   */
  @Override
  public int getBlockSize() {
    return this.blockSize;
  }
}
