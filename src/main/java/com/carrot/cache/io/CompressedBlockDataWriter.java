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

import static com.carrot.cache.compression.CompressionCodec.COMP_META_SIZE;
import static com.carrot.cache.compression.CompressionCodec.SIZE_OFFSET;
import static com.carrot.cache.compression.CompressionCodec.COMP_SIZE_OFFSET;
import static com.carrot.cache.compression.CompressionCodec.DICT_VER_OFFSET;

import com.carrot.cache.compression.CodecFactory;
import com.carrot.cache.compression.CompressionCodec;

import com.carrot.cache.util.CarrotConfig;
import com.carrot.cache.util.UnsafeAccess;
import com.carrot.cache.util.Utils;

/**
 * 
 * This data writer combines cached items into blocks and compresses them using Zstandard. 
 * Default size of a block - 8192 bytes. Block size can be configured in the configuration file
 * All cached items in a block have the same location, which is a block offset 
 * Each block starts with 12 bytes meta:
 *  
 *  1. Uncompressed size - 4 bytes
 *  2. Dictionary version - 4 bytes (all zeros - uncompressed)
 *  3. Compressed size - 4 bytes
 *  
 * Upon opening new block, writer must guarantee that meta section is clear (all 0) 
 * This is a singleton object, which is created by IOEngine
 *  
 */
public class CompressedBlockDataWriter implements DataWriter {
  
  private int blockSize;
  
  private CompressionCodec codec;
  
  private boolean compressKeys;
  
  private String cacheName;
  
  public CompressedBlockDataWriter() {
  }
  
  @Override
  public long append(Segment s, long keyPtr, int keySize, long valuePtr, int valueSize) {
    
    processEmptySegment(s);
    
    if (codec.isTrainingRequired()) {
      if (this.compressKeys) {
        codec.addTrainingData(keyPtr, keySize);
      }
      codec.addTrainingData(valuePtr, valueSize);
    }
    // Required size to write k-v pair (without compression)
    final int requiredSize = Utils.requiredSize(keySize, valueSize);
    // Segment total data size
    long dataSize = s.getSegmentDataSize();
    // Offset of a current block from segment beginning
    long currentBlockOffset = s.getCurrentBlockOffset();
    // Size of a data in a current block(including meta section)
    int lastBlockSize = (int)(dataSize - currentBlockOffset);
    //TODO: lastBlockSize = 0
    // This does not take into account block header
    if (requiredSize + dataSize > s.size()) {
      // Segment is full
      int compSize = compressBlock(s.getAddress() + currentBlockOffset, lastBlockSize);
      // Update segment size
      s.setSegmentDataSize(currentBlockOffset + compSize + COMP_META_SIZE);
      // Segment is full
      return -1;
    }
    // Check if current block is full
    if (lastBlockSize + requiredSize >= this.blockSize) {
      // Start new block if last block size > requiredSize or lastBlockSize >= blockSize
      if (lastBlockSize > requiredSize || lastBlockSize >= this.blockSize) {
        int compSize = compressBlock(s.getAddress() + currentBlockOffset, lastBlockSize);
        // Update segment size
        dataSize = currentBlockOffset + compSize + COMP_META_SIZE;
        s.setSegmentDataSize(dataSize);      
        // start new block, but first check if it can fit
        if (requiredSize + dataSize + COMP_META_SIZE > s.size()) {
          // kind of edge case 
          return -1;
        } 
        // else start new block, advance currentBlockOffset
        currentBlockOffset += COMP_META_SIZE + compSize;
        s.setCurrentBlockOffset(currentBlockOffset);
        newBlock(s);
      } // else add to the current block
    }
    long addr = s.getAddress() + s.getSegmentDataSize();
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
    // update sizes
    s.incrDataSize(requiredSize);
    incrBlockDataSize(s.getAddress() + currentBlockOffset, requiredSize);
    return currentBlockOffset;
  }
  
  /**
   * Compresses last block in the segment
   * updates block meta: sets compression dictionary version
   * (-1 - no compression, data is not compressible), uncompressed size (excluding meta) and
   * compressed size, 
   * 
   * @param addr block start address
   * @param size block size (including meta header)
   * @return compressed size (excluding meta header)
   */
  private int compressBlock(long addr, int size) {
    int compressedSize = 0;
    int dictVersion = 0;
    int toCompress = size - COMP_META_SIZE;
    checkCodec();
    dictVersion = this.codec.getCurrentDictionaryVersion();
    compressedSize = this.codec.compress(addr + COMP_META_SIZE, toCompress, dictVersion);
    //Update block header
    UnsafeAccess.putInt(addr + SIZE_OFFSET, toCompress);
    if(compressedSize >= toCompress) {
      dictVersion = -1; // no compression
      compressedSize = toCompress;
    }
    UnsafeAccess.putInt(addr + DICT_VER_OFFSET, dictVersion);
    UnsafeAccess.putInt(addr + COMP_SIZE_OFFSET, compressedSize);
    return compressedSize;
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
    
    if (codec.isTrainingRequired()) {
      if (this.compressKeys) {
        codec.addTrainingData(key, keyOffset, keySize);
      }
      codec.addTrainingData(value, valueOffset, valueSize);
    }
    // Required size to write k-v pair (without compression)
    final int requiredSize = Utils.requiredSize(keySize, valueSize);
    // Segment total data size
    long dataSize = s.getSegmentDataSize();
    // Offset of a current block from segment beginning
    long currentBlockOffset = s.getCurrentBlockOffset();
    // Size of a data in a current block(including meta section)
    int lastBlockSize = (int)(dataSize - currentBlockOffset);
    //TODO: lastBlockSize = 0
    // This does not take into account block header
    if (requiredSize + dataSize > s.size()) {
      // Segment is full
      int compSize = compressBlock(s.getAddress() + currentBlockOffset, lastBlockSize);
      // Update segment size
      s.setSegmentDataSize(currentBlockOffset + compSize + COMP_META_SIZE);
      // Segment is full
      return -1;
    }
    // Check if current block is full
    if (lastBlockSize + requiredSize >= this.blockSize) {
      // Start new block if last block size > requiredSize or lastBlockSize >= blockSize
      if (lastBlockSize > requiredSize || lastBlockSize >= this.blockSize) {
        int compSize = compressBlock(s.getAddress() + currentBlockOffset, lastBlockSize);
        // Update segment size
        dataSize = currentBlockOffset + compSize + COMP_META_SIZE;
        s.setSegmentDataSize(dataSize);      
        // start new block, but first check if it can fit
        if (requiredSize + dataSize + COMP_META_SIZE > s.size()) {
          // kind of edge case 
          return -1;
        } 
        // else start new block, advance currentBlockOffset
        currentBlockOffset += COMP_META_SIZE + compSize;
        s.setCurrentBlockOffset(currentBlockOffset);
        newBlock(s);
      } // else add to the current block
    }
    long addr = s.getAddress() + s.getSegmentDataSize();
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
    // update sizes
    s.incrDataSize(requiredSize);
    incrBlockDataSize(s.getAddress() + currentBlockOffset, requiredSize);
    return currentBlockOffset;
  }
  
  /**
   * Increment block data size
   * @param blockStart current block start address
   * @param incr increment value
   */
  private void incrBlockDataSize(long blockStart,  int incr) {
    long ptr = blockStart + SIZE_OFFSET;
    int size = UnsafeAccess.toInt(ptr);
    UnsafeAccess.putInt(ptr, size + incr);
  }
  
  /**
   * Processes empty segment
   * @param s segment
   */
  private void processEmptySegment(Segment s) {
    if (s.getTotalItems() == 0) {
      newBlock(s);
    }
  }
  
  /**
   * Clear first 12 bytes of a new block (for meta)
   * @param blockAddr
   */
  private void newBlock(Segment s) {
    long blockAddr = s.getAddress();
    long off = s.getCurrentBlockOffset();
    // clear first 12 bytes
    UnsafeAccess.setMemory(blockAddr + off, COMP_META_SIZE, (byte) 0);
    s.setSegmentDataSize(off + COMP_META_SIZE);
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
    CarrotConfig config = CarrotConfig.getInstance();
    this.blockSize = config.getCacheCompressionBlockSize(cacheName);
    // Can be null on initialization
    this.codec = CodecFactory.getInstance().getCompressionCodecForCache(cacheName);
    this.compressKeys = config.isCacheCompressionKeysEnabled(cacheName);
    this.cacheName = cacheName;
  }
  
  
  private void checkCodec() {
    if (this.codec == null) {
      this.codec = CodecFactory.getInstance().getCompressionCodecForCache(cacheName);
      if (this.codec == null) {
        throw new RuntimeException(String.format("Codec type is undefined for cache \'%s'", cacheName));
      }
    }
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
