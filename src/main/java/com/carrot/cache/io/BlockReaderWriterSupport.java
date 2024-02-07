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

import com.carrot.cache.util.UnsafeAccess;
import com.carrot.cache.util.Utils;

public class BlockReaderWriterSupport {
  public final static int SIZE_OFFSET = 0;
  public final static int META_SIZE = Utils.SIZEOF_INT;
  
  /**
   * Get data size in block n
   * @param s segment
   * @param blockSize block size
   * @param blockNumber block number (0-based)
   * @return data size
   */
  public static int getBlockDataSize(Segment s, int blockSize, int blockNumber) {
    long ptr = s.getAddress() + blockNumber * blockSize + SIZE_OFFSET;
    int blockS =  UnsafeAccess.toInt(ptr);
    return blockS;
  } 
  
  /**
   * Get data size in block at offset
   * @param blockPtr block address
   * @return data size
   */
  public static int getBlockDataSize(long blockPtr) {
    return UnsafeAccess.toInt(blockPtr);
  } 
  
  /**
   * Get data size in block at offset
   * @param block data block (first 4 bytes contains size)
   * @return data size
   */
  public static int getBlockDataSize(byte[] block) {
    return UnsafeAccess.toInt(block, 0);
  } 
  
  
  /**
   * Get data size in block at offset
   * @param buffer data block (first 4 bytes contains size)
   * @param offset offset in the data block
   * @return data size
   */
  public static int getBlockDataSize(byte[] buffer, int offset) {
    return UnsafeAccess.toInt(buffer, offset);
  } 
  
  /**
   * Get real data size in a segment
   * @param s segment
   * @param blockSize block size
   * @return data size
   */
  public static long getDataSize(Segment s, int blockSize) {
    long size = s.getSegmentDataSize();
    int currentBlock = (int) (size / blockSize);
    size += getBlockDataSize(s, blockSize, currentBlock);
    return size;
  }
  
  /**
   * Get full data size (including last block META section size) in a segment
   * @param s segment
   * @param blockSize block size
   * @return data size
   */
  public static long getFullDataSize(Segment s, int blockSize) {
    long size = s.getSegmentBlockDataSize();
    int currentBlock = (int) (size / blockSize);
    int blockDataSize =  getBlockDataSize(s, blockSize, currentBlock);
    size += blockDataSize;
    return size + (blockDataSize > 0? META_SIZE: 0);
  }
  
  /**
   * Find key in a memory block
   * @param ptr block address
   * @param key key buffer
   * @param keyOffset key offset
   * @param keySize key size
   * @return address of a K-V pair or -1 (not found)
   */
  public static long findInBlock(long ptr, byte[] key, int keyOffset, int keySize) {
    int blockDataSize = getBlockDataSize(ptr);
    return findInBlock(ptr, blockDataSize, key, keyOffset, keySize);
  }
  
  /**
   * Find key in a memory block
   * @param ptr block address
   * @param blockDataSize block data size
   * @param key key buffer
   * @param keyOffset key offset
   * @param keySize key size
   * @return address of a K-V pair or -1 (not found)
   */
  public static long findInBlock(long ptr, int blockDataSize, byte[] key, int keyOffset, int keySize) {
    long $ptr = ptr + META_SIZE;
    long found = IOEngine.NOT_FOUND;

    while ($ptr < ptr + blockDataSize) {
      // Format of a key-value pair in a buffer: key-size, value-size, key, value
      int kSize = Utils.readUVInt($ptr);
      int kSizeSize = Utils.sizeUVInt(kSize);
      $ptr += kSizeSize;
      int vSize = Utils.readUVInt($ptr);
      int vSizeSize = Utils.sizeUVInt(vSize);
      $ptr += vSizeSize;
      if (kSize != keySize) {
        $ptr += kSize + vSize;
        continue;
      }
      if ($ptr + kSize >= ptr + blockDataSize) {
        break;
      }
      if (Utils.compareTo(key, keyOffset, keySize, $ptr, kSize) == 0) {
        $ptr -= kSizeSize + vSizeSize;
        found = $ptr;
      }
      $ptr += kSize + vSize;
    }
    return found;
  }
  
  /**
   * Find key in a memory block
   * 
   * There is a chance that the same key is present more than once
   * in t he block. In this case the last one will be considered as 
   * a right one. It is not a transactional DB and consistency requirements
   * is relaxed.
   * 
   * 
   * @param ptr block address
   * @param keyPtr key address
   * @param keySize key size
   * @return address of a K-V pair or -1 (not found)
   */
  public static long findInBlock(long ptr, long keyPtr, int keySize) {
    int blockDataSize = getBlockDataSize(ptr);
    return findInBlock(ptr, blockDataSize, keyPtr, keySize);
  }
  
  /**
   * Find key in a memory block
   * 
   * There is a chance that the same key is present more than once
   * in the block. In this case the last one will be considered as 
   * a right one. It is not a transactional DB and consistency requirements
   * is relaxed.
   * 
   * 
   * @param ptr block address
   * @param blockDataSize block data size
   * @param keyPtr key address
   * @param keySize key size
   * @return address of a K-V pair or -1 (not found)
   */
  public static long findInBlock(long ptr, int blockDataSize, long keyPtr, int keySize) {
    long $ptr = ptr + META_SIZE;
    long found = IOEngine.NOT_FOUND;
    while ($ptr < ptr + blockDataSize) {
      // Format of a key-value pair in a buffer: key-size, value-size, key, value
      int kSize = Utils.readUVInt($ptr);
      int kSizeSize = Utils.sizeUVInt(kSize);
      $ptr += kSizeSize;
      int vSize = Utils.readUVInt($ptr);
      int vSizeSize = Utils.sizeUVInt(vSize);
      $ptr += vSizeSize;
      if (kSize != keySize) {
        $ptr += kSize + vSize;
        continue;
      }
      if ($ptr + kSize >= ptr + blockDataSize) {
        break;
      }
      if (Utils.compareTo(keyPtr, keySize, $ptr, kSize) == 0) {
        $ptr -= kSizeSize + vSizeSize;
        found = $ptr;
      }
      $ptr += kSize + vSize;
    }
    return found;
  }
  
  /**
   * Find key in a block buffer
   * @param block data block
   * @param blockOff block offset
   * @param key key buffer
   * @param keyOffset key offset
   * @param keySize key size
   * @return offset of a K-V pair or -1 (not found)
   */
  public static long findInBlock(byte[] block, int blockOff, byte[] key, int keyOffset, int keySize) {
    int blockDataSize = getBlockDataSize(block, blockOff);
    return findInBlock(block, blockOff, blockDataSize, key, keyOffset, keySize);
  }
  
  /**
   * Find key in a block buffer
   * @param block data block
   * @param blockOff block offset
   * @param blockDataSize block data size
   * @param key key buffer
   * @param keyOffset key offset
   * @param keySize key size
   * @return offset of a K-V pair or -1 (not found)
   */
  public static long findInBlock(byte[] block, int blockOff, int blockDataSize, byte[] key, int keyOffset, int keySize) {
    int off = META_SIZE + blockOff;
    long found = IOEngine.NOT_FOUND;

    while (off < blockDataSize) {
      // Format of a key-value pair in a buffer: key-size, value-size, key, value
      int kSize = Utils.readUVInt(block, off);
      int kSizeSize = Utils.sizeUVInt(kSize);
      off += kSizeSize;
      int vSize = Utils.readUVInt(block, off);
      int vSizeSize = Utils.sizeUVInt(vSize);
      off += vSizeSize;
      if (kSize != keySize) {
        off += kSize + vSize;
        continue;
      }
      if (off + kSize >= blockDataSize) {
        break;
      }
      if (Utils.compareTo(key, keyOffset, keySize, block, off, kSize) == 0) {
        found = off - kSizeSize - vSizeSize;
      }
      off += kSize + vSize;
    }
    return found;
  }
  
  /**
   * Find key in a block buffer
   * @param block data block
   * @param blockOff offset in the block
   * @param keyPtr key address
   * @param keySize key size
   * @return offset of a K-V pair or -1 (not found)
   */
  public static long findInBlock(byte[] block, int blockOff, long keyPtr, int keySize) {
    int blockDataSize = getBlockDataSize(block, blockOff);
    return findInBlock(block, blockOff, blockDataSize, keyPtr, keySize);
  }
  
  /**
   * Find key in a block buffer
   * @param block data block
   * @param blockOff offset in the block
   * @param blockDataSize block data size
   * @param keyPtr key address
   * @param keySize key size
   * @return offset of a K-V pair or -1 (not found)
   */
  public static long findInBlock(byte[] block, int blockOff, int blockDataSize, long keyPtr, int keySize) {
    int off = META_SIZE + blockOff;
    long found = IOEngine.NOT_FOUND;

    while (off < blockDataSize) {
      // Format of a key-value pair in a buffer: key-size, value-size, key, value
      int kSize = Utils.readUVInt(block, off);
      int kSizeSize = Utils.sizeUVInt(kSize);
      off += kSizeSize;
      int vSize = Utils.readUVInt(block, off);
      int vSizeSize = Utils.sizeUVInt(vSize);
      off += vSizeSize;
      if (kSize != keySize) {
        off += kSize + vSize;
        continue;
      }
      if (off + kSize >= blockDataSize) {
        break;
      }
      if (Utils.compareTo(block, off, kSize, keyPtr, keySize) == 0) {
        found = off - kSizeSize - vSizeSize;
      }
      off += kSize + vSize;
    }
    return found;
  }
}
