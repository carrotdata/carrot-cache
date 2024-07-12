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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotdata.cache.index.MemoryIndex;
import com.carrotdata.cache.util.CacheConfig;
import com.carrotdata.cache.util.UnsafeAccess;
import com.carrotdata.cache.util.Utils;

public class BaseBatchDataWriter implements DataWriter {

  private static final Logger LOG = LoggerFactory.getLogger(BaseBatchDataWriter.class);

  protected int blockSize;

  public BaseBatchDataWriter() {
  }

  @Override
  public boolean isWriteBatchSupported() {
    return true;
  }

  @Override
  public WriteBatch newWriteBatch() {
    return new WriteBatch(blockSize);
  }

  @Override
  public void init(String cacheName) {
    CacheConfig config = CacheConfig.getInstance();
    this.blockSize = config.getBlockWriterBlockSize(cacheName);
  }

  @Override
  public long append(Segment s, long keyPtr, int keySize, long itemPtr, int itemSize) {
    int requiredSize = Utils.requiredSize(keySize, itemSize);
    if (requiredSize + s.getSegmentDataSize() > s.size()) {
      return -1;
    }
    long addr = s.getAddress() + s.getSegmentDataSize();
    // Key size
    Utils.writeUVInt(addr, keySize);
    int kSizeSize = Utils.sizeUVInt(keySize);
    addr += kSizeSize;
    // Value size
    Utils.writeUVInt(addr, itemSize);
    int vSizeSize = Utils.sizeUVInt(itemSize);
    addr += vSizeSize;
    // Copy key
    UnsafeAccess.copy(keyPtr, addr, keySize);
    addr += keySize;
    // Copy value (item)
    UnsafeAccess.copy(itemPtr, addr, itemSize);
    long retValue = s.getSegmentDataSize();
    s.incrDataSize(requiredSize);
    return retValue;
  }

  @Override
  public long append(Segment s, byte[] key, int keyOffset, int keySize, byte[] value,
      int valueOffset, int valueSize) {
    int requiredSize = Utils.requiredSize(keySize, valueSize);
    if (requiredSize + s.getSegmentDataSize() > s.size()) {
      return -1;
    }
    if (s.getAddress() == 0) {
      LOG.error("PTR=NULL sid={} dataSize={} isSealed={} isValid={} isMemory={}", s.getId(),
        s.getSegmentDataSize(), Boolean.toString(s.isSealed()), Boolean.toString(s.isValid()),
        Boolean.toString(s.isMemory()));
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
    long retValue = s.getSegmentDataSize();
    s.incrDataSize(requiredSize);
    return retValue;
  }

  @Override
  public long append(Segment s, WriteBatch batch) {

    long src = batch.memory();
    int len = batch.position();

    long offset = 0;
    try {
      s.writeLock();
      if (s.isFull() || s.isSealed()) {
        return -1;
      }
      offset = s.getSegmentDataSize();
      if (s.size() - offset < len) {
        s.setFull(true);
        return -1;
      }
      long sdst = s.getAddress() + offset;
      // Copy
      UnsafeAccess.copy(src, sdst, len);
      // Update segment
      s.setSegmentDataSize(offset + len);
      s.setCurrentBlockOffset(offset + len);

    } finally {
      s.writeUnlock();
    }
    // Now we need update MemoryIndex
    MemoryIndex mi = s.getMemoryIndex();
    // We do not need to read lock b/c this thread is the only writer
    // to this write batch
    int off = 0;
    final short sid = (short) s.getId();
    final int id = batch.getId();
    while (off < len) {
      int kSize = Utils.readUVInt(src + off);
      off += Utils.sizeUVInt(kSize);
      int vSize = Utils.readUVInt(src + off);
      off += Utils.sizeUVInt(vSize);
      mi.compareAndUpdate(src + off, kSize, (short) -1, id, sid, (int) offset);
      off += kSize + vSize;
    }
    // Reset batch to accept new writes
    batch.reset();
    return offset + len;
  }

  @Override
  public long appendSingle(Segment s, long keyPtr, int keySize, long itemPtr, int itemSize) {
    return append(s, keyPtr, keySize, itemPtr, itemSize);
  }

  @Override
  public long appendSingle(Segment s, byte[] key, int keyOffset, int keySize, byte[] value,
      int valueOffset, int valueSize) {
    return append(s, key, keyOffset, keySize, value, valueOffset, valueSize);
  }

  /**
   * Sets block size
   * @param size block size
   */
  public void setBlockSize(int size) {
    this.blockSize = size;
  }

  /**
   * Is block based data writer
   * @return true false
   */
  @Override
  public boolean isBlockBased() {
    return false;
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
