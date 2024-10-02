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

import static com.carrotdata.cache.io.BlockReaderWriterSupport.OPT_META_SIZE;

import com.carrotdata.cache.index.MemoryIndex;
import com.carrotdata.cache.util.CacheConfig;
import com.carrotdata.cache.util.UnsafeAccess;
import com.carrotdata.cache.util.Utils;

public class BaseBatchDataWriterOpt implements DataWriter {

  private int blockSize;

  @Override
  public boolean isWriteBatchSupported() {
    return true;
  }

  @Override
  public WriteBatch newWriteBatch() {
    return new WriteBatch(blockSize);
  }

  @Override
  public long append(Segment s, WriteBatch batch) {
    final long src = batch.memory();
    final int len = batch.position();
    
    if (len == 0) {
      // Its not possible
      throw new RuntimeException("write batch size is 0");
    }
    
    final int id = -1;
    long offset = 0;
    try {
      s.writeLock();
      if (s.isFull() || s.isSealed()) {
        return -1;
      }
      offset = s.getSegmentDataSize();
      if (s.size() - offset < len + OPT_META_SIZE) {
        s.setFull(true);
        return -1;
      }
      long sdst = s.getAddress() + offset + OPT_META_SIZE;
      // Copy
      UnsafeAccess.copy(src, sdst, len);
      sdst -= OPT_META_SIZE;
      UnsafeAccess.putInt(sdst, len);
      UnsafeAccess.putInt(sdst + Utils.SIZEOF_INT, id);
      UnsafeAccess.putInt(sdst + 2 * Utils.SIZEOF_INT, len);
      // Update segment
      s.setSegmentDataSize(offset + len + OPT_META_SIZE);
      s.setCurrentBlockOffset(offset + len + OPT_META_SIZE);
      s.incrNumEntries(batch.size());
    } finally {
      s.writeUnlock();
    }
    // Now we need update MemoryIndex
    MemoryIndex mi = s.getMemoryIndex();
    // We do not need to read lock b/c this thread is the only writer
    // to this write batch
    int off = 0;
    final short sid = (short) s.getId();
    final int batchId = batch.getId();
    while (off < len) {
      int kSize = Utils.readUVInt(src + off);
      off += Utils.sizeUVInt(kSize);
      int vSize = Utils.readUVInt(src + off);
      off += Utils.sizeUVInt(vSize);
      mi.compareAndUpdate(src + off, kSize, (short) -1, batchId, sid, (int) offset);
      off += kSize + vSize;
    }
    // Reset batch to accept new writes
    batch.reset();
    return offset;
  }

  @Override
  public long appendSingle(Segment s, long keyPtr, int keySize, long valuePtr, int valueSize) {

    final int reqSize = Utils.kvSize(keySize, valueSize);
    final int id = -1;
    long offset = 0;
    try {
      s.writeLock();
      if (s.isFull() || s.isSealed()) {
        return -1;
      }
      offset = s.getSegmentDataSize();
      if (s.size() - offset < reqSize + OPT_META_SIZE) {
        s.setFull(true);
        return -1;
      }
      long sdst = s.getAddress() + offset + OPT_META_SIZE;      
      // Copy
      int off = Utils.writeUVInt(sdst, keySize);
      off += Utils.writeUVInt(sdst + off, valueSize);
      UnsafeAccess.copy(keyPtr, sdst + off, keySize);
      off += keySize;
      UnsafeAccess.copy(valuePtr, sdst + off, valueSize); 
      sdst -= OPT_META_SIZE;
      UnsafeAccess.putInt(sdst, reqSize);
      UnsafeAccess.putInt(sdst + Utils.SIZEOF_INT, id);
      UnsafeAccess.putInt(sdst + 2 * Utils.SIZEOF_INT, reqSize);
      // Update segment
      s.setSegmentDataSize(offset + reqSize + OPT_META_SIZE);
      s.setCurrentBlockOffset(offset + reqSize + OPT_META_SIZE);
      s.incrNumEntries(1);
      return offset;
    } finally {
      s.writeUnlock();
    }
  }

  @Override
  public long appendSingle(Segment s, byte[] key, int keyOffset, int keySize, byte[] value,
      int valueOffset, int valueSize) {

    final int reqSize = Utils.kvSize(keySize, valueSize);
    final int id = -1;
    long offset = 0;
    try {
      s.writeLock();
      if (s.isFull() || s.isSealed()) {
        return -1;
      }
      offset = s.getSegmentDataSize();
      if (s.size() - offset < reqSize + OPT_META_SIZE) {
        s.setFull(true);
        return -1;
      }
      long sdst = s.getAddress() + offset + OPT_META_SIZE;
      // Copy
      int off = Utils.writeUVInt(sdst, keySize);
      off += Utils.writeUVInt(sdst + off, valueSize);
      UnsafeAccess.copy(key, keyOffset, sdst + off, keySize);
      off += keySize;
      UnsafeAccess.copy(value, valueOffset, sdst + off, valueSize); 
      sdst -= OPT_META_SIZE;
      UnsafeAccess.putInt(sdst, reqSize);
      UnsafeAccess.putInt(sdst + Utils.SIZEOF_INT, id);
      UnsafeAccess.putInt(sdst + 2 * Utils.SIZEOF_INT, reqSize);
      // Update segment
      s.setSegmentDataSize(offset + reqSize + OPT_META_SIZE);
      s.setCurrentBlockOffset(offset + reqSize + OPT_META_SIZE);
      s.incrNumEntries(1);
      return offset;
    } finally {
      s.writeUnlock();
    }
  }

  @Override
  public long append(Segment s, long keyPtr, int keySize, long itemPtr, int itemSize) {
    throw new UnsupportedOperationException("append");
  }

  @Override
  public long append(Segment s, byte[] key, int keyOffset, int keySize, byte[] value,
      int valueOffset, int valueSize) {
    throw new UnsupportedOperationException("append");
  }
  
  @Override
  public void init(String cacheName) {
    CacheConfig config = CacheConfig.getInstance();
    this.blockSize = config.getBlockWriterBlockSize(cacheName);
  }
}
