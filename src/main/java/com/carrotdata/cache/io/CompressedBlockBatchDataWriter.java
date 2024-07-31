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

import static com.carrotdata.cache.compression.CompressionCodec.COMP_META_SIZE;

import com.carrotdata.cache.index.MemoryIndex;
import com.carrotdata.cache.util.UnsafeAccess;
import com.carrotdata.cache.util.Utils;

public class CompressedBlockBatchDataWriter extends CompressedBlockDataWriter {

  static ThreadLocal<Long> bufTLS = new ThreadLocal<Long>();

  static ThreadLocal<Integer> bufSizeTLS = new ThreadLocal<Integer>();

  private int bufferSize;

  private long getBuffer(int reqSize) {
    Integer size = bufSizeTLS.get();
    Long ptr = bufTLS.get();
    if (size == null || size < reqSize) {
      reqSize = Math.max(reqSize, 4 * this.blockSize);
      if (ptr != null) {
        UnsafeAccess.free(ptr);
      }
      ptr = UnsafeAccess.mallocZeroed(reqSize);
      this.bufferSize = reqSize;
      bufTLS.set(ptr);
      bufSizeTLS.set(this.bufferSize);
    } else {
      this.bufferSize = size.intValue();
    }
    return ptr.longValue();
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
  public long append(Segment s, WriteBatch batch) {
    checkCodec();
    long src = batch.memory();
    int len = batch.position();
    
    if (len == 0) {
      // Its not possible
      throw new RuntimeException("write batch size is 0");
    }
    
    if (codec.isTrainingRequired()) {
      codec.addTrainingData(src, len);
    }
    long dst = getBuffer(2 * len);
    int dictVersion = this.codec.getCurrentDictionaryVersion();
    int compressed = this.codec.compress(src, len, dictVersion, dst, bufferSize);
    if (compressed >= len) {
      dictVersion = -1;// uncompressed
      compressed = len;
      dst = src;
    }
    long offset = 0;
    try {
      s.writeLock();
      if (s.isFull() || s.isSealed()) {
        //TODO: We compressed write batch already !!! Reuse it
        return -1;
      }
      offset = s.getSegmentDataSize();
      if (s.size() - offset < compressed + COMP_META_SIZE) {
        //TODO: We compressed write batch already !!! Reuse it
        s.setFull(true);
        return -1;
      }
      long sdst = s.getAddress() + offset + COMP_META_SIZE;
      // Copy
      UnsafeAccess.copy(dst, sdst, compressed);
      sdst -= COMP_META_SIZE;
      UnsafeAccess.putInt(sdst, len);
      UnsafeAccess.putInt(sdst + Utils.SIZEOF_INT, dictVersion);
      UnsafeAccess.putInt(sdst + 2 * Utils.SIZEOF_INT, compressed);
      // Update segment
      s.setSegmentDataSize(offset + compressed + COMP_META_SIZE);
      s.setCurrentBlockOffset(offset + compressed + COMP_META_SIZE);
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
    return offset;
  }

  @Override
  public long appendSingle(Segment s, long keyPtr, int keySize, long valuePtr, int valueSize) {
    checkCodec();
    if (codec.isTrainingRequired()) {
      codec.addTrainingData(valuePtr, valueSize);
      //TODO keys
    }
    int reqSize = Utils.kvSize(keySize, valueSize);
    long dst = getBuffer(reqSize);
    // Copy k-v to dst
    int off = Utils.writeUVInt(dst, keySize);
    off += Utils.writeUVInt(dst + off, valueSize);
    UnsafeAccess.copy(keyPtr, dst + off, keySize);
    off += keySize;
    UnsafeAccess.copy(valuePtr, dst + off, valueSize);

    int dictVersion = this.codec.getCurrentDictionaryVersion();
    int compressed = this.codec.compress(dst, reqSize, dictVersion);
    if (compressed >= reqSize) {
      dictVersion = -1;// uncompressed
      compressed = reqSize;
    }
    long offset = 0;
    try {
      s.writeLock();
      if (s.isFull() || s.isSealed()) {
        return -1;
      }
      offset = s.getSegmentDataSize();
      if (s.size() - offset < compressed + COMP_META_SIZE) {
        s.setFull(true);
        return -1;
      }
      long sdst = s.getAddress() + offset + COMP_META_SIZE;
      // Copy
      UnsafeAccess.copy(dst, sdst, compressed);
      sdst -= COMP_META_SIZE;
      UnsafeAccess.putInt(sdst, reqSize);
      UnsafeAccess.putInt(sdst + Utils.SIZEOF_INT, dictVersion);
      UnsafeAccess.putInt(sdst + 2 * Utils.SIZEOF_INT, compressed);
      // Update segment
      s.setSegmentDataSize(offset + compressed + COMP_META_SIZE);
      s.setCurrentBlockOffset(offset + compressed + COMP_META_SIZE);
      s.incrNumEntries(1);
      return offset;
    } finally {
      s.writeUnlock();
    }
  }

  @Override
  public long appendSingle(Segment s, byte[] key, int keyOffset, int keySize, byte[] value,
      int valueOffset, int valueSize) {
    checkCodec();
    if (codec.isTrainingRequired()) {
      codec.addTrainingData(value, valueOffset, valueSize);
      //TODO keys
    }
    int reqSize = Utils.kvSize(keySize, valueSize);
    long dst = getBuffer(reqSize);
    // Copy k-v to dst
    int off = Utils.writeUVInt(dst, keySize);
    off += Utils.writeUVInt(dst + off, valueSize);
    UnsafeAccess.copy(key, keyOffset, dst + off, keySize);
    off += keySize;
    UnsafeAccess.copy(value, 0, dst + off, valueSize);

    int dictVersion = this.codec.getCurrentDictionaryVersion();
    int compressed = this.codec.compress(dst, reqSize, dictVersion);
    if (compressed >= reqSize) {
      dictVersion = -1;// uncompressed
      compressed = reqSize;
    }
    long offset = 0;
    try {
      s.writeLock();
      if (s.isFull() || s.isSealed()) {
        return -1;
      }
      offset = s.getSegmentDataSize();
      if (s.size() - offset < compressed + COMP_META_SIZE) {
        s.setFull(true);
        return -1;
      }
      long sdst = s.getAddress() + offset + COMP_META_SIZE;
      // Copy
      UnsafeAccess.copy(dst, sdst, compressed);
      sdst -= COMP_META_SIZE;
      UnsafeAccess.putInt(sdst, reqSize);
      UnsafeAccess.putInt(sdst + Utils.SIZEOF_INT, dictVersion);
      UnsafeAccess.putInt(sdst + 2 * Utils.SIZEOF_INT, compressed);
      // Update segment
      s.setSegmentDataSize(offset + compressed + COMP_META_SIZE);
      s.setCurrentBlockOffset(offset + compressed + COMP_META_SIZE);
      s.incrNumEntries(1);
      return offset;
    } finally {
      s.writeUnlock();
    }
  }
}
