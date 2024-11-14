/*
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
package com.carrotdata.cache.io;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotdata.cache.index.MemoryIndex;
import com.carrotdata.cache.util.CacheConfig;
import com.carrotdata.cache.util.UnsafeAccess;
import com.carrotdata.cache.util.Utils;

public class BaseBatchDataWriter implements DataWriter {

  @SuppressWarnings("unused")
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
      int kSizeSize = Utils.sizeUVInt(kSize);
      int vSize = Utils.readUVInt(src + off + kSizeSize);
      int vSizeSize = Utils.sizeUVInt(vSize);
      mi.compareAndUpdate(src + off + kSizeSize + vSizeSize, kSize, (short) -1, id, sid, (int) offset + off);
      off += kSize + vSize + kSizeSize + vSizeSize;
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
