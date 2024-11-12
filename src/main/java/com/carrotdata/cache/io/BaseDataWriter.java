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

import static com.carrotdata.cache.io.BlockReaderWriterSupport.OPT_META_SIZE;

import com.carrotdata.cache.index.MemoryIndex;
import com.carrotdata.cache.util.CacheConfig;
import com.carrotdata.cache.util.UnsafeAccess;
import com.carrotdata.cache.util.Utils;

public class BaseDataWriter implements DataWriter {

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
