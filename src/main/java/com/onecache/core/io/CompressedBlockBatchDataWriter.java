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
package com.onecache.core.io;

import com.onecache.core.index.MemoryIndex;
import com.onecache.core.util.UnsafeAccess;
import com.onecache.core.util.Utils;

import static com.onecache.core.compression.CompressionCodec.COMP_META_SIZE;

public class CompressedBlockBatchDataWriter extends CompressedBlockDataWriter {

  static ThreadLocal<Long> bufTLS = new ThreadLocal<Long>();
   
  private int bufferSize;
  
  private long getBuffer() {
    Long ptr = bufTLS.get();
    if (ptr == null) {
      this.bufferSize = 4 * this.blockSize;
      ptr = UnsafeAccess.mallocZeroed(this.bufferSize);
      bufTLS.set(ptr);
    }
    return ptr;
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
    processEmptySegment(s);
    checkCodec();
    long src = batch.memory();
    int len = batch.capacity();
    long dst = getBuffer();
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
      offset = s.getSegmentDataSize();
      if (s.size() - offset < compressed + COMP_META_SIZE) {
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

}
