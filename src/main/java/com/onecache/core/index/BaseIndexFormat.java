/**
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
package com.onecache.core.index;

import com.onecache.core.util.UnsafeAccess;
import com.onecache.core.util.Utils;

/**
 * 
 * Index format for main queue (cache)
 * It does not support expiration
 * 
 */
public class BaseIndexFormat extends AbstractIndexFormat {
  /*
   * MQ Index item is 20 bytes:
   * 8 bytes - hashed key value
   * 4 bytes - total item size (key + value)
   * 8 bytes - location in the storage - information 
   */

  public BaseIndexFormat() {
    super();
  }
  
  @Override
  public final boolean equals(long ptr, long hash) {
    return UnsafeAccess.toLong(ptr) == hash;
  }

  @Override
  public final int indexEntrySize() {
    return Utils.SIZEOF_LONG + 3 * Utils.SIZEOF_INT;
  }

  @Override
  public final int fullEntrySize(long ptr) {
    return Utils.SIZEOF_LONG + 3 * Utils.SIZEOF_INT;
  }
  
  @Override
  public final long advance(long current) {
    return current + Utils.SIZEOF_LONG + 3 * Utils.SIZEOF_INT;
  }

  @Override
  public final int getKeyValueSize(long buffer) {
    return UnsafeAccess.toInt(buffer + Utils.SIZEOF_LONG);
  }

  @Override
  public final int getSegmentId(long buffer) {
    int ref = UnsafeAccess.toInt(buffer + Utils.SIZEOF_INT + Utils.SIZEOF_LONG);
    // Segment id (low 2 bytes of a first 4 bytes )
    return  ref  & 0xffff;    
  }

  @Override
  public final long getOffset(long buffer) {
    long ref = UnsafeAccess.toInt(buffer + 2 * Utils.SIZEOF_INT + Utils.SIZEOF_LONG);
    return ref & 0xffffffff;
  }

  @Override
  public final int getEmbeddedOffset() {
    return Utils.SIZEOF_LONG + Utils.SIZEOF_INT; 
  }
  
  @Override
  public final long getExpire(long ibPtr, long buffer) {
    // Does not support expiration
    return -1;
  }

  @Override
  public final int getIndexBlockHeaderSize() {
    return 3 * Utils.SIZEOF_SHORT;
  }

  @Override
  public final int getHitCount(long buffer) {
    int ref = UnsafeAccess.toInt(buffer + Utils.SIZEOF_INT + Utils.SIZEOF_LONG);
    // Segment id (low 2 bytes of a first 4 bytes )
    return  ref >>> 31;   
  }

  @Override
  public final void hit(long ptr) {
    ptr += Utils.SIZEOF_INT + Utils.SIZEOF_LONG;
    int v = UnsafeAccess.toInt(ptr);
    v |= 0x80000000;
    UnsafeAccess.putInt(ptr, v);
  }

  @Override
  public int fullEntrySize(int keySize, int valueSize) {
    return Utils.SIZEOF_LONG + 3 * Utils.SIZEOF_INT;
  }

  @Override
  public final void writeIndex(
      long ibPtr,
      long ptr,
      byte[] key,
      int keyOffset,
      int keySize,
      byte[] value,
      int valueOffset,
      int valueSize,
      int sid,
      int dataOffset,
      int dataSize,
      long expire /* not supported here*/) 
  {
    long hash = Utils.hash64(key, keyOffset, keySize);
    UnsafeAccess.putLong(ptr + this.hashOffset, hash);
    UnsafeAccess.putInt(ptr + this.sizeOffset, dataSize);
    UnsafeAccess.putInt(ptr + this.sidOffset, sid);
    UnsafeAccess.putInt(ptr + this.dataOffsetOffset, dataOffset);
  }

  @Override
  public final void writeIndex(
      long ibPtr,
      long ptr,
      long keyPtr,
      int keySize,
      long valuePtr,
      int valueSize,
      int sid,
      int dataOffset,
      int dataSize,
      long expire) 
  {
    long hash = Utils.hash64(keyPtr, keySize);
    UnsafeAccess.putLong(ptr + this.hashOffset, hash);
    UnsafeAccess.putInt(ptr + this.sizeOffset, dataSize);
    UnsafeAccess.putInt(ptr + this.sidOffset, sid);
    UnsafeAccess.putInt(ptr + this.dataOffsetOffset, dataOffset);
  }

  @Override
  public int hashOffset() {
    return 0;
  }

  @Override
  public int sidOffset() {
    return Utils.SIZEOF_LONG + Utils.SIZEOF_INT;
  }

  @Override
  public int dataOffsetOffset() {
    return 2 * Utils.SIZEOF_LONG ;
  }

  @Override
  public int expireOffset() {
    return -1;
  }

  @Override
  public int sizeOffset() {
    return Utils.SIZEOF_LONG;
  }

  @Override
  public final void updateIndex(long ptr, int sid, int dataOffset) {
    UnsafeAccess.putInt(ptr + this.sidOffset, sid);
    UnsafeAccess.putInt(ptr + this.dataOffsetOffset, dataOffset);    
  }
}
