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
package com.carrot.cache.index;

import com.carrot.cache.util.UnsafeAccess;
import com.carrot.cache.util.Utils;

/**
 * 
 * Index format for main queue (cache)
 * It does not support expiration
 * 
 */
public class MQIndexFormat implements IndexFormat {
  /*
   * MQ Index item is 20 bytes:
   * 8 bytes - hashed key value
   * 4 bytes - total item size (key + value)
   * 8 bytes - location in the storage - information 
   */
  public MQIndexFormat() {
  }
  
  @Override
  public boolean equals(long ptr, long hash) {
    return UnsafeAccess.toLong(ptr) == hash;
  }

  @Override
  public final int indexEntrySize() {
    return Utils.SIZEOF_INT + 2 * Utils.SIZEOF_LONG;
  }

  @Override
  public final int fullEntrySize(long ptr) {
    return indexEntrySize();
  }
  
  @Override
  public long advance(long current) {
    return current + fullEntrySize(current);
  }

  //TODO: remove this API
  @Override
  public int find(long ibPtr, long keyPtr, int keySize, boolean hit, long buffer) {
    // TODO Auto-generated method stub
    return 0;
  }
  
  //TODO: remove this API
  @Override
  public int find(long ibPtr, byte[] key, int keyOffset, int keySize, boolean hit, long buffer) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int getKeyValueSize(long buffer) {
    return UnsafeAccess.toInt(buffer + Utils.SIZEOF_LONG);
  }

  @Override
  public int getSegmentId(long buffer) {
    long ref = UnsafeAccess.toLong(buffer + Utils.SIZEOF_INT + Utils.SIZEOF_LONG);
    // Segment id (low 2 bytes of a first 4 bytes )
    return (int) (ref >>> 32) & 0xffff;    
  }

  @Override
  public long getOffset(long buffer) {
    long ref = UnsafeAccess.toLong(buffer + Utils.SIZEOF_INT + Utils.SIZEOF_LONG);
    return ref & 0xffffffff;
  }

  @Override
  public int getEmbeddedOffset() {
    return Utils.SIZEOF_LONG + Utils.SIZEOF_INT; 
  }
  
  @Override
  public long getExpire(long ibPtr, long buffer) {
    // Does not support expiration
    return 0;
  }

  @Override
  public int getIndexBlockHeaderSize() {
    return 3 * Utils.SIZEOF_SHORT;
  }

  @Override
  public int getHitCount(long buffer) {
    long ref = UnsafeAccess.toLong(buffer + Utils.SIZEOF_INT + Utils.SIZEOF_LONG);
    // Segment id (high 2 bytes of a first 4 bytes )
    return (int) (ref >>> 32) & 0xffff0000;   
  }

  @Override
  public void hit(long ptr) {
    ptr += Utils.SIZEOF_INT + Utils.SIZEOF_LONG;
    int v = UnsafeAccess.toInt(ptr);
    v |= 0x80000000;
    UnsafeAccess.putInt(ptr, v);
  }

  @Override
  public int fullEntrySize(int keySize, int valueSize) {
    return indexEntrySize();
  }

  @Override
  public void writeIndex(
      long ptr,
      byte[] key,
      int keyOffset,
      int keySize,
      byte[] value,
      int valueOffset,
      int valueSize,
      short sid,
      int dataOffset,
      int dataSize) 
  {
    long hash = Utils.hash8(key, keyOffset, keySize);
    UnsafeAccess.putLong(ptr, hash);
    ptr += Utils.SIZEOF_LONG;
    UnsafeAccess.putInt(ptr, dataSize);
    ptr += Utils.SIZEOF_INT;
    UnsafeAccess.putShort(ptr, sid);
    ptr += Utils.SIZEOF_INT; // Yes, 4 bytes
    UnsafeAccess.putInt(ptr, dataSize);
  }

  @Override
  public void writeIndex(
      long ptr,
      long keyPtr,
      int keySize,
      long valuePtr,
      int valueSize,
      short sid,
      int dataOffset,
      int dataSize) 
  {
    long hash = Utils.hash8(keyPtr, keySize);
    UnsafeAccess.putLong(ptr, hash);
    ptr += Utils.SIZEOF_LONG;
    UnsafeAccess.putInt(ptr, dataSize);
    ptr += Utils.SIZEOF_INT;
    UnsafeAccess.putShort(ptr, sid);
    ptr += Utils.SIZEOF_INT; // Yes, 4 bytes
    UnsafeAccess.putInt(ptr, dataSize);
  }
}
