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

public class AQIndexFormat implements IndexFormat {
  /*
   * AQ Index item is 8 bytes hashed key value
   */
  public AQIndexFormat() {}
  
  @Override
  public boolean equals(long ptr, long hash) {
    return UnsafeAccess.toLong(ptr) == hash;
  }

  @Override
  public final int indexEntrySize() {
    return Utils.SIZEOF_LONG;
  }

  @Override
  public final int fullEntrySize(long ptr) {
    return indexEntrySize();
  }

  @Override
  public long advance(long current) {
    return current + fullEntrySize(current);
  }

  @Override
  public int find(long ibPtr, long keyPtr, int keySize, boolean hit, long buffer) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int find(long ibPtr, byte[] key, int keyOffset, int keySize, boolean hit, long buffer) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int getKeyValueSize(long buffer) {
    // Does not support
    return 0;
  }

  @Override
  public int getSegmentId(long buffer) {
    // Does not support
    return 0;
  }

  @Override
  public long getOffset(long buffer) {
    // Does not support
    return 0;
  }

  @Override
  public long getExpire(long ibPtr, long buffer) {
    // Does not support
    return 0;
  }

  @Override
  public int getIndexBlockHeaderSize() {
    return 3 * Utils.SIZEOF_SHORT;
  }

  @Override
  public int getHitCount(long buffer) {
    // Does not support
    return 0;
  }

  @Override
  public void hit(long ptr) {
    // do nothing
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
    long hash = Utils.hash64(key, keyOffset, keySize);
    UnsafeAccess.putLong(ptr, hash);
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
    long hash = Utils.hash64(keyPtr, keySize);
    UnsafeAccess.putLong(ptr, hash);
  }
}
