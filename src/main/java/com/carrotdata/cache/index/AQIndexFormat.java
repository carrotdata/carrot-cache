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
package com.carrotdata.cache.index;

import com.carrotdata.cache.util.UnsafeAccess;
import com.carrotdata.cache.util.Utils;

public final class AQIndexFormat extends AbstractIndexFormat {
  /*
   * AQ Index item is 8 bytes hashed key value
   */
  public AQIndexFormat() {
    super();
  }

  @Override
  public boolean equals(long ptr, long hash) {
    return UnsafeAccess.toLong(ptr) == hash;
  }

  @Override
  public int indexEntrySize() {
    return Utils.SIZEOF_LONG;
  }

  @Override
  public int fullEntrySize(long ptr) {
    return Utils.SIZEOF_LONG;
  }

  @Override
  public long advance(long current) {
    return current + Utils.SIZEOF_LONG;
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
    return Utils.SIZEOF_LONG;
  }

  @Override
  public void writeIndex(long ibPtr, long ptr, byte[] key, int keyOffset, int keySize, byte[] value,
      int valueOffset, int valueSize, int sid, int dataOffset, int dataSize, long expire) {
    long hash = Utils.hash64(key, keyOffset, keySize);
    UnsafeAccess.putLong(ptr, hash);
  }

  @Override
  public void writeIndex(long ibPtr, long ptr, long keyPtr, int keySize, long valuePtr,
      int valueSize, int sid, int dataOffset, int dataSize, long expire) {
    long hash = Utils.hash64(keyPtr, keySize);
    UnsafeAccess.putLong(ptr, hash);
  }

  @Override
  public boolean isSizeSupported() {
    return false;
  }

  @Override
  public int hashOffset() {
    return 0;
  }

  @Override
  public int sidOffset() {
    return 0;
  }

  @Override
  public int dataOffsetOffset() {
    return 0;
  }

  @Override
  public int expireOffset() {
    return 0;
  }

  @Override
  public int sizeOffset() {
    return 0;
  }

  @Override
  public void updateIndex(long ptr, int sid, int dataOffset) {
    // Do nothing
  }

}
