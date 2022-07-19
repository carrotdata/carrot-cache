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

import com.carrot.cache.expire.ExpireSupport;
import com.carrot.cache.util.CacheConfig;

/**
 * Format of an index entry (8 bytes):
 * expire - 2 bytes
 * hash -   2 bytes
 * sid  -   2 bytes
 * offset - 2 bytes 
 * 
 *
 */
public class SubCompactWithExpireIndexFormat extends CompactIndexFormat {
  
  ExpireSupport expireSupport;
  
  public SubCompactWithExpireIndexFormat() {
    
  }

  @Override
  public boolean isExpirationSupported() {
    return true;
  }

  @Override
  public long getExpire(long ibPtr, long ptr) {
    ibPtr += super.getIndexBlockHeaderSize();
    ptr += expireOffset();
    return this.expireSupport.getExpire(ibPtr, ptr);
  }

  @Override
  public boolean begin(long ibPtr, boolean force) {
    ibPtr += super.getIndexBlockHeaderSize();
    return this.expireSupport.begin(ibPtr, force);
  }

  @Override
  public void end(long ibPtr) {
    ibPtr += super.getIndexBlockHeaderSize();
    this.expireSupport.end(ibPtr);
  }

  @Override
  public void setCacheName(String cacheName) {
    super.setCacheName(cacheName);
    try {
      this.expireSupport = CacheConfig.getInstance().getExpireSupport(cacheName);
    } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
      // TODO Auto-generated catch block
      throw new RuntimeException(e);
    }
  }

  @Override
  public int getIndexBlockHeaderSize() {
    return super.getIndexBlockHeaderSize() + this.expireSupport.getExpireMetaSectionSize();
  }

  @Override
  public int getEmbeddedOffset() {
    return super.getEmbeddedOffset() + ExpireSupport.FIELD_SIZE;
  }

  @Override
  public int indexEntrySize() {
    return super.indexEntrySize() + ExpireSupport.FIELD_SIZE;
  }
  
  @Override
  public void writeIndex(
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
      long expire) {
    super.writeIndex(ibPtr, ptr, key, keyOffset, keySize, value, valueOffset,
      valueSize,sid,dataOffset,dataSize,expire);
    ibPtr += super.getIndexBlockHeaderSize();
    this.expireSupport.setExpire(ibPtr, ptr + expireOffset(), expire);
    
  }

  @Override
  public void writeIndex(
      long ibPtr,
      long ptr,
      long keyPtr,
      int keySize,
      long valuePtr,
      int valueSize,
      int sid,
      int dataOffset,
      int dataSize,
      long expire) {
    super.writeIndex(ibPtr, ptr, keyPtr, keySize, valuePtr, 
      valueSize, sid, dataOffset, dataSize, expire);
    ibPtr += super.getIndexBlockHeaderSize();
    this.expireSupport.setExpire(ibPtr, ptr + expireOffset(), expire);

  }

  @Override
  int hashOffset() {
    return super.hashOffset() + ExpireSupport.FIELD_SIZE;
  }

  @Override
  int sidOffset() {
    return super.sidOffset() + ExpireSupport.FIELD_SIZE;
  }

  @Override
  int dataOffsetOffset() {
    return super.dataOffsetOffset() + ExpireSupport.FIELD_SIZE;
  }

  @Override
  int expireOffset() {
    return 0;
  }
  
  @Override
  public boolean isSizeSupported() {
    return false;
  }
}
