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
package com.carrot.cache.io;

import com.carrot.cache.util.UnsafeAccess;
import com.carrot.cache.util.Utils;

public class BaseDataAppender implements DataAppender {
  
  public BaseDataAppender() {
  }
  
  @Override
  public long append(Segment s, long keyPtr, int keySize, long itemPtr, int itemSize) {
    int requiredSize = Utils.requiredSize(keySize, itemSize);
    if (requiredSize + s.dataSize() > s.size()) {
      return -1;
    }
    long addr = s.getAddress() + s.dataSize();
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
    return requiredSize;
  }

  @Override
  public long append(
      Segment s,
      byte[] key,
      int keyOffset,
      int keySize,
      byte[] value,
      int valueOffset,
      int valueSize) {
    int requiredSize = Utils.requiredSize(keySize, valueSize);
    if (requiredSize + s.dataSize() > s.size()) {
      return -1;
    }
    long addr = s.getAddress() + s.dataSize();
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
    return requiredSize;
  }

  @Override
  public void init(String cacheName) {
    // do nothing
    
  }
}
