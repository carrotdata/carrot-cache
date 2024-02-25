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

import com.onecache.core.io.DataWriter;
import com.onecache.core.io.Segment;
import com.onecache.core.util.UnsafeAccess;
import com.onecache.core.util.Utils;

public class BaseDataWriter implements DataWriter {
  
  public BaseDataWriter() {
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
  public long append(
      Segment s,
      byte[] key,
      int keyOffset,
      int keySize,
      byte[] value,
      int valueOffset,
      int valueSize) {
    int requiredSize = Utils.requiredSize(keySize, valueSize);
    if (requiredSize + s.getSegmentDataSize() > s.size()) {
      return -1;
    }
    if (s.getAddress() == 0){
      /*DEBUG*/ System.err.printf("PTR=NULL sid=%d dataSize=%d isSealed=%s isValid=%s isOffheap=%s\n",
            s.getId(), s.getSegmentDataSize(), Boolean.toString(s.isSealed()), Boolean.toString(s.isValid()),
            Boolean.toString(s.isOffheap()));
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
  public void init(String cacheName) {
    // do nothing
    
  }
}
