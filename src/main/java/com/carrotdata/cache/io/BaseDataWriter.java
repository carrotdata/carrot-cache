/*
 * Copyright (C) 2024-present Carrot Data, Inc. 
 * <p>This program is free software: you can redistribute it
 * and/or modify it under the terms of the Server Side Public License, version 1, as published by
 * MongoDB, Inc.
 * <p>This program is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
 * PURPOSE. See the Server Side Public License for more details. 
 * <p>You should have received a copy of the Server Side Public License along with this program. If not, see
 * <http://www.mongodb.com/licensing/server-side-public-license>.
 */
package com.carrotdata.cache.io;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotdata.cache.util.UnsafeAccess;
import com.carrotdata.cache.util.Utils;

public class BaseDataWriter implements DataWriter {
  private static final Logger LOG = LoggerFactory.getLogger(BaseDataWriter.class);

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
  public long append(Segment s, byte[] key, int keyOffset, int keySize, byte[] value,
      int valueOffset, int valueSize) {
    int requiredSize = Utils.requiredSize(keySize, valueSize);
    if (requiredSize + s.getSegmentDataSize() > s.size()) {
      return -1;
    }
    if (s.getAddress() == 0) {
      LOG.error("PTR=NULL sid={} dataSize={} isSealed={} isValid={} isOffheap={}", s.getId(),
        s.getSegmentDataSize(), Boolean.toString(s.isSealed()), Boolean.toString(s.isValid()),
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
