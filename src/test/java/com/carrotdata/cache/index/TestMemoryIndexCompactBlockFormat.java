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
package com.carrotdata.cache.index;

import java.util.Random;

import com.carrotdata.cache.eviction.EvictionPolicy;
import com.carrotdata.cache.eviction.SLRUEvictionPolicy;
import com.carrotdata.cache.util.CacheConfig;

public class TestMemoryIndexCompactBlockFormat extends TestMemoryIndexFormatBase {

  int blockSize;

  @Override
  protected MemoryIndex getMemoryIndex() {
    MemoryIndex index = new MemoryIndex("default");
    EvictionPolicy policy = new SLRUEvictionPolicy();
    index.setEvictionPolicy(policy);
    IndexFormat format = new CompactBlockIndexFormat();
    format.setCacheName("default");
    index.setIndexFormat(format);
    return index;
  }

  @Override
  int nextOffset(Random r, int max) {
    if (this.blockSize == 0) {
      CacheConfig config = CacheConfig.getInstance();
      this.blockSize = config.getBlockWriterBlockSize("default");
    }
    int n = max / this.blockSize;
    return r.nextInt(n) * this.blockSize;
  }

}
