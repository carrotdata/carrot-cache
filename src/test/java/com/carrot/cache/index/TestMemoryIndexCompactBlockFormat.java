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
package com.carrot.cache.index;

import java.util.Random;

import com.carrot.cache.eviction.EvictionPolicy;
import com.carrot.cache.eviction.SLRUEvictionPolicy;
import com.carrot.cache.util.CarrotConfig;

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
      CarrotConfig config = CarrotConfig.getInstance();
      this.blockSize = config.getBlockWriterBlockSize("default");
    }
    int n = max / this.blockSize;
    return r.nextInt(n) * this.blockSize;
  }
  
  
}
