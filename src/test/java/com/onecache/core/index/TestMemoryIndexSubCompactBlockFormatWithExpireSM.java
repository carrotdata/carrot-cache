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
package com.onecache.core.index;

import java.util.Random;

import com.onecache.core.eviction.EvictionPolicy;
import com.onecache.core.eviction.SLRUEvictionPolicy;
import com.onecache.core.expire.ExpireSupportSecondsMinutes;
import com.onecache.core.util.CacheConfig;

public class TestMemoryIndexSubCompactBlockFormatWithExpireSM extends TestMemoryIndexFormatBase {

  int blockSize;
  
  @Override
  protected MemoryIndex getMemoryIndex() {
    MemoryIndex index = new MemoryIndex("default");
    EvictionPolicy policy = new SLRUEvictionPolicy();
    index.setEvictionPolicy(policy);
    SubCompactBlockWithExpireIndexFormat format = new SubCompactBlockWithExpireIndexFormat();
    format.setCacheName("default");
    format.setExpireSupport(new ExpireSupportSecondsMinutes());
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
  
  @Override
  long nextExpire() {
    return System.currentTimeMillis() + 1000 * 1000;
  }
}