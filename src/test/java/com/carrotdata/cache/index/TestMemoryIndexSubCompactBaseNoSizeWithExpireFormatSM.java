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

import com.carrotdata.cache.eviction.EvictionPolicy;
import com.carrotdata.cache.eviction.SLRUEvictionPolicy;
import com.carrotdata.cache.expire.ExpireSupportSecondsMinutes;

public class TestMemoryIndexSubCompactBaseNoSizeWithExpireFormatSM
    extends TestMemoryIndexFormatBase {

  int blockSize;

  @Override
  protected MemoryIndex getMemoryIndex() {
    MemoryIndex index = new MemoryIndex("default");
    EvictionPolicy policy = new SLRUEvictionPolicy();
    index.setEvictionPolicy(policy);
    SubCompactBaseNoSizeWithExpireIndexFormat format =
        new SubCompactBaseNoSizeWithExpireIndexFormat();
    format.setCacheName("default");
    format.setExpireSupport(new ExpireSupportSecondsMinutes());
    index.setIndexFormat(format);
    return index;
  }

  @Override
  long nextExpire() {
    return System.currentTimeMillis() + 1000 * 1000;
  }

}
