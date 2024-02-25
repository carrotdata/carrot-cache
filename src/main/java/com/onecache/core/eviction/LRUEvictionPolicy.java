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
package com.onecache.core.eviction;

public class LRUEvictionPolicy implements EvictionPolicy {
  
  public LRUEvictionPolicy() {
  }
  
  @Override
  public int getPromotionIndex(long cacheItemPtr, int cacheItemIndex, int totalItems) {
    // promotes always into the head
    return 0;
  }

  @Override
  public int getEvictionCandidateIndex(long idbPtr, int totalItems) {
    // evicts last one
    return totalItems -1;
  }

  @Override
  public int getInsertIndex(long idbPtr, int totalItems) {
    // Inserts always into the head
    return 0;
  }
}
