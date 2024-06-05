/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.carrotdata.cache.controllers;

import com.carrotdata.cache.Cache;
import com.carrotdata.cache.index.MemoryIndex;
import com.carrotdata.cache.util.CacheConfig;

public class PromotionQueue extends AdmissionQueue {

  /**
   * Public constructor
   * @param cache parent cache
   */
  public PromotionQueue(Cache cache) {
    this.cache = cache;
    CacheConfig conf = this.cache.getCacheConfig();
    this.cacheName = this.cache.getName();
    this.currentMaxSizeRatio = conf.getPromotionQueueStartSizeRatio(this.cacheName);
    this.globalMaxSizeRatio = conf.getPromotionQueueMaxSizeRatio(this.cacheName);
    this.globalMinSizeRatio = conf.getPromotionQueueMinSizeRatio(this.cacheName);
    this.maxCacheSize = conf.getCacheMaximumSize(this.cacheName);
    this.index = new MemoryIndex(this.cacheName, MemoryIndex.Type.AQ);
  }

  /**
   * Constructor for testing
   */
  public PromotionQueue() {
    this.cacheName = "default";
    CacheConfig conf = CacheConfig.getInstance();
    this.currentMaxSizeRatio = conf.getPromotionQueueStartSizeRatio(this.cacheName);
    this.globalMaxSizeRatio = conf.getPromotionQueueMaxSizeRatio(this.cacheName);
    this.globalMinSizeRatio = conf.getPromotionQueueMinSizeRatio(this.cacheName);
    this.maxCacheSize = conf.getCacheMaximumSize(this.cacheName);
    this.index = new MemoryIndex(this.cacheName, MemoryIndex.Type.AQ);

  }

  /**
   * Constructor for testing
   * @param conf cache configuration
   */
  public PromotionQueue(CacheConfig conf) {
    this.cacheName = "default";
    this.currentMaxSizeRatio = conf.getPromotionQueueStartSizeRatio(this.cacheName);
    this.globalMaxSizeRatio = conf.getPromotionQueueMaxSizeRatio(this.cacheName);
    this.globalMinSizeRatio = conf.getPromotionQueueMinSizeRatio(this.cacheName);
    this.maxCacheSize = conf.getCacheMaximumSize(this.cacheName);
    this.index = new MemoryIndex(this.cacheName, MemoryIndex.Type.AQ);
  }
}
