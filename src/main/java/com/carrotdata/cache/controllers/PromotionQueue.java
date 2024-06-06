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
