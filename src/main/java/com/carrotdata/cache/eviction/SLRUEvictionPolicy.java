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
package com.carrotdata.cache.eviction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotdata.cache.util.CacheConfig;

public class SLRUEvictionPolicy implements EvictionPolicy {
  /** Logger */
  @SuppressWarnings("unused")
  private static final Logger LOG = LoggerFactory.getLogger(SLRUEvictionPolicy.class);

  /* Number of segments in SLRU */
  private int numSegments;

  /* Insertion segment */
  private int insertPoint;

  /* Default rank to insert */
  private int defaultRank;

  /**
   * Default constructor
   */
  public SLRUEvictionPolicy() {
    this.numSegments = CacheConfig.DEFAULT_SLRU_NUMBER_SEGMENTS;
    this.insertPoint = CacheConfig.DEFAULT_SLRU_CACHE_INSERT_POINT;
  }

  /**
   * Constructor
   * @param ns number of segments
   * @param ip insert point
   */
  public SLRUEvictionPolicy(int ns, int ip) {
    this.numSegments = ns;
    this.insertPoint = ip;
  }

  @Override
  public void setCacheName(String cacheName) {
    CacheConfig config = CacheConfig.getInstance();
    this.numSegments = config.getSLRUNumberOfSegments(cacheName);
    this.insertPoint = config.getSLRUInsertionPoint(cacheName);
    int numRanks = config.getNumberOfPopularityRanks(cacheName);
    this.defaultRank = this.insertPoint * numRanks / this.numSegments;
  }

  @Override
  public int getPromotionIndex(long cacheItemPtr, int cacheItemIndex, int totalItems) {
    int num = getRankForIndex(numSegments, cacheItemIndex, totalItems);
    if (num == 0) return 0;
    return getStartIndexForRank(numSegments, num - 1, totalItems);
  }

  @Override
  public int getEvictionCandidateIndex(long idbPtr, int totalItems) {
    return totalItems - 1;
  }

  @Override
  public int getInsertIndex(long idbPtr, int totalItems) {
    // insert point is 0- based,
    return getStartIndexForRank(numSegments, insertPoint, totalItems);
  }

  @Override
  public int getDefaultRankForInsert() {
    return this.defaultRank;
  }
}
