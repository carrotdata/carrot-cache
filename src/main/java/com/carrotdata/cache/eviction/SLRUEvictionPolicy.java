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
package com.carrotdata.cache.eviction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotdata.cache.util.CacheConfig;

public class SLRUEvictionPolicy implements EvictionPolicy {
  /** Logger */
  @SuppressWarnings("unused")
  private static final Logger LOG = LoggerFactory.getLogger(SLRUEvictionPolicy.class);
  
  /* Number of segments in SLRU*/
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
