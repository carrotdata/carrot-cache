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
package com.carrot.cache.eviction;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SLRUEvictionPolicy implements EvictionPolicy {
  /** Logger */
  @SuppressWarnings("unused")
  private static final Logger LOG = LogManager.getLogger(SLRUEvictionPolicy.class);
  
  public final static int DEFAULT_NUMBER_SEGMENTS = 8;
  
  public final static int DEFAULT_INSERT_POINT = 5;
  
  private final int numSegments;
  
  private final int insertPoint;
  
  /**
   * Default constructor
   */
  public SLRUEvictionPolicy() {
    this.numSegments = 8;
    this.insertPoint = 5;
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
  public int getPromotionIndex(long cacheItemPtr, int cacheItemIndex, int totalItems) {
    int num = getSegmentForIndex(numSegments, cacheItemIndex, totalItems);
    if (num == 0) return 0;
    return getStartIndexForSegment(numSegments, num - 1, totalItems);
  }
  
  
  @Override
  public int getEvictionCandidateIndex(long idbPtr, int totalItems) {
    return totalItems - 1;
  }

  @Override
  public int getInsertIndex(long idbPtr, int totalItems) {
    // insert point is 0- based, 
    return getStartIndexForSegment(numSegments, insertPoint - 1, totalItems);
  }
}
