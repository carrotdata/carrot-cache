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
package com.carrot.cache.controllers;

import com.carrot.cache.io.Segment;

/**
 * 
 * This selector selects a segment with a lowest average item rank
 *
 */
public class LowestRankRecyclingSelector implements RecyclingSelector {

  public LowestRankRecyclingSelector() {   
  }
  
  @Override
  public Segment selectForRecycling(Segment[] segments) {
    int selectionIndex = -1;
    double minRank = Double.MAX_VALUE;
    for(int i = 0; i < segments.length; i++) {
      Segment s = segments[i];
      if (s == null || !s.isSealed()) continue;
      Segment.Info info = s.getInfo();
      double rank = info.getAverageRank();
      if (s.isAllExpireSegment()) {
        long maxExpire = info.getMaxExpireAt();
        if (System.currentTimeMillis() > maxExpire) {
          s.allExpired();
          rank = 0;
        }
      }
      if (rank < minRank) {
        minRank = rank;
        selectionIndex = i;
      }
    }
    return segments[selectionIndex];
  }
}