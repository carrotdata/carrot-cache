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
package com.onecache.core.controllers;

import com.onecache.core.io.Segment;

/**
 * 
 * This selector selects the "oldest" segment(s)
 * Least Recently Created (LRC)
 *
 */
public class PopularityBasedRecyclingSelector implements RecyclingSelector {

  public PopularityBasedRecyclingSelector() {
  }
  
  @Override
  public Segment selectForRecycling(Segment[] segments) {
    Segment selection = null;
    long minCounts = Long.MAX_VALUE;
    long timeWindow = 0; // seconds
    for(int i = 0; i < segments.length; i++) {
      Segment s = segments[i];
      if (s == null || !s.isSealed() || s.isRecycling()) {
        continue;
      }
      Segment.Info info = s.getInfo();
      if (timeWindow == 0) {
 //       timeWindow = s.getCounter().windowSize();
      }
      long maxExpireAt = info.getMaxExpireAt();
      long currentTime = System.currentTimeMillis();
      if (info.getTotalActiveItems() == 0 || 
          (maxExpireAt > 0 && maxExpireAt < currentTime)){
        return s;
      }
      long time = info.getCreationTime();
      if (System.currentTimeMillis() - time < timeWindow ) {
        // Skip most fresh segments whose age is less than timeWindow
        continue;
      }
      long count = 0;//s.getCounter().count();
      if (count < minCounts) {
        minCounts = count;
        selection = s;
      }
    }
    if (selection != null) {
      selection.setRecycling(true);
    }
    return selection;
  }
}
