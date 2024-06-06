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

import com.carrotdata.cache.io.Segment;

/**
 * This selector selects the "oldest" segment(s) Least Recently Created (LRC)
 */
public class PopularityBasedRecyclingSelector implements RecyclingSelector {

  public PopularityBasedRecyclingSelector() {
  }

  @Override
  public Segment selectForRecycling(Segment[] segments) {
    Segment selection = null;
    long minCounts = Long.MAX_VALUE;
    long timeWindow = 0; // seconds
    for (int i = 0; i < segments.length; i++) {
      Segment s = segments[i];
      if (s == null || !s.isSealed() || s.isRecycling()) {
        continue;
      }
      Segment.Info info = s.getInfo();
      if (timeWindow == 0) {
        // timeWindow = s.getCounter().windowSize();
      }
      long maxExpireAt = info.getMaxExpireAt();
      long currentTime = System.currentTimeMillis();
      if (info.getTotalActiveItems() == 0 || (maxExpireAt > 0 && maxExpireAt < currentTime)) {
        return s;
      }
      long time = info.getCreationTime();
      if (System.currentTimeMillis() - time < timeWindow) {
        // Skip most fresh segments whose age is less than timeWindow
        continue;
      }
      long count = 0;// s.getCounter().count();
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
