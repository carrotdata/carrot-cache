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
 * This selector selects a segment with a minimum number of alive items
 */
public class MinAliveRecyclingSelector implements RecyclingSelector {

  public MinAliveRecyclingSelector() {
  }

  @Override
  public Segment selectForRecycling(Segment[] segments) {
    Segment selection1 = null, selection2 = null;
    // TODO Make it configurable
    double minRatio = 0.99;// little hack
    long minCreationTime = Long.MAX_VALUE;

    for (int i = 0; i < segments.length; i++) {
      Segment s = segments[i];
      if (s == null || !s.isSealed() || s.isRecycling()) {
        continue;
      }
      Segment.Info info = s.getInfo();

      long maxExpire = info.getMaxExpireAt();
      if (maxExpire > 0 && System.currentTimeMillis() > maxExpire) {
        // All expired
        return s;
      }
      int active = info.getTotalActiveItems();
      int total = info.getTotalItems();
      double r = (double) active / total;
      if (r < minRatio) {
        minRatio = r;
        selection1 = s;
      }
      long creationTime = info.getCreationTime();
      if (creationTime < minCreationTime) {
        minCreationTime = creationTime;
        selection2 = s;
      }
    }
    Segment ss = selection1 != null ? selection1 : selection2;
    if (ss != null) {
      ss.setRecycling(true);
    }
    return ss;
  }
}
