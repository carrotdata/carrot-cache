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
 * This selector selects the "youngest" segment Most Recently Created (MRC)
 */
public class MRCRecyclingSelector implements RecyclingSelector {

  public MRCRecyclingSelector() {
  }

  @Override
  public Segment selectForRecycling(Segment[] segments) {
    Segment selection = null;
    long maxCreationTime = Long.MIN_VALUE;
    for (int i = 0; i < segments.length; i++) {
      Segment s = segments[i];
      if (s == null || !s.isSealed() || s.isRecycling()) {
        continue;
      }
      Segment.Info info = s.getInfo();
      long maxExpireAt = info.getMaxExpireAt();
      long currentTime = System.currentTimeMillis();
      if (info.getTotalActiveItems() == 0 || (maxExpireAt > 0 && maxExpireAt < currentTime)) {
        return s;
      }
      long time = info.getCreationTime();
      if (time > maxCreationTime) {
        maxCreationTime = time;
        selection = s;
      }
    }
    if (selection != null) {
      selection.setRecycling(true);
    }
    return selection;
  }
}
