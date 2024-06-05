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

import com.carrotdata.cache.io.Segment;

/**
 * This selector selects the "oldest" segment Least Recently Created (LRC)
 */
public class LeastAccessedRecyclingSelector implements RecyclingSelector {

  public LeastAccessedRecyclingSelector() {
  }

  @SuppressWarnings("unused")
  @Override
  public Segment selectForRecycling(Segment[] segments) {
    Segment selection = null;
    long minAccess = Long.MAX_VALUE;
    int nulls = 0;
    int notSealed = 0;

    for (int i = 0; i < segments.length; i++) {
      Segment s = segments[i];
      if (s == null) {
        nulls++;
        continue;
      }
      if (!s.isSealed()) {
        notSealed++;
        continue;
      }

      if (s.isRecycling()) {
        continue;
      }

      Segment.Info info = s.getInfo();
      long maxExpireAt = info.getMaxExpireAt();
      long currentTime = System.currentTimeMillis();
      if (info.getTotalActiveItems() == 0 || (maxExpireAt > 0 && maxExpireAt < currentTime)) {
        return s;
      }
      long n = info.getAccessCount();
      if (n < minAccess) {
        minAccess = n;
        selection = s;
      }
    }
    if (selection != null) {
      selection.setRecycling(true);
    }
    return selection;
  }
}
