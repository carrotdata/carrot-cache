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
 * This selector selects a segment with a minimum number of alive items
 *
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
    
    for(int i = 0; i < segments.length; i++) {
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
        selection1  = s;
      }
      long creationTime = info.getCreationTime();
      if (creationTime < minCreationTime) {
        minCreationTime = creationTime;
        selection2 = s;
      }
    }
    Segment ss = selection1 != null? selection1: selection2;
    if (ss != null) {
      ss.setRecycling(true);
    }
    return ss;
  }
}
