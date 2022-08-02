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

import java.io.IOException;

import com.carrot.cache.Cache;
import com.carrot.cache.io.Segment;
import com.carrot.cache.util.CacheConfig;

/**
 * This admission controller can be used 
 * for Disk - based caches only. It employs admission queue to decide
 * acceptance and throughput control
 * the only thing it does differently  - it adjusts
 * item ranks according to its TTL, based on predefined set of bins, defined in configuration file. 
 * The lower TTL - the lower rank of an item is going to be.
 *
 */

public class ExpirationAwareAdmissionControllerDisk extends AQBasedAdmissionController{
  
  long[] ttlBins;
  
  public ExpirationAwareAdmissionControllerDisk() {
  }
  
  @Override
  public void setCache(Cache cache) throws IOException {
    super.setCache(cache);
    CacheConfig conf = this.cache.getCacheConfig();
    int numRanks = conf.getNumberOfPopularityRanks(cache.getName());
    this.ttlBins = new long[numRanks];
    
    long binStart = conf.getExpireStartBinValue(cache.getName());
    double multiplier = conf.getExpireBinMultiplier(cache.getName());
    
    this.ttlBins[numRanks - 1] = binStart;
    this.ttlBins[0] = 0;  // no upper limit
    
    for (int i = numRanks - 2; i > 0; i--) {
      this.ttlBins[i] = (long) (multiplier * this.ttlBins[i + 1]);
    }
  }

  @Override
  public int adjustRank(int rank, long expire) {
    if (expire <= 0 || this.ttlBins[rank] < expire) {
      return rank;
    }
    for (int i = rank + 1; i < this.ttlBins.length; i++) {
      if (this.ttlBins[i] < expire) return i - 1;
    }
    return rank;
  } 
  
  @Override
  public void startSegment(Segment s) {
    // Do nothing
  }
  
  @Override
  public void finishSegment(Segment s) {
    // Do nothing
  }
}
