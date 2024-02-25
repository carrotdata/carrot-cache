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

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.onecache.core.controllers.AdmissionController;
import com.onecache.core.controllers.ExpirationAwareAdmissionController;
import com.onecache.core.Cache;
import com.onecache.core.util.CarrotConfig;

/**
 * This admission controller can be used for RAM - based caches only.
 * It accepts all items, it does not control
 * write throughput, the only thing it does - it adjusts
 * item ranks according to its TTL. The lower TTL - the lower rank of 
 * an item is going to be. It adjusts  expiration time in a such way that 
 * for every bin (rank) all admitted items must have the same relative expiration time
 * to guarantee property of monotonicity:
 * 
 * 1. All cached items in the same bin have the same relative expiration time
 * 2. If two data segments: S1 and S2 have the same rank (bin) and t1 is S! creation time, 
 *   t2 - S2 creation time, then:
 *   2.1 if t2 &gt; t1 and S2 is all-expired-items segment, then S1 is also - all-expired-items segment
 *   2.2 if S1 is not all-expired-items segment, then S2 is full (no expired items at all) 
 * 
 * The controller MUST decrease expiration time only. One exclusion is when expiration time 
 * is less than minimum supported by the system. In a such case the system assigns the minimum 
 * supported expiration time and MUST log warning message.
 * 
 * For items w/o expiration the maximum supported expiration time MUST be assigned
 * But this is not recommended. 
 *
 */

public class ExpirationAwareAdmissionController implements AdmissionController{
  /** Logger */
  private static final Logger LOG = LogManager.getLogger(ExpirationAwareAdmissionController.class);
  long[] ttlBins;
  long binStart;
  double multiplier;
  
  public ExpirationAwareAdmissionController() {
  }
  
  @Override
  public void setCache(Cache cache) throws IOException {
    CarrotConfig conf = cache.getCacheConfig();
    int numRanks = conf.getNumberOfPopularityRanks(cache.getName());
    // Number of ranks is the number of expiration bins
    this.ttlBins = new long[numRanks];
    
    this.binStart = conf.getExpireStartBinValue(cache.getName());
    this.multiplier = conf.getExpireBinMultiplier(cache.getName());
    sanityCheck();
    /**
     * This is the minimum expiration time supported by the Cache
     * ALl items MUST have expiration time greater or equals to 'binStart'
     * In case if item has expiration time lower than 'binStart' system MUST 
     * log warning and assign 'binMin' instead
     */
    this.ttlBins[numRanks - 1] = binStart; // in seconds, must be > 0
    
    for (int i = numRanks - 2; i >= 0; i--) {
      this.ttlBins[i] = (long) (multiplier * this.ttlBins[i + 1]);
    }
  }
  
  private void sanityCheck() {
    if (binStart <= 0) {
      LOG.error(String.format("Wrong value for  expiration bin start value {}, assigning 1", binStart));
      binStart = 1;
    }
    if (multiplier <= 1.0) {
      LOG.error(String.format("Wrong value for  expiration bin multiplier value {}, assigning 2.", multiplier));
      multiplier = 2.0;
    }
  }
  
  @Override
  public int adjustRank(int popularityRank, int groupRank, long expire) {
    if (expire <= 0) {
      return 0; // maximum
    }
    long currentTime = System.currentTimeMillis();
    long relExpireSec = (expire - currentTime) / 1000;
    if (relExpireSec < 0) {
      // Log warning
      return ttlBins.length - 1;
    }
    int newRank = ttlBins.length - 1; // set to minimum
    for (int i = 0; i < ttlBins.length; i++) {
      if (ttlBins[i] < relExpireSec) {
        newRank = i;
        break;
      }
    }
    return newRank;
  }

  @Override
  public long adjustExpirationTime(long expire) {
    long currentTime = System.currentTimeMillis();
    if (expire <= 0) {
      return ttlBins[0] * 1000 + currentTime; // maximum
    }
    long relExpireSec = (expire - currentTime) / 1000;
    if (relExpireSec < 0) {
      // Log warning
      return ttlBins[ttlBins.length - 1] * 1000 + currentTime;
    }
    long newExpire = ttlBins[ttlBins.length - 1]; // set to minimum
    for (int i = 0; i < ttlBins.length; i++) {
      if (ttlBins[i] < relExpireSec) {
        newExpire = ttlBins[i];
        break;
      }
    }
    return newExpire * 1000 + currentTime;
  } 
}
