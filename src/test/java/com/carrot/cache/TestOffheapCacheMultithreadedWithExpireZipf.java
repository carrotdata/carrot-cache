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
package com.carrot.cache;

import java.util.Random;

import org.junit.Before;

import com.carrot.cache.controllers.ExpirationAwareAdmissionControllerMemory;

public abstract class TestOffheapCacheMultithreadedWithExpireZipf extends TestOffheapCacheMultithreadedZipf {
  
  protected int binStartValue = 1;
  
  protected double binMultiplier = 2.0;
  
  protected int numberOfBins = 10; // number of ranks
  
  protected int[] expArray = new int[] {2, 2, 2, 2, 2, 2, 2, 100, 100, 100, 100, 100};
  
  Random r;
  
  @Override
  @Before
  public void setUp() {
    super.setUp();
    this.binStartValue = 1;
    this.binMultiplier = 2.0;
    this.numberOfBins = 10;
    this.numThreads = 4;
    this.numRecords = 1000000;
    this.numIterations = 2 * this.numRecords;
    this.scavDumpBelowRatio = 0.5;
    this.minActiveRatio = 0.9;
    this.acClz = ExpirationAwareAdmissionControllerMemory.class;
    r = new Random();
  }
  
  @Override
  protected Cache.Builder withAddedConfigurations(Cache.Builder b) {
     b = b.withExpireStartBinValue(binStartValue)
         .withExpireBinMultiplier(binMultiplier)
         .withNumberOfPopularityRanks(numberOfBins);
     return b;
  }
  
  protected long getExpire(int n) {
    int index = r.nextInt(expArray.length);
    return System.currentTimeMillis() + 1000L * expArray[index];
  }
}
