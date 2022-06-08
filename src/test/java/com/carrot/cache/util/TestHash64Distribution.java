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
package com.carrot.cache.util;

import java.util.Random;

import org.junit.Test;

public class TestHash64Distribution {
  int[] sizes = new int[] {100000, 200000, 1000000, 2000000, 300000, 4000000, 5000000, 6000000, 7000000, 8000000, 9000000, 10000000};
  int[] slots = new int[] {1024, 2048, 4096, 8192, 16384, 32768, 65536}; 
  
  //@Test
  public void runAll() {
    for(int i = 0; i < slots.length; i++) {
      for(int j = 0; j < sizes.length; j++) {
        runTest(sizes[j], slots[i]);
      }
    }
  }
  
  private void runTest(int size, int indexSize) {
    int[] direct = new int[indexSize];
    int[] reverse = new int[indexSize];
    System.out.println("INDEX SIZE=" + indexSize + " NUMBER="+ size) ;
    Random r = new Random();
    
    for (int i = 0; i < size; i++) {
      byte[] key = TestUtils.randomBytes(16, r);
      long hash = Utils.hash64(key, 0, key.length);
      int slot = getSlotNumber(hash, indexSize);
      direct[slot]++;
      slot = getSlotNumberReverse(hash, indexSize);
      reverse[slot]++;
    }
    System.out.println("Direct "+ analyze(direct) + " Reverse="+ analyze(reverse));

  }
  
  @Test
  public void runSingleInLoop() {
    for (int i = 0; i < 100; i++) {
      runTest(200000, 1024);
    }
  }
  
  private String analyze(int[] arr) {
    StringBuffer sb = new StringBuffer();
    int min = Integer.MAX_VALUE;
    int max = Integer.MIN_VALUE;
    
    for(int i = 0; i < arr.length; i++) {
      if (arr[i] < min) min = arr[i];
      if (arr[i] > max) max = arr[i];
    }
    double ratio = ((double) max) / min;
    sb.append("min=" + min).append(" max="+ max).append(" max-min ratio="+ ratio);
    return sb.toString();
  }

  private int getSlotNumber(long hash, int indexSize) {
    int level = Integer.numberOfTrailingZeros(indexSize);
    int $slot = (int) (hash >>> (64 - level));
    return $slot;
  }
  
  private int getSlotNumberReverse(long hash, int indexSize) {
    int level = Integer.numberOfTrailingZeros(indexSize);
    long $slot =  (hash << (64 - level));
    return (int) ($slot >>> (64 - level));
  }
}
