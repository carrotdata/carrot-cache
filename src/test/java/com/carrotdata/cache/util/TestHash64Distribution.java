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
package com.carrotdata.cache.util;

import java.util.Random;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestHash64Distribution {
  private static final Logger LOG = LoggerFactory.getLogger(TestHash64Distribution.class);

  int[] sizes = new int[] { 100000, 200000, 1000000, 2000000, 300000, 4000000, 5000000, 6000000,
      7000000, 8000000, 9000000, 10000000 };
  int[] slots = new int[] { 1024, 2048, 4096, 8192, 16384, 32768, 65536 };

  // @Test
  public void runAll() {
    for (int i = 0; i < slots.length; i++) {
      for (int j = 0; j < sizes.length; j++) {
        runTest(sizes[j], slots[i]);
      }
    }
  }

  private void runTest(int size, int indexSize) {
    int[] direct = new int[indexSize];
    int[] reverse = new int[indexSize];
    LOG.info("INDEX SIZE=" + indexSize + " NUMBER=" + size);
    Random r = new Random();

    for (int i = 0; i < size; i++) {
      byte[] key = TestUtils.randomBytes(16, r);
      long hash = Utils.hash64(key, 0, key.length);
      int slot = getSlotNumber(hash, indexSize);
      direct[slot]++;
      slot = getSlotNumberReverse(hash, indexSize);
      reverse[slot]++;
    }
    LOG.info("Direct " + analyze(direct) + " Reverse=" + analyze(reverse));

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

    for (int i = 0; i < arr.length; i++) {
      if (arr[i] < min) min = arr[i];
      if (arr[i] > max) max = arr[i];
    }
    double ratio = ((double) max) / min;
    sb.append("min=" + min).append(" max=" + max).append(" max-min ratio=" + ratio);
    return sb.toString();
  }

  private int getSlotNumber(long hash, int indexSize) {
    int level = Integer.numberOfTrailingZeros(indexSize);
    int $slot = (int) (hash >>> (64 - level));
    return $slot;
  }

  private int getSlotNumberReverse(long hash, int indexSize) {
    int level = Integer.numberOfTrailingZeros(indexSize);
    long $slot = (hash << (64 - level));
    return (int) ($slot >>> (64 - level));
  }
}