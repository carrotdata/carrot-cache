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

import java.util.Arrays;
import java.util.Random;

public class Percentile {

  private long[] bins;
  private int[] indexes;
  private int currentIndex = 0;
  private int N;
  int counter = 0;
  private Random rnd = new Random();
  private boolean sorted = false;
  private long min = Long.MAX_VALUE, max = Long.MIN_VALUE;

  public Percentile(int binCount, int max) {
    this.N = max;
    bins = new long[binCount];
    indexes = new int[binCount];
    generateIndexes();
  }

  private void generateIndexes() {
    int n = bins.length;
    int i = 0;
    while (i < n) {
      int v = rnd.nextInt(N);
      boolean found = false;
      for (int k = 0; k < i; k++) {
        if (indexes[k] == v) {
          found = true;
          break;
        }
      }
      if (!found) {
        indexes[i++] = v;
      }
    }
    Arrays.sort(indexes);
  }

  public void add(long value) {
    if (currentIndex < bins.length && counter == indexes[currentIndex]) {
      bins[currentIndex] = value;
      currentIndex++;
    }
    if (value < min) {
      min = value;
    } else if (value > max) {
      max = value;
    }
    counter++;
  }

  public long value(double percentile) {
    if (!sorted) {
      Arrays.sort(bins);
    }
    int index = (int) (percentile * bins.length);
    if (index == bins.length) {
      index--;
    }
    return bins[index];
  }

  public long min() {
    return min;
  }

  public long max() {
    return max;
  }
}
