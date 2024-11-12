/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
