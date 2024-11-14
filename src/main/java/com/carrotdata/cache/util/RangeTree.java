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

import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotdata.cache.ObjectCache;

public class RangeTree {
  private static final Logger LOG = LoggerFactory.getLogger(RangeTree.class);

  public static class Range implements Comparable<Range> {
    long start;
    int size;

    Range() {
    }

    Range(long start, int size) {
      this.start = start;
      this.size = size;
    }

    @Override
    public int compareTo(Range o) {
      if (start > o.start) return 1;
      if (start < o.start) return -1;
      return 0;
    }
  }

  private TreeMap<Range, Range> map = new TreeMap<Range, Range>();

  public RangeTree() {
  }

  public synchronized Range add(Range r) {
    return map.put(r, r);
  }

  public synchronized Range delete(long address) {
    search.start = address;
    search.size = 0;
    return map.remove(search);
  }

  private Range search = new Range();

  public synchronized boolean inside(long start, int size) {
    search.start = start;
    search.size = size;
    Range r = map.floorKey(search);
    boolean result = r != null && start >= r.start && (start + size) <= r.start + r.size;
    if (!result && r != null) {
      LOG.error("Check FAILED for range [" + start + "," + size + "] Found allocation [" + r.start
          + "," + r.size + "]");
    } else if (!result) {
      LOG.error("Check FAILED for range [" + start + "," + size + "] No allocation found.");
    }
    return result;
  }

  public synchronized int size() {
    return map.size();
  }

  public synchronized Set<Map.Entry<Range, Range>> entrySet() {
    return map.entrySet();
  }
}
