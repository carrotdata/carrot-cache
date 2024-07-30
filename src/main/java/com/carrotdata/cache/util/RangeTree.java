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
