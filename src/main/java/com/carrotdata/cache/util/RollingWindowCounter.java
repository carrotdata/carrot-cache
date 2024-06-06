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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class RollingWindowCounter implements Persistent {

  private long startTime;

  private int[] bins;

  private int index;

  private int window; // window duration in seconds

  public RollingWindowCounter() {

  }

  public RollingWindowCounter(int numBins, int windowSec) {
    this.window = windowSec;
    this.bins = new int[numBins];
    this.index = 0;
    this.startTime = System.currentTimeMillis();

  }

  public synchronized void increment() {
    long time = System.currentTimeMillis();
    long slot = (time - startTime) * window / (1000L * bins.length);
    slot = slot % bins.length;
    if (slot != index) {
      index = (int) slot;
      this.bins[index] = 1;
    } else {
      this.bins[index]++;
    }
  }

  /**
   * Get count
   * @return count
   */
  public long count() {
    long sum = 0;
    for (int i = 0; i < bins.length; i++) {
      sum += bins[i];
    }
    return sum;
  }

  /**
   * Get count for a given bin
   * @param bin
   * @return count for a given bin
   */
  public int binCount(int bin) {
    return bins[bin];
  }

  public int windowSize() {
    return this.window;
  }

  @Override
  public void save(OutputStream os) throws IOException {
    DataOutputStream dos = Utils.toDataOutputStream(os);
    dos.writeLong(startTime);
    dos.writeInt(index);
    dos.writeInt(window);
    dos.writeInt(bins.length);
    for (int i = 0; i < bins.length; i++) {
      dos.writeInt(bins[i]);
    }
  }

  @Override
  public void load(InputStream is) throws IOException {
    DataInputStream dis = Utils.toDataInputStream(is);
    this.startTime = dis.readLong();
    this.index = dis.readInt();
    this.window = dis.readInt();
    int n = dis.readInt();
    this.bins = new int[n];
    for (int i = 0; i < n; i++) {
      bins[i] = dis.readInt();
    }
  }
}
