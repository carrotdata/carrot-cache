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
  
  public long count() {
    long sum = 0;
    for(int i = 0; i < bins.length; i++) {
      sum += bins[i];
    }
    return sum;
  }
  
  @Override
  public void save(OutputStream os) throws IOException {
    DataOutputStream dos = Utils.toDataOutputStream(os);
    dos.writeLong(startTime);
    dos.writeInt(index);
    dos.writeInt(window);
    dos.writeInt(bins.length);
    for(int i = 0; i < bins.length; i++) {
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
