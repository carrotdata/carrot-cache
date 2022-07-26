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
package com.carrot.cache.controllers;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicLongArray;

import com.carrot.cache.Cache;
import com.carrot.cache.io.FileIOEngine;
import com.carrot.cache.util.Utils;

/**
 * Basic cache admission controller. For newly admitted items it always
 * directs them to the Admission queue. For evicted and re-admitted items 
 * it checks  access counter and if it greater than 0 item gets re-admitted 
 * to the Admission queue, otherwise - it is dumped. 
 * It does nothing on item access.
 *
 */
public class BaseAdmissionController implements AdmissionController {
  
  @SuppressWarnings("unused")
  private Cache cache;
  private boolean fileCache = false;
  
  /* Section for keeping data to adjust rank according item TTL */
  
  private long[] ttlCounts;
  private long[] cumTtl;
  
  private AtomicLongArray avgTTL;
  
  public BaseAdmissionController() {
    
  }
  
  /**
   * Constructor
   * @param cache
   */
  public BaseAdmissionController(Cache cache) {
    this.cache = cache;
    int ranks = cache.getCacheConfig().getNumberOfAdmissionRanks(cache.getName());
    this.ttlCounts= new long[ranks];
    this.cumTtl = new long[ranks];
    this.avgTTL = new AtomicLongArray(ranks);
    this.fileCache = cache.getEngine() instanceof FileIOEngine;
  }
  
  @Override
  public void setCache(Cache cache) {
    this.cache = cache;
    int ranks = cache.getCacheConfig().getNumberOfAdmissionRanks(cache.getName());
    this.ttlCounts= new long[ranks];
    this.cumTtl = new long[ranks];
    this.avgTTL = new AtomicLongArray(ranks);
    this.fileCache = cache.getEngine() instanceof FileIOEngine;
  }
  /**
   * New items are always submitted to the Admission Queue
   */
  @Override
  public Type admit(long keyPtr, int keySize) {
    return this.fileCache? Type.AQ: Type.MQ;
  }
  
  /**
   * New items are always submitted to the Admission Queue
   */
  @Override
  public Type admit(byte[] key, int off, int size) {
    return this.fileCache? Type.AQ: Type.MQ;
  }

  @Override
  public void access(byte[] key, int off, int size) {
    // nothing yet
  }
  
  @Override
  public void access(long keyPtr, int keySize) {
    // nothing yet
  }

  @Override
  public void save(OutputStream os) throws IOException {
    // save TTL stuff
    DataOutputStream dos= Utils.toDataOutputStream(os);
    int ranks = this.ttlCounts.length;
    dos.writeInt(ranks);
    for (int i = 0; i < ranks; i++) {
      dos.writeLong(this.ttlCounts[i]);
    }
    for (int i = 0; i < ranks; i++) {
      dos.writeLong(this.cumTtl[i]);
    }
  }

  @Override
  public void load(InputStream is) throws IOException {
    DataInputStream dis = Utils.toDataInputStream(is);
    int ranks = dis.readInt();
    this.ttlCounts = new long[ranks];
    this.cumTtl = new long[ranks];
    for(int i = 0; i < ranks; i++) {
      this.ttlCounts[i] = dis.readLong();
    }
    for(int i = 0; i < ranks; i++) {
      this.cumTtl[i] = dis.readLong();
    }
    this.avgTTL = new AtomicLongArray(ranks);
    
  }

  @Override
  public int adjustRank(int rank, long expire /* relative - not absolute in ms*/) {
    int ranks = this.ttlCounts.length;
    int minRank = rank;
    long minPositive = this.avgTTL.get(rank -1) - expire;
    if (minPositive <= 0) return minRank;

    for (int i = rank; i < ranks; i++) {
      long expDiff = this.avgTTL.get(i) - expire;
      if (expDiff <= 0) {
        return minRank;
      } else if (expDiff < minPositive) {
        minPositive = expDiff;
        minRank = i + 1;
      }
    }
    return minRank;
  }

  @Override
  public synchronized void registerSegmentTTL(int rank, long ttl) {
    // rank is 1 - based
    this.ttlCounts[rank - 1] ++;
    this.cumTtl[rank - 1] += ttl;
    this.avgTTL.set(rank - 1, this.cumTtl[rank - 1] / this.cumTtl[rank - 1]);
  }
}