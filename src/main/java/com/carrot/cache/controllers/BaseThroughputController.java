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
import java.util.concurrent.atomic.AtomicLong;

import com.carrot.cache.Cache;
import com.carrot.cache.Scavenger;
import com.carrot.cache.util.CarrotConfig;
import com.carrot.cache.util.Utils;

/**
 * Basic implementation of ThroughputController
 *
 */
public abstract class BaseThroughputController implements ThroughputController {
  
  /* Controller's start time */
  protected long startTime;
  
  /* Total bytes written */
  protected AtomicLong totalBytesWritten;
  
  /* Throughput goal */
  protected long throughputGoal;
  
  /* Tolerance limit */
  protected double toleranceLimit;
  
  /* Cache configuration */
  protected CarrotConfig config;
  
  /* Cache name */
  protected String cacheName;
  
  /* Parent cache */
  protected Cache cache;
  
  /* Admission controller */
  protected AdmissionController admissionController;
    
  /**
   * Constructor
   */
  public BaseThroughputController(Cache parent) {
    setCache(cache);
  }
  
  public BaseThroughputController() {
  }

  public void setCache(Cache parent) {
    this.cache = parent;
    this.cacheName = parent.getName();
    this.startTime = System.currentTimeMillis();
    this.config = parent.getCacheConfig();
    this.throughputGoal = this.config.getCacheWriteLimit(this.cacheName);
    this.toleranceLimit = this.config.getThroughputToleranceLimit(this.cacheName);
    this.admissionController = parent.getAdmissionController();
  }
  
  @Override
  public void save(OutputStream os) throws IOException {
    DataOutputStream dos = Utils.toDataOutputStream(os);
    dos.writeLong(this.startTime);
    dos.writeLong(this.totalBytesWritten.get());
    dos.writeLong(this.throughputGoal);
  }

  @Override
  public void load(InputStream is) throws IOException {
    DataInputStream dis = Utils.toDataInputStream(is);
    this.startTime = dis.readLong();
    this.totalBytesWritten.set(dis.readLong());
    this.throughputGoal = dis.readLong();
    
  }

  @Override
  public void record(long bytes) {
    this.totalBytesWritten.addAndGet(bytes);
  }

  @Override
  public long getStartTime() {
    return this.startTime;
  }

  @Override
  public long getThroughputGoal() {
    return this.throughputGoal;
  }

  @Override
  public long getCurrentThroughput() {
    long secs = (System.currentTimeMillis() - this.startTime) / 1000;
    return totalBytesWritten.get()/ secs;
  }

  @Override
  public long getTotalBytesWritten() {
    return this.totalBytesWritten.get();
  }

  @Override
  public void setThrougputGoal(long goal) {
    this.throughputGoal = goal;
  }
  
  @Override
  public boolean adjustParameters() {
    long current = getCurrentThroughput();
    if (current >= (1.0 - toleranceLimit) * throughputGoal && 
        current <= (1.0 + toleranceLimit) * throughputGoal) {
      return false;
    } else if (current < (1.0 - toleranceLimit) * throughputGoal) {
      return increaseThroughput();
    } else {
      return decreaseThroughput();
    }    
  }

  protected boolean decreaseThroughput() {
    // Check scavenger first
    if (Scavenger.decreaseThroughput(this.cacheName)) { 
      return true; 
    } else {
      // Then admission controller
      return this.admissionController.decreaseThroughput();
    }
  };

  protected boolean increaseThroughput() {
    // Check admission controller first
    if (this.admissionController.increaseThroughput()) {
      return true;
    } else {
      return Scavenger.increaseThroughput(this.cacheName);
    }
  }
}
