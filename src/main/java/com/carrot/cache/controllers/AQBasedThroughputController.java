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
import com.carrot.cache.util.CacheConfig;
import com.carrot.cache.util.Utils;

/**
 * Basic implementation of ThroughputController
 *
 */
public class AQBasedThroughputController implements ThroughputController {
  
  /* Controller's start time */
  protected long startTime;
  
  /* Total bytes written */
  protected AtomicLong totalBytesWritten;
  
  /* Throughput goal */
  protected long throughputGoal;
  
  /* Tolerance limit */
  protected double toleranceLimit;
  
  /* Cache configuration */
  protected CacheConfig config;
  
  /* Cache name */
  private String cacheName;
  
  /* Parent cache */
  protected Cache cache;
  
  /* Admission Queue */
  
  /* AQ current size */
  protected long aqCurrent;
  
  /* AQ minimum size */
  protected long aqMin;
  
  /* AQ maximum size */
  protected long aqMax;
  
  /* Scavenger */
  
  /* Minimum scavenger discard ratio  */
  protected double scMinRatio;
  
  /* Maximum scavenger discard ratio */
  protected double scMaxRatio;
  
  /* Current scavenger discard ratio */
  protected double scCurrentRatio;
  
  /* AQ adjustment step index */
  protected int aqAdjustStepIndex;
 
  /* Scavenger adjustment step index */
  protected int scavengerAdjustStepIndex;
  
  /* Number of adjustment steps */
  protected int numberAdustSteps;
  
  /**
   * Constructor
   */
  public AQBasedThroughputController(Cache parent) {
    setCache(cache);
  }
  
  public AQBasedThroughputController() {
  }

  public void setCache(Cache parent) {
    this.cache = parent;
    this.cacheName = parent.getName();
    this.startTime = System.currentTimeMillis();
    this.config = parent.getCacheConfig();
    this.throughputGoal = this.config.getCacheWriteLimit(this.cacheName);
    this.toleranceLimit = this.config.getThroughputToleranceLimit(this.cacheName);
    /* Admission Queue */
    this.aqMin = this.config.getAdmissionQueueMinSize(this.cacheName);
    this.aqMax = this.config.getAdmissionQueueMaxSize(this.cacheName);
    this.aqCurrent = this.config.getAdmissionQueueMaxSize(this.cacheName);
    /* Scavenger */
    this.scMinRatio = this.config.getScavengerDumpEntryBelowStart(this.cacheName);
    this.scMaxRatio = this.config.getScavengerDumpEntryBelowStop(this.cacheName);
    this.scCurrentRatio = this.scMinRatio;
    
    this.numberAdustSteps = this.config.getThrougputControllerNumberOfAdjustmentSteps(this.cacheName);
    this.scavengerAdjustStepIndex = this.numberAdustSteps;
  }
  
  @Override
  public void save(OutputStream os) throws IOException {
    DataOutputStream dos = Utils.toDataOutputStream(os);
    dos.writeLong(this.startTime);
    dos.writeLong(this.totalBytesWritten.get());
    dos.writeLong(this.throughputGoal);
    /* AQ current size */
    dos.writeLong(this.aqCurrent);
    /* AQ minimum size */
    dos.writeLong(this.aqMin);
    /* AQ maximum size */
    dos.writeLong(this.aqMax);
    
    /* Scavenger */
    /* Minimum scavenger discard ratio  */
    dos.writeLong(Double.doubleToLongBits(this.scMinRatio));
    /* Maximum scavenger discard ratio */
    dos.writeLong(Double.doubleToLongBits(this.scMaxRatio));
    /* Current scavenger discard ratio */
    dos.writeLong(Double.doubleToLongBits(this.scCurrentRatio));
    /* AQ adjustment step index */
    dos.writeInt(this.aqAdjustStepIndex);
    /* Scavenger adjustment step index */
    dos.writeInt(this.scavengerAdjustStepIndex);
    /* Number of adjustment steps */
    dos.writeInt(this.numberAdustSteps);
  }

  @Override
  public void load(InputStream is) throws IOException {
    DataInputStream dis = Utils.toDataInputStream(is);
    this.startTime = dis.readLong();
    this.totalBytesWritten.set(dis.readLong());
    this.throughputGoal = dis.readLong();
    
    /* AQ current size */
    this.aqCurrent = dis.readLong();
    /* AQ minimum size */
    this.aqMin = dis.readLong();
    /* AQ maximum size */
    this.aqMax = dis.readLong();
    
    /* Scavenger */
    /* Minimum scavenger discard ratio  */
    this.scMinRatio = Double.longBitsToDouble(dis.readLong());
    /* Maximum scavenger discard ratio */
    this.scMaxRatio = Double.longBitsToDouble(dis.readLong());
    /* Current scavenger discard ratio */
    this.scCurrentRatio = Double.longBitsToDouble(dis.readLong());
    /* AQ adjustment step index */
    this.aqAdjustStepIndex = dis.readInt();
    /* Scavenger adjustment step index */
    this.scavengerAdjustStepIndex = dis.readInt();
    /* Number of adjustment steps */
    this.numberAdustSteps = dis.readInt();
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
  public long getAdmissionQueueSize() {
    return this.aqCurrent;
  }

  @Override
  public double getCompactionDiscardThreshold() {
    return this.scCurrentRatio;
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

  private boolean decreaseThroughput() {
    if (this.scavengerAdjustStepIndex > 0) {
      this.scavengerAdjustStepIndex--;
      this.scCurrentRatio = this.scMaxRatio - 
          (this.numberAdustSteps - this.scavengerAdjustStepIndex) * (this.scMaxRatio - this.scMinRatio) /
           this.numberAdustSteps;
      return true;
    } else if (this.aqAdjustStepIndex > 0) {
      this.aqAdjustStepIndex--;
      this.aqCurrent =  this.aqAdjustStepIndex * (this.aqMax - this.aqMin) / this.numberAdustSteps + this.aqMin;
      AdmissionQueueBased controller = 
          (AdmissionQueueBased) this.cache.getAdmissionController();
      AdmissionQueue queue = controller.getAdmissionQueue();
      queue.setMaximumSize(this.aqCurrent);
      return true;
    } 
    return false;  
  }

  private boolean increaseThroughput() {
    if (this.aqAdjustStepIndex < this.numberAdustSteps) {
      this.aqAdjustStepIndex++;
      this.aqCurrent =  this.aqAdjustStepIndex * (this.aqMax - this.aqMin) / this.numberAdustSteps + this.aqMin;
      AdmissionQueueBased controller = 
          (AdmissionQueueBased) this.cache.getAdmissionController();
      AdmissionQueue queue = controller.getAdmissionQueue();
      queue.setMaximumSize(this.aqCurrent);
      return true;
    } else if (this.scavengerAdjustStepIndex < this.numberAdustSteps) {
      this.scavengerAdjustStepIndex++;
      this.scCurrentRatio = this.scMaxRatio - 
          (this.numberAdustSteps - this.scavengerAdjustStepIndex) * (this.scMaxRatio - this.scMinRatio) /
           this.numberAdustSteps;
      return true;
    }
    return false;
  }
}
