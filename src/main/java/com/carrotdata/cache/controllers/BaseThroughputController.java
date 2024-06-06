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
package com.carrotdata.cache.controllers;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicLong;

import com.carrotdata.cache.Cache;
import com.carrotdata.cache.Scavenger;
import com.carrotdata.cache.util.CacheConfig;
import com.carrotdata.cache.util.Utils;

/**
 * Basic implementation of ThroughputController
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
  protected CacheConfig config;

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
    return totalBytesWritten.get() / secs;
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
    if (current >= (1.0 - toleranceLimit) * throughputGoal
        && current <= (1.0 + toleranceLimit) * throughputGoal) {
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
