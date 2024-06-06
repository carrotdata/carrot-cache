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
import java.util.Random;

import com.carrotdata.cache.Cache;
import com.carrotdata.cache.util.CacheConfig;
import com.carrotdata.cache.util.Utils;

public class RandomAdmissionController extends BaseAdmissionController {

  /** Maximum threshold */
  private double startThreshold;

  /** Minimum threshold */
  private double stopThreshold;

  /** Current threshold */
  private double threshold;

  /** Adjustment step */
  private double adjStep;

  private Random r = new Random();

  @Override
  public void setCache(Cache cache) throws IOException {
    super.setCache(cache);
    CacheConfig conf = CacheConfig.getInstance();
    // init thresholds
    this.startThreshold = conf.getRandomAdmissionControllerStartRatio(cache.getName());
    this.stopThreshold = conf.getRandomAdmissionControllerStopRatio(cache.getName());
    this.threshold = this.startThreshold;
    int steps = conf.getThrougputControllerNumberOfAdjustmentSteps(cache.getName());
    this.adjStep = (this.startThreshold - this.stopThreshold) / steps;

  }

  @Override
  public boolean admit(long keyPtr, int keySize, int valueSize) {
    double v = r.nextDouble();
    if (v < this.threshold) {
      return true;
    }
    return false;
  }

  @Override
  public boolean admit(byte[] key, int off, int size, int valueSize) {
    double v = r.nextDouble();
    if (v < this.threshold) {
      return true;
    }
    return false;
  }

  @Override
  public void save(OutputStream os) throws IOException {
    super.save(os);
    DataOutputStream dos = Utils.toDataOutputStream(os);
    dos.writeDouble(this.threshold);
    dos.writeDouble(this.startThreshold);
    dos.writeDouble(this.stopThreshold);
    dos.writeDouble(this.adjStep);
  }

  @Override
  public void load(InputStream is) throws IOException {
    super.load(is);
    DataInputStream dis = Utils.toDataInputStream(is);
    this.threshold = dis.readDouble();
    this.startThreshold = dis.readDouble();
    this.stopThreshold = dis.readDouble();
    this.adjStep = dis.readDouble();
  }

  @Override
  public boolean decreaseThroughput() {
    if (threshold - adjStep < stopThreshold) {
      return false;
    }
    threshold -= adjStep;
    return true;
  }

  @Override
  public boolean increaseThroughput() {
    if (threshold + adjStep > startThreshold) {
      return false;
    }
    threshold += adjStep;
    return true;
  }
}
