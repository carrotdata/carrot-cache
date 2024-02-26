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

package com.onecache.core.controllers;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Random;

import com.onecache.core.Cache;
import com.onecache.core.util.CacheConfig;
import com.onecache.core.util.Utils;

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
