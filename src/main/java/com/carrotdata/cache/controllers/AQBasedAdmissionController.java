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
package com.carrotdata.cache.controllers;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import com.carrotdata.cache.Cache;
import com.carrotdata.cache.util.CacheConfig;
import com.carrotdata.cache.util.Utils;

/**
 * Basic cache admission controller with AdmissionQueue For newly admitted items it always
 * directs them to the Admission queue. For evicted and re-admitted items 
 * it checks  access counter and if it greater than 0 item gets re-admitted 
 * to the Admission queue, otherwise - it is dumped. 
 * It does nothing on item access.
 * 
 * For disk - based caches (SSD)
 *
 */
public class AQBasedAdmissionController extends BaseAdmissionController 
  implements AdmissionQueueBased {
 
  protected AdmissionQueue admissionQueue;
  /* AQ current size ratio */
  protected double aqCurrentRatio;
  /* AQ minimum size ratio */
  protected double aqMinRatio;
  /* AQ maximum size ratio */
  protected double aqMaxRatio;
  /* AQ size adjustment step */
  protected double adjStep;
  
  public AQBasedAdmissionController() {
    super();
  }
  
  @Override
  public void setCache(Cache cache) throws IOException {
    super.setCache(cache);
    initAdmissionQueue();
    /* Admission Queue */
    CacheConfig config = CacheConfig.getInstance();
    String cacheName = cache.getName();
    this.aqMinRatio = config.getAdmissionQueueMinSizeRatio(cacheName);
    this.aqMaxRatio = config.getAdmissionQueueMaxSizeRatio(cacheName);
    this.aqCurrentRatio = config.getAdmissionQueueStartSizeRatio(cacheName);
    int steps = config.getThrougputControllerNumberOfAdjustmentSteps(cacheName);
    this.adjStep = (this.aqMaxRatio - this.aqMinRatio) / steps;
  }
  
  /**
   * Initialize admission queue
   * @throws IOException 
   */
  private void initAdmissionQueue() throws IOException {
    this.admissionQueue = new AdmissionQueue(this.cache);
  }
  
  /**
   * New items are always submitted to the Admission Queue
   */
  @Override
  public boolean admit(long keyPtr, int keySize, int valueSize) {
    return !this.admissionQueue.addIfAbsentRemoveIfPresent(keyPtr, keySize, valueSize);
  }
  
  /**
   * New items are always submitted to the Admission Queue
   */
  @Override
  public boolean admit(byte[] key, int off, int size, int valueSize) {
    return !this.admissionQueue.addIfAbsentRemoveIfPresent(key,  off, size, valueSize);
  }

  @Override
  public void save(OutputStream os) throws IOException {
    super.save(os);
    DataOutputStream dos = Utils.toDataOutputStream(os);
    dos.writeDouble(this.aqCurrentRatio);
    dos.writeDouble(this.aqMaxRatio);
    dos.writeDouble(this.aqMinRatio);
    dos.writeDouble(this.adjStep);
    this.admissionQueue.save(os);
  }

  @Override
  public void load(InputStream is) throws IOException {
    super.load(is);
    DataInputStream dis = Utils.toDataInputStream(is);
    this.aqCurrentRatio = dis.readDouble();
    this.aqMaxRatio = dis.readDouble();
    this.aqMinRatio = dis.readDouble();
    this.adjStep = dis.readDouble();
    this.admissionQueue.load(is);
  }

  @Override
  public AdmissionQueue getAdmissionQueue() {
    return this.admissionQueue;
  }
  
  @Override
  public void setAdmissionQueue(AdmissionQueue queue) {
    this.admissionQueue = queue;
  }

  @Override
  public boolean decreaseThroughput() { 
    if (this.aqCurrentRatio - this.adjStep < this.aqMinRatio) {
      return false;
    }
    this.aqCurrentRatio -= this.adjStep;
    this.admissionQueue.setCurrentMaxSizeRatio(this.aqCurrentRatio);
    return true;
  }

  @Override
  public boolean increaseThroughput() {
    if (this.aqCurrentRatio + this.adjStep > this.aqMaxRatio) {
      return false;
    }
    this.aqCurrentRatio += this.adjStep;
    this.admissionQueue.setCurrentMaxSizeRatio(this.aqCurrentRatio);
    return true;
  }
}