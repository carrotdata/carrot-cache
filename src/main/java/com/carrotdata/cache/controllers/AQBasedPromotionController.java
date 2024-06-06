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

import com.carrotdata.cache.Cache;
import com.carrotdata.cache.util.CacheConfig;
import com.carrotdata.cache.util.Utils;

/**
 * Basic cache promotion controller with AdmissionQueue for admitted items it always directs them to
 * the Admission (Promotion) queue.
 */
public class AQBasedPromotionController extends BasePromotionController
    implements AdmissionQueueBased {

  protected PromotionQueue promotionQueue;
  /* AQ current size ratio */
  protected double aqCurrentRatio;
  /* AQ minimum size ratio */
  protected double aqMinRatio;
  /* AQ maximum size ratio */
  protected double aqMaxRatio;
  /* AQ size adjustment step */
  protected double adjStep;

  public AQBasedPromotionController() {
    super();
  }

  @Override
  public void setCache(Cache cache) throws IOException {
    super.setCache(cache);
    initPromotionQueue();
    /* Admission Queue */
    CacheConfig config = CacheConfig.getInstance();
    String cacheName = cache.getName();
    this.aqMinRatio = config.getPromotionQueueMinSizeRatio(cacheName);
    this.aqMaxRatio = config.getPromotionQueueMaxSizeRatio(cacheName);
    this.aqCurrentRatio = config.getPromotionQueueStartSizeRatio(cacheName);
    int steps = config.getThrougputControllerNumberOfAdjustmentSteps(cacheName);
    this.adjStep = (this.aqMaxRatio - this.aqMinRatio) / steps;
  }

  /**
   * Initialize admission queue
   * @throws IOException
   */
  private void initPromotionQueue() throws IOException {
    this.promotionQueue = new PromotionQueue(this.cache);
  }

  /**
   * New items are always submitted to the Admission Queue
   */
  @Override
  public boolean promote(long keyPtr, int keySize, int valueSize) {
    return !this.promotionQueue.addIfAbsentRemoveIfPresent(keyPtr, keySize, valueSize);
  }

  /**
   * New items are always submitted to the Admission Queue
   */
  @Override
  public boolean promote(byte[] key, int off, int size, int valueSize) {
    return !this.promotionQueue.addIfAbsentRemoveIfPresent(key, off, size, valueSize);
  }

  @Override
  public void save(OutputStream os) throws IOException {
    super.save(os);
    DataOutputStream dos = Utils.toDataOutputStream(os);
    dos.writeDouble(this.aqCurrentRatio);
    dos.writeDouble(this.aqMaxRatio);
    dos.writeDouble(this.aqMinRatio);
    dos.writeDouble(this.adjStep);
    this.promotionQueue.save(os);
  }

  @Override
  public void load(InputStream is) throws IOException {
    super.load(is);
    DataInputStream dis = Utils.toDataInputStream(is);
    this.aqCurrentRatio = dis.readDouble();
    this.aqMaxRatio = dis.readDouble();
    this.aqMinRatio = dis.readDouble();
    this.adjStep = dis.readDouble();
    this.promotionQueue.load(is);
  }

  @Override
  public AdmissionQueue getAdmissionQueue() {
    return this.promotionQueue;
  }

  @Override
  public void setAdmissionQueue(AdmissionQueue queue) {
    this.promotionQueue = (PromotionQueue) queue;
  }

  @Override
  public boolean decreaseThroughput() {
    if (this.aqCurrentRatio - this.adjStep < this.aqMinRatio) {
      return false;
    }
    this.aqCurrentRatio -= this.adjStep;
    this.promotionQueue.setCurrentMaxSizeRatio(this.aqCurrentRatio);
    return true;
  }

  @Override
  public boolean increaseThroughput() {
    if (this.aqCurrentRatio + this.adjStep > this.aqMaxRatio) {
      return false;
    }
    this.aqCurrentRatio += this.adjStep;
    this.promotionQueue.setCurrentMaxSizeRatio(this.aqCurrentRatio);
    return true;
  }
}
