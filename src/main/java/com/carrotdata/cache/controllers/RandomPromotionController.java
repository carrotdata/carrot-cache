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
import java.util.concurrent.ThreadLocalRandom;

import com.carrotdata.cache.Cache;
import com.carrotdata.cache.util.CacheConfig;
import com.carrotdata.cache.util.Utils;

/**
 * Simple promotion controller which tosses the coin *
 */
public class RandomPromotionController extends BasePromotionController {

  private double probability;

  @Override
  public boolean promote(long keyPtr, int keySize, int valueSize) {
    double value = ThreadLocalRandom.current().nextDouble();
    return value < probability;
  }

  @Override
  public boolean promote(byte[] key, int keyOffset, int keySize, int valueSize) {
    double value = ThreadLocalRandom.current().nextDouble();
    return value < probability;
  }

  @Override
  public void load(InputStream is) throws IOException {
    super.load(is);
    DataInputStream dis = Utils.toDataInputStream(is);
    this.probability = dis.readDouble();
  }

  @Override
  public void save(OutputStream os) throws IOException {
    super.save(os);
    DataOutputStream dos = Utils.toDataOutputStream(os);
    dos.writeDouble(probability);
  }

  @Override
  public void setCache(Cache cache) throws IOException {
    super.setCache(cache);
    String cacheName = cache.getName();
    CacheConfig conf = CacheConfig.getInstance();
    probability = conf.getRandomPromotionProbability(cacheName);
  }

}
