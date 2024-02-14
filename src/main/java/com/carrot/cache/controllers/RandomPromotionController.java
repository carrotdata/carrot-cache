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
import java.util.concurrent.ThreadLocalRandom;

import com.carrot.cache.Cache;
import com.carrot.cache.util.CarrotConfig;
import com.carrot.cache.util.Utils;

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
    CarrotConfig conf = CarrotConfig.getInstance();
    probability = conf.getPromotionProbability(cacheName);
  }
  
  
}
