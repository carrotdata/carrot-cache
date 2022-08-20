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

import java.io.IOException;

import com.carrot.cache.Cache;

/**
 * Basic (pass-through) cache admission controller 
 */
public class BaseAdmissionController implements AdmissionController {
  
  protected Cache cache;
  
  public BaseAdmissionController() {
  }
    
  @Override
  public void setCache(Cache cache) throws IOException {
    this.cache = cache;
  }
  
  /**
   * New items are always admitted
   */
  @Override
  public boolean admit(long keyPtr, int keySize, int valueSize) {
    return true;
  }
  
  /**
   * New items are always admitted
   */
  @Override
  public boolean admit(byte[] key, int off, int size, int valueSize) {
    return true;
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
  public int adjustRank(int rank, long expire /* relative - not absolute in ms*/) {
    return rank;
  }

  @Override
  public boolean decreaseThroughput() {
    return false;
  }

  @Override
  public boolean increaseThroughput() {
    return false;
  }
  
}