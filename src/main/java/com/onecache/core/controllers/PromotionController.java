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

import java.io.IOException;

import com.onecache.core.controllers.ThroughputControllable;
import com.onecache.core.Cache;
import com.onecache.core.util.Persistent;

/**
 * Promotion controller decides if item from an victim cache should
 * be admitted to a cache
 */
public interface PromotionController extends Persistent, ThroughputControllable {
  
  public default void setCache(Cache cache) throws IOException {
  }
  
  /**
   * Returns if a victim's cache item should be promoted to the cache
   * @param keyPtr key's address
   * @param keySize item's key size
   * @param valueSize value size
   * @return true if item must be admitted to the cache, false - otherwise
   */
  public default boolean promote(long keyPtr, int keySize, int valueSize) {
    return true;
  }
  
  /**
   * Returns if a victim's cache item should be promote to the cache
   * @param key item key buffer
   * @param keyOffset item key buffer offset
   * @param keySize item's key size
   * @param valueSize value size
   * @return true if item must be admitted to the cache, false - otherwise
   */
  
  public default boolean promote(byte[] key, int keyOffset, int keySize, int valueSize) {
    return true;
  }
}
