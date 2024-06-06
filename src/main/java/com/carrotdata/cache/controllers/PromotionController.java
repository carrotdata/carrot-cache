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

import java.io.IOException;

import com.carrotdata.cache.Cache;
import com.carrotdata.cache.util.Persistent;

/**
 * Promotion controller decides if item from an victim cache should be admitted to a cache
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
