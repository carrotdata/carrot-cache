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
package com.carrotdata.cache.eviction;

public class LRUEvictionPolicy implements EvictionPolicy {

  public LRUEvictionPolicy() {
  }

  @Override
  public int getPromotionIndex(long cacheItemPtr, int cacheItemIndex, int totalItems) {
    // promotes always into the head
    return 0;
  }

  @Override
  public int getEvictionCandidateIndex(long idbPtr, int totalItems) {
    // evicts last one
    return totalItems - 1;
  }

  @Override
  public int getInsertIndex(long idbPtr, int totalItems) {
    // Inserts always into the head
    return 0;
  }
}
