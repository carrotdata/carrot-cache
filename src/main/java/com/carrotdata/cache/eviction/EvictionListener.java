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

public interface EvictionListener {
  /**
   * On eviction item from memory index
   * @param ibPtr index block address
   * @param ptr item being evicted index address
   */
  public void onEviction(long ibPtr, long ptr);

  /**
   * On item promotion if parent cache is present
   * @param ibPtr index block address
   * @param ptr item being promoted index address
   * @return true if was promoted, false - otherwise
   */
  public boolean onPromotion(long ibPtr, long ptr);
}
