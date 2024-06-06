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

package com.carrotdata.cache.util;

import java.util.concurrent.ConcurrentLinkedQueue;

public class ObjectPool<T> {

  private ConcurrentLinkedQueue<T> pool = new ConcurrentLinkedQueue<T>();

  private int maxSize;

  public ObjectPool(int size) {
    this.maxSize = size;
  }

  public boolean offer(T obj) {
    if (pool.size() >= this.maxSize) {
      return false;
    } else {
      pool.offer(obj);
    }
    return true;
  }

  public T poll() {
    return this.pool.poll();
  }

  public int getMaxSize() {
    return maxSize;
  }
}
