/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
