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
package com.carrotdata.cache.util;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.LongAdder;


public abstract class ConcurrentLongPool {
  
  private static int DEFAULT_MAX_ATTEMPTS = 20;
  /**
   * Resource allocator & deallocator
   */
  public static interface Allocator {
    
    public long allocate();
    
    public void deallocate(long v);
    
    public default boolean isInitialized() {
      return true;
    }
    
    public default void setInitialized() { 
    }
  }
  
  /* 
   * Backing atomic long array 
   */
  AtomicLongArray atomicArray ;
 
  /*
   * Pool size (?)
   */
  LongAdder size = new LongAdder();
  
  /* 
   * Allocates and deallocates resources designated by long values
   */
  Allocator allocator;
  
  /*
   * Maximum capacity of the pool
   */
  final int maxCapacity;
  
  /*
   * Maximum number of attempts on write and read
   */
  final int maxAttempts;

  /*
   * Pool is disabled
   */
  volatile boolean disabled = false;
  
  public ConcurrentLongPool(int maxCapacity) {
    this(maxCapacity, DEFAULT_MAX_ATTEMPTS);
  }

  public ConcurrentLongPool(int maxCapacity, int maxAttempts) {
    this.maxCapacity = (int) Utils.nextPow2(maxCapacity);
    this.maxAttempts = maxAttempts;
    this.allocator = getAllocator();
    init();
  }
  
  protected abstract Allocator getAllocator();
    
  protected void init() {
    if (!this.allocator.isInitialized()) {
      return;
    }
    atomicArray = new AtomicLongArray(this.maxCapacity);
    for (int i = 0; i < this.maxCapacity; i++) {
      long ptr = this.allocator.allocate();
      atomicArray.set(i, ptr);
    }
    size.add(this.maxCapacity);
  }
  
  public final long poll () {
    checkDisabled();
    ThreadLocalRandom r = ThreadLocalRandom.current();
    final int mask = this.maxCapacity - 1;
    final int maxAttempts = this.maxAttempts;
    int index = r.nextInt() & mask;
    long found = 0; // missing pointer
    int attempt = 0;
    while (attempt++ < maxAttempts && (found = atomicArray.getAndSet(index, 0L)) == 0) {
      index = (index + 1) & mask;
    }
    if (found == 0) {
      return this.allocator.allocate();
    }
    //size.decrement();
    return found;
  }
  
  private void checkDisabled() {
    if (disabled) {
      throw new IllegalStateException("Memory buffer pool is disabled");
    }
  }

  public final boolean offer (long ptr) {
    checkDisabled();
    ThreadLocalRandom r = ThreadLocalRandom.current();
    final int mask = this.maxCapacity - 1;
    final int maxAttempts = this.maxAttempts;
    int index = r.nextInt() & mask;
    boolean result = false;
    int attempt = 0;
    while (attempt++ < maxAttempts && ((result = atomicArray.compareAndSet(index, 0L, ptr)) == false)) {
      index = (index + 1) & mask;
    }
    if (!result) {
      allocator.deallocate(ptr);
    } else {
      //size.increment();
    }
    return result;
  }
  
  /*
   * Same as shutdown
   */
  public void dispose()  {
    shutdown();
  }
  
  /*
   * Return number of non-zero elements
   */
  public long size() {
    checkDisabled();
    return size.longValue();
//    long size = 0;
//    for (int i = 0; i < atomicArray.length(); i++) {
//      if (atomicArray.get(i) != 0) size++;
//    }
//    return size;
  }
  
  /**
   * Dispose all pooled resources
   */
  public synchronized void shutdown() {
    for(int i = 0; i < atomicArray.length(); i++) {
      long ptr = atomicArray.getAndSet(i, 0); 
      if (ptr != 0) {
        this.allocator.deallocate(ptr);
      }
    }
  }
}
