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
  AtomicLongArray atomicArray;

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

  public final long poll() {
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
    // size.decrement();
    return found;
  }

  private void checkDisabled() {
    if (disabled) {
      throw new IllegalStateException("Memory buffer pool is disabled");
    }
  }

  public final boolean offer(long ptr) {
    checkDisabled();
    ThreadLocalRandom r = ThreadLocalRandom.current();
    final int mask = this.maxCapacity - 1;
    final int maxAttempts = this.maxAttempts;
    int index = r.nextInt() & mask;
    boolean result = false;
    int attempt = 0;
    while (attempt++ < maxAttempts
        && ((result = atomicArray.compareAndSet(index, 0L, ptr)) == false)) {
      index = (index + 1) & mask;
    }
    if (!result) {
      allocator.deallocate(ptr);
    } else {
      // size.increment();
    }
    return result;
  }

  /*
   * Same as shutdown
   */
  public void dispose() {
    shutdown();
  }

  /*
   * Return number of non-zero elements
   */
  public long size() {
    checkDisabled();
    return size.longValue();
    // long size = 0;
    // for (int i = 0; i < atomicArray.length(); i++) {
    // if (atomicArray.get(i) != 0) size++;
    // }
    // return size;
  }

  /**
   * Dispose all pooled resources
   */
  public synchronized void shutdown() {
    for (int i = 0; i < atomicArray.length(); i++) {
      long ptr = atomicArray.getAndSet(i, 0);
      if (ptr != 0) {
        this.allocator.deallocate(ptr);
      }
    }
  }
}
