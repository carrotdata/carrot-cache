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

public class UltraFastMemoryBufferPool extends ConcurrentLongPool {

  private static int DEFAULT_BUFFER_SIZE = 4096;

  protected int bufferSize = DEFAULT_BUFFER_SIZE;

  public UltraFastMemoryBufferPool(int maxCapacity) {
    super(maxCapacity);
    this.allocator.setInitialized();
    init();
  }

  public UltraFastMemoryBufferPool(int maxCapacity, int maxAttempts) {
    super(maxCapacity, maxAttempts);
    this.allocator.setInitialized();
    init();
  }

  public UltraFastMemoryBufferPool(int maxCapacity, int maxAttempts, int bufferSize) {
    super(maxCapacity, maxAttempts);
    this.bufferSize = bufferSize;
    this.allocator.setInitialized();
    init();
  }

  @Override
  protected Allocator getAllocator() {
    Allocator alloc = new Allocator() {

      boolean initialized = false;

      @Override
      public final long allocate() {
        return UnsafeAccess.mallocZeroed(bufferSize);
      }

      @Override
      public final void deallocate(long v) {
        UnsafeAccess.free(v);
      }

      @Override
      public boolean isInitialized() {
        return this.initialized;
      }

      @Override
      public void setInitialized() {
        this.initialized = true;
      }
    };
    return alloc;
  }

}
