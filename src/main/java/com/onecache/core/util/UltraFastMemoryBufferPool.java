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
package com.onecache.core.util;

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
      
      boolean initialized  = false;
      
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
