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

import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class MemoryBufferPool {
  /** Logger */
  private static final Logger LOG = LoggerFactory.getLogger(MemoryBufferPool.class);
  
  ConcurrentLinkedQueue<Long> memoryBuffers = new ConcurrentLinkedQueue<Long>();
  /*
   * Memory buffer size in bytes
   */
  int bufferSize;
  /*
   * Maximum capacity of the cache
   */
  int maxCapacity;
  
  // We have only 1 thread here. We need more testing to justify anything large than 1. 
  // page fault in OS kernel even minor can take more than 2000 cycles (~1 microsec), so 
  // maximum memory initialization throughput is limited by 4K * 1_000_000 - 4GB/s
  // We need more testing on different platforms to decide if we need more threads in the pool
  ThreadPoolExecutor executor = (ThreadPoolExecutor) Executors.newFixedThreadPool(1, new ThreadFactory() {
    AtomicInteger id = new AtomicInteger(0);
    @Override
    public Thread newThread(Runnable r) {
      return new Thread(r, "occ-memory-buffer-cache-"+ id.incrementAndGet());
    }
  });
  /*
   * Pool is disabled
   */
  volatile boolean disabled = false;
  
  public static AtomicLong allocated = new AtomicLong();
  
  public static AtomicLong allocations = new AtomicLong();
  
  public MemoryBufferPool(int bufferSize, int maxCapacity) {
    this.bufferSize = bufferSize;
    this.maxCapacity = maxCapacity;
    init();
  }

  private void init() {
    long t1 = System.currentTimeMillis();
    Future<?> result = executor.submit(() -> addBuffers());
    try {
      result.get();
      long t2 = System.currentTimeMillis();
      LOG.debug("Initialized {} memory buffers of size {} in {}ms", maxCapacity, bufferSize, t2 - t1);
    } catch (Exception e) {
      LOG.warn("Failed", e);
    }
  }
  
  private void addBuffers() {

    while(memoryBuffers.size() < maxCapacity) {
      if (Thread.interrupted()) {
        return;
      }
      long ptr = UnsafeAccess.mallocZeroed(bufferSize);
      memoryBuffers.add(ptr);
      allocated.addAndGet(bufferSize);
      allocations.incrementAndGet();
    }
  }
  
  public long poll () {
    checkDisabled();
    Long ptr = null;
    if (memoryBuffers.size() < maxCapacity) {
      if (executor.getActiveCount() == 0) {
        executor.submit(() -> addBuffers());
      }
    }
    ptr = memoryBuffers.poll();
    if (ptr  == null) {
      // do not initialize
      // TODO: Dirty segments can be potential hazard
      // Check if DataWriter can accidentally read old meta data
      //FIXME: this is safety precaution. 
      ptr = UnsafeAccess.mallocZeroed(this.bufferSize);
    }
    return ptr;
  }
  
  private void checkDisabled() {
    if (disabled) {
      throw new IllegalStateException("Memory buffer pool is disabled");
    }
  }

  public boolean offer (long ptr) {
    checkDisabled();
    if (memoryBuffers.size() >= maxCapacity) {
      return false;
    } else {
      // Clear buffer
      UnsafeAccess.setMemory(ptr, bufferSize, (byte)0);
      this.memoryBuffers.add(ptr);
      return true;
    }
  }
  
  public void dispose()  {
    shutdown();
  }
  
  public int size() {
    checkDisabled();
    return this.memoryBuffers.size();
  }
  
  public synchronized void shutdown() {
    if (disabled) {
      return;
    }
    disabled = true;
    executor.shutdownNow();
    try {
      executor.awaitTermination(10, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      
    }
    // clean up queue
    Long ptr = null; 
    while((ptr = memoryBuffers.poll()) != null) {
      UnsafeAccess.free(ptr);
    }
  }
}
