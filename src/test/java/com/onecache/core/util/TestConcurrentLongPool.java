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

import java.text.NumberFormat;
import java.util.concurrent.ThreadLocalRandom;

import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class TestConcurrentLongPool {
  private static final Logger LOG = LoggerFactory.getLogger(TestConcurrentLongPool.class);

  static class ConcurrentRandomLongPool extends ConcurrentLongPool {

    public ConcurrentRandomLongPool(int maxCapacity) {
      super(maxCapacity);
    }

    public ConcurrentRandomLongPool(int maxCapacity, int maxAttempts) {
      super(maxCapacity, maxAttempts);
    }
    
    @Override
    protected Allocator getAllocator() {
      Allocator alloc = new Allocator() {
        @Override
        public final long allocate() {
          return ThreadLocalRandom.current().nextLong();
        }
        @Override
        public final void deallocate(long v) {
        }      
      };
      return alloc;
    }
  }
  
  static class ConcurrentMemoryLongPool extends ConcurrentLongPool {

    public ConcurrentMemoryLongPool(int maxCapacity) {
      super(maxCapacity);
    }

    public ConcurrentMemoryLongPool(int maxCapacity, int maxAttempts) {
      super(maxCapacity, maxAttempts);
    }
    
    @Override
    protected Allocator getAllocator() {
      Allocator alloc = new Allocator() {
        @Override
        public final long allocate() {
          return UnsafeAccess.mallocZeroed(bufferSize);
        }
        @Override
        public final void deallocate(long v) {
          UnsafeAccess.free(v);
        }      
      };
      return alloc;
    }
  }
  
  
  ConcurrentLongPool pool;
  final int maxCapacity = 4096;
  final static int bufferSize = 4096;
  final int numThreads = 4;
  final int numIterations = 100_000_000;
  final int maxAttempts = 20;
  
  @After
  public void tearDown() {
    pool.dispose();
  }
  
  private void setRandomNumberAllocator() {
    pool = new ConcurrentRandomLongPool(maxCapacity, maxAttempts);
  }
  
  private void setMemoryAllocator() {
    pool = new ConcurrentMemoryLongPool(maxCapacity, maxAttempts);
  }
  
  private void testPollOfferSingleThread() {
    long start = System.nanoTime();
    for (int i = 0; i < numIterations; i++) {
      long v = pool.poll();
      pool.offer(v);
    }
    long end = System.nanoTime();
    LOG.info("Time for {} pool-offer is {} microsec", numIterations,  (end - start)/ 1000);
  }
  
  @Test
  public void testPollOfferRandomAllocatorMultithreaded() throws InterruptedException {
    setRandomNumberAllocator();
    
    Runnable r = () -> testPollOfferSingleThread();
    long start = System.currentTimeMillis();
    Thread[] workers = new Thread[numThreads];
    for (int i = 0; i < numThreads; i++) {
      workers[i] = new Thread(r);
      workers[i].start();
    }
    for (int i = 0; i < numThreads; i++) {
      workers[i].join();
    }
    long end = System.currentTimeMillis();
    String formatted = NumberFormat.getInstance().format(2L * numThreads * numIterations * 1000 / (end - start));
    LOG.info("Throughput {} op/sec size={}", formatted, pool.size());
  }
  
  @Test
  public void testPollOfferMemoryAllocatorMultithreaded() throws InterruptedException {
    setMemoryAllocator();
    
    Runnable r = () -> testPollOfferSingleThread();
    long start = System.currentTimeMillis();
    Thread[] workers = new Thread[numThreads];
    for (int i = 0; i < numThreads; i++) {
      workers[i] = new Thread(r);
      workers[i].start();
    }
    for (int i = 0; i < numThreads; i++) {
      workers[i].join();
    }
    long end = System.currentTimeMillis();
    String formatted = NumberFormat.getInstance().format(2L * numThreads * numIterations * 1000 / (end - start));
    LOG.info("Throughput {} op/sec size={}", formatted, pool.size());
  }
}
