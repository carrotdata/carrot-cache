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

import java.text.NumberFormat;

import org.junit.After;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class TestUltraFastMemoryBufferPool {
  private static final Logger LOG = LoggerFactory.getLogger(TestUltraFastMemoryBufferPool.class);

  ConcurrentLongPool pool;

  final int maxCapacity = 4096;
  final static int bufferSize = 4096;
  final int numThreads = 8;
  final int numIterations = 100_000_000;
  final int maxAttempts = 20;

  @After
  public void tearDown() {
    pool.dispose();
  }

  private void setMemoryAllocator() {
    pool = new UltraFastMemoryBufferPool(maxCapacity, maxAttempts, bufferSize);
  }

  private void testPollOfferSingleThread() {
    long start = System.nanoTime();
    for (int i = 0; i < numIterations; i++) {
      long v = pool.poll();
      pool.offer(v);
    }
    long end = System.nanoTime();
    LOG.info("Time for {} pool-offer is {} microsec", numIterations, (end - start) / 1000);
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
    String formatted =
        NumberFormat.getInstance().format(2L * numThreads * numIterations * 1000 / (end - start));
    LOG.info("Throughput {} op/sec size={}", formatted, pool.size());
  }
}