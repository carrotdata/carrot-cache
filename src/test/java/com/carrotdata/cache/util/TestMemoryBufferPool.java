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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public abstract class TestMemoryBufferPool {

  int bufferSize = 10000;
  int maxBuffers = 10;
  MemoryBufferPool pool;

  @BeforeClass
  public static void setup() {
    UnsafeAccess.setMallocDebugEnabled(true);
  }

  @Before
  public void setUp() {
    pool = new MemoryBufferPool(bufferSize, maxBuffers);
  }

  @After
  public void tearDown() {
    // Does not throw exception - just silently ignore
    // if pool was shutdown already
    pool.shutdown();
  }

  @Test
  public void testSingleThread() throws InterruptedException {

    long data = TestUtils.randomMemory(bufferSize);
    // Get 100 buffers
    ArrayList<Long> forReuse = new ArrayList<Long>();
    for (int i = 0; i < 100; i++) {
      long ptr = pool.poll();
      assertTrue(ptr != 0);
      UnsafeAccess.copy(data, ptr, bufferSize);
      forReuse.add(ptr);
    }
    Thread.sleep(100);
    assertTrue(pool.size() == maxBuffers);
    for (Long ptr : forReuse) {
      boolean res = pool.offer(ptr);
      if (!res) {
        UnsafeAccess.free(ptr);
      }
    }
    assertTrue(pool.size() == maxBuffers);
    pool.shutdown();
    try {
      pool.poll();
      fail();
    } catch (IllegalStateException e) {
    }

    try {
      pool.offer(0);
      fail();
    } catch (IllegalStateException e) {
    }

    try {
      pool.size();
      fail();
    } catch (IllegalStateException e) {
    }
    UnsafeAccess.free(data);
    long allocated = UnsafeAccess.mallocStats.allocated.get();
    long freed = UnsafeAccess.mallocStats.freed.get();
    assertEquals(allocated, freed);
  }

  @Test
  public void testMultiplethreads() throws InterruptedException {

    final long data = TestUtils.randomMemory(bufferSize);
    // Get 100 buffers
    Runnable r = () -> {
      ArrayList<Long> forReuse = new ArrayList<Long>();
      for (int i = 0; i < 100; i++) {
        long ptr = pool.poll();
        assertTrue(ptr != 0);
        UnsafeAccess.copy(data, ptr, bufferSize);
        forReuse.add(ptr);
      }
      for (Long ptr : forReuse) {
        boolean res = pool.offer(ptr);
        if (!res) {
          UnsafeAccess.free(ptr);
        }
      }
    };

    Thread[] workers = new Thread[10];
    for (int i = 0; i < workers.length; i++) {
      workers[i] = new Thread(r);
      workers[i].start();
    }

    for (int i = 0; i < workers.length; i++) {
      workers[i] = new Thread(r);
      workers[i].join();
    }

    Thread.sleep(100);
    assertTrue(pool.size() >= maxBuffers);
    pool.shutdown();
    try {
      pool.poll();
      fail();
    } catch (IllegalStateException e) {
    }

    try {
      pool.offer(0);
      fail();
    } catch (IllegalStateException e) {
    }

    try {
      pool.size();
      fail();
    } catch (IllegalStateException e) {
    }
    UnsafeAccess.free(data);

    long allocated = UnsafeAccess.mallocStats.allocated.get();
    long freed = UnsafeAccess.mallocStats.freed.get();
    assertEquals(allocated, freed);
  }

}
