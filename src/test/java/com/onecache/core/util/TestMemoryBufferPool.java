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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class TestMemoryBufferPool {

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
    for(int i = 0; i < 100; i++) {
      long ptr = pool.poll();
      assertTrue (ptr != 0);
      UnsafeAccess.copy(data,  ptr,  bufferSize);
      forReuse.add(ptr);
    }
    Thread.sleep(100);
    assertTrue(pool.size() == maxBuffers);
    for(Long ptr: forReuse) {
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
    } catch (IllegalStateException e) {}
    
    try {
      pool.offer(0);
      fail();
    } catch (IllegalStateException e) {}
    
    try {
      pool.size();
      fail();
    } catch (IllegalStateException e) {}
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
    } catch (IllegalStateException e) {}
    
    try {
      pool.offer(0);
      fail();
    } catch (IllegalStateException e) {}
    
    try {
      pool.size();
      fail();
    } catch (IllegalStateException e) {}
    UnsafeAccess.free(data);

    long allocated = UnsafeAccess.mallocStats.allocated.get();
    long freed = UnsafeAccess.mallocStats.freed.get();
    assertEquals(allocated, freed);
  }
  
}
