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

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;

public abstract class TestLong2LongHashMap {
  private static final Logger LOG = LoggerFactory.getLogger(TestLong2LongHashMap.class);

  static ThreadLocal<long[]> keysTLS = new ThreadLocal<long[]>();
  
  Long2LongHashMap map;
  int numThreads = 8;
  int numIteration = 16 << 20;
  

  @Test
  public void testPutGetWithNoHashedKeys() throws InterruptedException {
    LOG.info("Test put-get no hashed keys, delay=0");
    testPutGet(false, 0);
    LOG.info("Test put-get no hashed keys, delay=200ms");
    testPutGet(false, 200);

  }
  
  @Test
  public void testPutGetWithHashedKeys() throws InterruptedException {
    LOG.info("Test put-get with hashed keys, delay=0");
    testPutGet(true, 0);
    LOG.info("Test put-get no hashed keys, delay=200ms");
    testPutGet(true, 200);
  }
  
  @Test
  public void testPutDeleteAllGet() throws InterruptedException {
    LOG.info("Test put-delete all-get, delay=0");
    testPutDeleteGet(numIteration, 0);
    LOG.info("Test put-delete all-get, delay=200");
    testPutDeleteGet(numIteration, 200);
  }
  
  @Test
  public void testPutDeletePartialGet() throws InterruptedException {
    LOG.info("Test put-delete partial-get, delay=0");
    testPutDeleteGet(numIteration / 2, 0);
    LOG.info("Test put-delete partial-get, delay=200ms");
    testPutDeleteGet(numIteration / 2, 200);
  }
    
  @Test
  public void testMultiPutDeleteAllGet() throws InterruptedException {
      LOG.info("Test multi-put-delete all-get, delay=200ms");
      testMultiPutDeleteGet(numIteration, 300);
  }
  
  private void testPutDeleteGet(int toDelete, long delay) throws InterruptedException {
    Runnable r = () -> {
      loadData();
      deleteData(toDelete);
      verifyData(toDelete);
    };
    
    runMultithreaded(r, true, delay);
  }
  
  private void testMultiPutDeleteGet(int toDelete, long delay) throws InterruptedException {
    Runnable r = () -> {     
      for (int i = 0; i < 1; i++) {
        LOG.info("******************{} delay={} RUN={}*******************\n", 
          Thread.currentThread().getName(), delay, i + 1);
        loadData();
        deleteData(toDelete);
        verifyData(toDelete);
        modifyData();
      }
    };
    runMultithreaded(r, true, delay);
  }
  
  private void modifyData() {
    long start = System.currentTimeMillis();
    long[] data = keysTLS.get();
    for (int i = 0; i < data.length; i++) {
      data[i] = Utils.squirrel3(data[i]);
    }
    long end = System.currentTimeMillis();
    LOG.info("{} modfied {} keys in {}ms", 
      Thread.currentThread().getName(), data.length, end - start);
  }

  private void loadData() {
    ThreadLocalRandom rnd = ThreadLocalRandom.current();
    final long id = Thread.currentThread().getId();
    int count = 0;
    long[] keys = keysTLS.get();
    if (keys == null) {
      keys = new long[numIteration];
      for (int i = 0; i < numIteration; i++) {
        keys[i] = rnd.nextLong();
      }
      keysTLS.set(keys);
    }

    int duplicates = 0;
    long t1 = System.currentTimeMillis();
    for (; count < numIteration; count++) {

      long key = keys[count];
      long oldValue = map.put(key, key + id);
      if (oldValue != 0) {
        duplicates++;
      }
    }

    long t2 = System.currentTimeMillis();
    LOG.info("{} loaded {} key-values in {}ms duplicates={}",
      Thread.currentThread().getName(), numIteration, t2 - t1, duplicates);
  }
  
  private void verifyData(int deleted) {
    long[] keys = keysTLS.get();
    int n = keys.length;
    final long id = Thread.currentThread().getId();
    boolean firstFailed = true;
    int lastFailed = 0;
    long t1 = System.currentTimeMillis();
    int failed = 0;
    int failedValue = 0;
    for (int count = 0; count < n; count++) {

      long key = keys[count];
      long value = map.get(key);
      if (count < deleted) {
        if (value != 0) {
          LOG.error("verify failed key={} value={} i={}", key, value, count);
          failed++;
        }
      } else if (value == 0 || value != key + id) {
        if (firstFailed) {
          LOG.info("First failed {}", count);
          firstFailed = false;
        }
        lastFailed=count;

        failed++;
        if (value != 0) {
          failedValue++;
        }
      }
      
    }
    long t2 = System.currentTimeMillis();
    LOG.info("{} verified {} key-values in {}ms failed={} failed value={} lastFailed={}",
      Thread.currentThread().getName(), n, t2 - t1, failed, failedValue, lastFailed);
    assertTrue(failed == 0);
  }
  
  private void deleteData(int toDelete) {
    long[] keys = keysTLS.get();

    long t1 = System.currentTimeMillis();
    int failed = 0;
    for (int count = 0; count < toDelete; count++) {

      long key = keys[count];
      long value = map.delete(key);
      //assertTrue (value != 0);
      if (value == 0) {
        map.trace = true;
        LOG.error("delete failed key={} i={}", key, count);
        failed ++;
      }
    }
    long t2 = System.currentTimeMillis();
    LOG.info("{} deleted {} key-values in {}ms failed={}",
      Thread.currentThread().getName(), toDelete, t2 - t1, failed);
    assertTrue( failed == 0);
  }
  
  private void testPutGet(boolean hashedKeys, long delay) throws InterruptedException {
    Runnable r = () -> {
      loadData();
      verifyData(0);
    };
    runMultithreaded(r, hashedKeys, delay);
  }
  
  private void runMultithreaded(Runnable r, boolean hashedKeys, long startDelay) throws InterruptedException {
    final int arraySize = numThreads * numIteration;
    map = new Long2LongHashMap(arraySize, hashedKeys);

    Thread[] threads = new Thread[numThreads];
    for (int i = 0; i < numThreads; i++) {
      threads[i] = new Thread(r);
      Thread.sleep(startDelay);
      threads[i].start();
    }

    long start = System.currentTimeMillis();

    for (int i = 0; i < numThreads; i++) {
      threads[i].join();
    }

    long end = System.currentTimeMillis();

    LOG.info("Time is {}\n size={} tombstones={} capacity={}", 
      end - start, map.size(), map.totalTombstoneObjects(), map.capacity());

  }
}
