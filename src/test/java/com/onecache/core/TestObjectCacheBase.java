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
package com.onecache.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.junit.After;
import org.junit.Test;

import com.onecache.core.util.TestUtils;
import com.onecache.core.Builder;
import com.onecache.core.ObjectCache;
import com.onecache.core.Scavenger;
import com.onecache.core.controllers.MinAliveRecyclingSelector;
import com.onecache.core.index.CompactBaseWithExpireIndexFormat;
import com.onecache.core.io.BaseDataWriter;
import com.onecache.core.io.BaseFileDataReader;
import com.onecache.core.io.BaseMemoryDataReader;
import com.onecache.core.util.Utils;

public abstract class TestObjectCacheBase  {
  
  boolean offheap = true;
  ObjectCache cache;
  int segmentSize = 4 * 1024 * 1024;
  long maxCacheSize = 100L * segmentSize;
  int scavengerInterval = 10000; // seconds - disable for test
  long expireTime;
  double scavDumpBelowRatio = 0.5;
  double minActiveRatio = 0.90;
  
  int numThreads = 1;
  
  @After
  public void tearDown() throws IOException {
    cache.getNativeCache().dispose();
    TestUtils.deleteCacheFiles(cache.getNativeCache());
  }
  
  protected ObjectCache createCache(String cacheName) throws IOException {
    // Data directory
    Path path = Files.createTempDirectory(null);
    File  dir = path.toFile();
    dir.deleteOnExit();
    String rootDir = dir.getAbsolutePath();
    
    Builder builder = new Builder(cacheName);
    
    builder
      
      .withCacheDataSegmentSize(segmentSize)
      .withCacheMaximumSize(maxCacheSize)
      .withScavengerRunInterval(scavengerInterval)
      .withScavengerDumpEntryBelowMin(scavDumpBelowRatio)
      .withRecyclingSelector(MinAliveRecyclingSelector.class.getName())
      .withDataWriter(BaseDataWriter.class.getName())
      .withMemoryDataReader(BaseMemoryDataReader.class.getName())
      .withFileDataReader(BaseFileDataReader.class.getName())
      .withMainQueueIndexFormat(CompactBaseWithExpireIndexFormat.class.getName())
      .withCacheRootDir(rootDir)
      .withMinimumActiveDatasetRatio(minActiveRatio)
      .withCacheStreamingSupportBufferSize(1 << 19)
      .withEvictionDisabledMode(true);
    
    if (offheap) {
      return builder.buildObjectMemoryCache();
    } else {
      return builder.buildObjectDiskCache();
    }
  }
  
 
  private int loadData(int num) throws IOException {

    int count = 0;
    long tn = Thread.currentThread().getId();
    long t1 = System.currentTimeMillis();
    while(count < num) {
      String key = tn + ":user:" + count;
      List<Integer> value = new ArrayList<Integer>();
      for(int i = 0; i < 1000; i++) {
        value.add(count + i);
      }
      long tt1 = System.nanoTime();
      boolean result = cache.put(key, value, 0);
      long tt2 = System.nanoTime();
      if (!result) {
        break;
      }
      count++;
      if (count % 10000 == 0) {
        System.out.printf("%d:loaded %d objects last put=%d micro\n", tn, count, (tt2- tt1)/1000);
      }
    }
    long t2 = System.currentTimeMillis();
    System.out.printf("%d:loaded %d in %d ms\n", tn, count, (t2 - t1));
    return count;
  }
  
  @SuppressWarnings("deprecation")
  private void verifyData(ObjectCache cache, int n) throws IOException {
    int count = 0;
    long tn = Thread.currentThread().getId();
    long t1 = System.currentTimeMillis();
    while(count < n) {
      String key = tn + ":user:" + count;
      long tt1 = System.nanoTime();
      Object v = cache.get(key);
      long tt2 = System.nanoTime();
      ArrayList<?> value = (ArrayList<?>) v;
      assertEquals(1000, value.size());
      for(int i = 0; i < 1000; i++) {
        assertEquals(new Integer(count + i), value.get(i));
      }
      count++;
      if (count % 10000 == 0) {
        System.out.printf("%d:verified %d objects get=%d micro\n",tn, count, (tt2- tt1) / 1000);
      }
    }
    long t2 = System.currentTimeMillis();
    System.out.printf("verified %d in %d ms\n", count, (t2 - t1));
  }

  private int loadPersonData(int num) throws IOException {

    int count = 0;
    long tn = Thread.currentThread().getId();

    long t1 = System.currentTimeMillis();
    Random r = new Random(1);
    while(count < num) {
      Person p = Person.nextPerson(r);
      String key = p.getFio() + tn;
      
      long tt1 = System.nanoTime();
      boolean result = cache.put(key, p, 0);
      long tt2 = System.nanoTime();
      if (!result) {
        break;
      }
      count++;
      if (count % 100000 == 0) {
        System.out.printf("%d:loaded %d objects last put=%d micro\n", tn, count, (tt2- tt1)/1000);
      }
    }
    long t2 = System.currentTimeMillis();
    System.out.printf("%d:loaded %d in %d ms\n", tn, count, (t2 - t1));
    return count;
  }
  
  private void verifyPersonData(ObjectCache cache, int n) throws IOException {
    int count = 0;
    long tn = Thread.currentThread().getId();
    long t1 = System.currentTimeMillis();
    Random r = new Random(1);
    while(count < n) {
      Person exp = Person.nextPerson(r);
      String key = exp.getFio() + tn;
      long tt1 = System.nanoTime();
      Person v = (Person) cache.get(key);
      assertTrue(v != null);
      long tt2 = System.nanoTime();
      assertEquals(exp.fio, v.fio);
      assertEquals(exp.address.street, v.address.street);
      assertEquals(exp.address.city, v.address.city);
      assertEquals(exp.address.state, v.address.state);
      assertEquals(exp.address.country, v.address.country);
      
      count++;
      if (count % 100000 == 0) {
        System.out.printf("%d:verified %d objects get=%d micro\n", tn, count, (tt2- tt1) / 1000);
      }
    }
    long t2 = System.currentTimeMillis();
    System.out.printf("%d:verified %d in %d ms\n", tn, count, (t2 - t1));
  }

  //@Ignore
  @Test
  public void testLoadAndVerify() throws IOException {
    System.out.println("Test load and verify");
    Scavenger.clear();
    this.cache = createCache("test");
    this.cache.addKeyValueClasses(String.class,  ArrayList.class);
    int loaded = loadData(1000000);
    verifyData(cache, loaded);
  }
  
  @Test
  public void testLoadAndVerifyPerson() throws IOException {
    System.out.println("Test load and verify person");
    Scavenger.clear();
    this.cache = createCache("testPerson");
    this.cache.addKeyValueClasses(String.class, Person.class);
    
    ObjectCache.SerdeInitializationListener listener = (x) -> {
      x.register(Address.class);
    };
    
    this.cache.addSerdeInitializationListener(listener);
    int loaded = loadPersonData(1000000);
    verifyPersonData(cache, loaded);
  }
  
  @Test
  public void testLoadAndVerifyMultithreaded() throws IOException {
    System.out.println("Test load and verify multithreaded");
    Scavenger.clear();
    
    this.maxCacheSize = 1000L * this.segmentSize;
    this.numThreads = 4;
    this.cache = createCache("test");
    this.cache.addKeyValueClasses(String.class, ArrayList.class);

    Runnable r = () -> {
      int loaded;
      try {
        loaded = loadData(1000000);
        verifyData(cache, loaded);
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
        fail();
      }
    };
    runAll(r);
  }
  
  @Test
  public void testLoadAndVerifyPersonMultithreaded() throws IOException {
    System.out.println("Test load and verify person multithreaded");
    Scavenger.clear();
    this.maxCacheSize = 1000L * this.segmentSize;
    this.numThreads = 4;
    this.cache = createCache("testPerson");
    this.cache.addKeyValueClasses(String.class, Person.class);

    ObjectCache.SerdeInitializationListener listener = (x) -> {
      x.register(Address.class);
    };
    
    this.cache.addSerdeInitializationListener(listener);
    
    Runnable r = () -> {
      int loaded;
      try {
        loaded = loadPersonData(1000000);
        verifyPersonData(cache, loaded);
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
        fail();
      }
    };
    runAll(r);
  }
  
  private void runAll(Runnable r) {
    Thread[] workers = new Thread[numThreads];
    for (int i = 0; i < numThreads; i++) {
      workers[i] = new Thread(r);
      workers[i].start();
    }
    
    for(int i = 0; i < numThreads; i++) {
      try {
        workers[i].join();
      } catch (InterruptedException e) {
      }
    }
  }
  
  //@Ignore
  @Test
  public void testSaveLoad() throws IOException {
    System.out.println("Test save load");
    Scavenger.clear();

    this.cache = createCache("test");
    this.cache.addKeyValueClasses(String.class, ArrayList.class);

    int loaded = loadData(1000000);
    
    System.out.println("loaded=" + loaded);
    verifyData(cache, loaded);

    String cacheName = cache.getName();
    long t1 = System.currentTimeMillis();
    cache.shutdown();
    long t2 = System.currentTimeMillis();
    System.out.printf("Saved %d in %d ms\n", cache.getStorageAllocated(), t2 - t1);
    
    t1 = System.currentTimeMillis();
    String rootDir = cache.getCacheConfig().getCacheRootDir(cacheName);
    ObjectCache newCache = ObjectCache.loadCache(rootDir, cacheName);
    newCache.addKeyValueClasses(String.class, ArrayList.class);

    t2 = System.currentTimeMillis();
    System.out.printf("Loaded %d in %d ms\n", cache.getStorageAllocated(), t2 - t1);
    
    assertEquals(cache.getCacheType(), newCache.getCacheType());
    assertEquals(cache.activeSize(), newCache.activeSize());
    assertEquals(cache.getMaximumCacheSize(), newCache.getMaximumCacheSize());
    assertEquals(cache.getStorageAllocated(), newCache.getStorageAllocated());
    assertEquals(cache.getStorageUsed(), newCache.getStorageUsed());
    assertEquals(cache.getTotalGets(), newCache.getTotalGets());
    assertEquals(cache.getTotalGetsSize(), newCache.getTotalGetsSize());
    assertEquals(cache.getTotalHits(), newCache.getTotalHits());
    assertEquals(cache.getTotalWrites(), newCache.getTotalWrites());
    assertEquals(cache.getTotalWritesSize(), newCache.getTotalWritesSize());

    verifyData(newCache, loaded);

    newCache.getNativeCache().dispose();;
    TestUtils.deleteCacheFiles(newCache.getNativeCache());
  }
  
  static class Person {
    String fio;
    Address address;
    
 
    public String getFio() {
      return fio;
    }
    
    static Person nextPerson(Random r) {
      Person p = new Person();
      p.fio = Utils.getRandomStr(r, 16);
      p.address = Address.nextAddress(r);
      return p;
    }
  }
  
  static class Address {
    
    String street;
    String city;
    String state;
    String country;
    
    static Address nextAddress(Random r) {
      Address a = new Address();
      a.street = Utils.getRandomStr(r, 20);
      a.city = Utils.getRandomStr(r, 15);
      a.state = Utils.getRandomStr(r, 10);
      a.country = Utils.getRandomStr(r, 10);
      return a;
    }
  }
}
