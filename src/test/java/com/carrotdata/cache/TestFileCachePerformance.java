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
package com.carrotdata.cache;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotdata.cache.index.SubCompactBaseIndexFormat;

public class TestFileCachePerformance {
  private static final Logger LOG = LoggerFactory.getLogger(TestFileCachePerformance.class);

  private static int numThreads = 8;
  
  private static long numRecords = 25_000_000;
  
  private static int valueSize = 10_000;
  
  private static byte[] value;
  
  private static String[] dataDirs;
  
  private static int segmentSize = 64_000_000;
  
  private static long maxCacheSize = 300_000_000_000L;
  
  private static AtomicLong loaded = new AtomicLong();
  
  private static Cache cache;
  
  
  //@Before
  public void setUp() throws IOException {
    updateConfig();
    logTestParameters();
    TestFileCachePerformance.value = createValue();
    cache = createCache();
  }
  
  private static void updateConfig() {
    String value = System.getProperty("segmentSize");
    if (value != null) {
      segmentSize = Integer.parseInt(value);
    }
    value = System.getProperty("cacheSize");
    if (value != null) {
      maxCacheSize = Long.parseLong(value);
    }
    
    value = System.getProperty("numThreads");
    if (value != null) {
      numThreads = Integer.parseInt(value);
    }
    
    value = System.getProperty("dataDirs");
    if (value != null) {
      dataDirs = value.split(",");
    }

    value = System.getProperty("valueSize");
    if (value != null) {
      valueSize = Integer.parseInt(value);
    }
    
    value = System.getProperty("numRecords");
    if (value != null) {
      numRecords = Long.parseLong(value);
    }
  }
  
  private static void logTestParameters() {
    LOG.info("cache size      ={}", maxCacheSize);
    LOG.info("segment size    ={}", segmentSize);
    LOG.info("value size      ={}", valueSize);
    LOG.info("data dirs       ={}", dataDirs != null? String.join(",", dataDirs): "temp");
    LOG.info("num records     ={}", numRecords);
    LOG.info("write threads   ={}", numThreads);
    LOG.info("read  threads   ={}", dataDirs != null? 16 * dataDirs.length:  16);

  }

  //@After
  public void tearDown() {
    cache.dispose();
  }
  
  //@Test
  public void testFileCacheLoadRead() throws InterruptedException, IOException {
    LOG.info("Started Test File Load and Read");
    loadData();
    readData();
    LOG.info("Finished Test File Load and Read, press any key ...");
    System.in.read();
  }
  
  public static void main(String[] args) throws IOException, InterruptedException {
    if (args != null && args.length > 0) {
      dataDirs = args[0].split(",");
    }
    
    updateConfig();
    logTestParameters();
    
    value = createValue();
    cache = createCache();
    loadData();
    readData();
    cache.dispose();
  }

  private static void readData() throws InterruptedException {
    int dirsNum = dataDirs != null? dataDirs.length: 1;
    numThreads = 16 * dirsNum;
    long toRead = numRecords / 10;
    long t1 = System.currentTimeMillis();
    Runnable r = () -> {
      byte[] buf = new byte[2 * valueSize];
      long count = 0;  
      int failed = 0;
      ThreadLocalRandom rnd = ThreadLocalRandom.current();
      while(count++ < toRead) {
        long n = rnd.nextLong(numRecords);
        byte[] key = ("KEY:"+ n).getBytes();
        try {
          long size = cache.get(key, 0, key.length, buf, 0);
          if (size < 0) {
            failed++;
          }
        } catch (IOException e) {
          e.printStackTrace();
        }
        if ((count % 100000) == 0) {
          LOG.info("read = {} failed={}", count, failed);
        }
      }
      LOG.info("Read ={} failed={}", toRead, failed);
    };
    
    Thread[] workers = new Thread[numThreads];
    for (int i = 0; i < numThreads; i++) {
      workers[i] = new Thread(r);
      workers[i].start();
    }
    
    for (int i = 0; i < numThreads; i++) {
      workers[i].join();
    }
    long t2 = System.currentTimeMillis();

    LOG.info("Read {} records in {}ms, RPS={}", toRead * numThreads, t2 - t1, toRead * numThreads * 1000 / (t2 - t1));
  }

  private static void loadData() throws InterruptedException {
    
    long t1 = System.currentTimeMillis();
    Runnable r = () -> {
      long count = 0;  
      while((count = loaded.getAndIncrement()) < numRecords) {
        byte[] key = ("KEY:"+ count).getBytes();
        try {
          cache.put(key, value, 0);
        } catch (IOException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
        if ((count % 100000) == 0) {
          LOG.info("loaded = {}", count);
        }
      }
    };
    Thread[] workers = new Thread[numThreads];
    for (int i = 0; i < numThreads; i++) {
      workers[i] = new Thread(r);
      workers[i].start();
    }
    
    for (int i = 0; i < numThreads; i++) {
      workers[i].join();
    }
    long t2 = System.currentTimeMillis();

    LOG.info("Loaded {} records in {}ms, RPS={}", numRecords, t2 - t1, numRecords * 1000 / (t2 - t1));
  }

  private static byte[] createValue() {
    value = new byte[valueSize];
    Random r = new Random();
    r.nextBytes(value);
    return value;
  }
  
  private static Cache createCache() throws IOException {
    String cacheName = "cache";
    // Data directory
    String[] roots = dataDirs;
    
    if (roots == null) {
      Path path = Files.createTempDirectory(null);
      File dir = path.toFile();
      dir.deleteOnExit();
      String rootDir = dir.getAbsolutePath();
      
      LOG.info("Data dir={}", rootDir);
      
      roots = new String[] { rootDir };
    }
    
    Builder builder = new Builder(cacheName);
    builder.withCacheDataSegmentSize(segmentSize)
      .withCacheMaximumSize(maxCacheSize)
      .withMainQueueIndexFormat(SubCompactBaseIndexFormat.class.getName())
      .withCacheRootDirs(roots)
      .withStartIndexNumberOfSlotsPower(21)
      .withTLSSupported(true)
      .withVacuumCleanerInterval(-1);
    return builder.buildDiskCache();
  }
}
