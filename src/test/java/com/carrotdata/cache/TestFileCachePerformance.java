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
  
  private static String dataDir;
  
  private static int segmentSize = 64_000_000;
  
  private static long maxCacheSize = 300_000_000_000L;
  
  private static AtomicLong loaded = new AtomicLong();
  
  private static Cache cache;
  
  public static void main(String[] args) throws IOException, InterruptedException {
    //dataDir = args[0];
    value = createValue();
    cache = createCache();
    loadData();
    readData();
    cache.dispose();
  }

  private static void readData() throws InterruptedException {
    numThreads = 16;
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
          LOG.info("read = {}", count);
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
    Path path = Files.createTempDirectory(null);
    File dir = path.toFile();
    dir.deleteOnExit();
    String rootDir = dir.getAbsolutePath();
    Builder builder = new Builder(cacheName);
    builder.withCacheDataSegmentSize(segmentSize)
      .withCacheMaximumSize(maxCacheSize)
      .withMainQueueIndexFormat(SubCompactBaseIndexFormat.class.getName())
      .withCacheRootDirs(new String[] {rootDir})
      .withStartIndexNumberOfSlotsPower(19)
      .withTLSSupported(true);
    return builder.buildDiskCache();
  }
}
