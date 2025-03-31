/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.carrotdata.cache;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.Callable;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.carrotdata.cache.controllers.MinAliveRecyclingSelector;
import com.carrotdata.cache.index.CompactBaseWithExpireIndexFormat;
import com.carrotdata.cache.io.BaseDataWriter;
import com.carrotdata.cache.io.BaseFileDataReader;
import com.carrotdata.cache.io.BaseMemoryDataReader;
import com.carrotdata.cache.util.TestUtils;

public class TestLoadingObjectCache {

  boolean memory = true;
  ObjectCache cache;
  int segmentSize = 4 * 1024 * 1024;
  long maxCacheSize = 100L * segmentSize;
  long expireTime;

  int numThreads = 1;

  private volatile boolean failed = false;

  @After
  public void tearDown() throws IOException {
    cache.getNativeCache().dispose();
    TestUtils.deleteCacheFiles(cache.getNativeCache());
  }

  @Before
  public void setUp() throws IOException {
    cache = createCache("test-cache");
    cache.registerClasses(Integer.class);
  }

  protected ObjectCache createCache(String cacheName) throws IOException {
    // Data directory
    Path path = Files.createTempDirectory(null);
    File dir = path.toFile();
    dir.deleteOnExit();
    String rootDir = dir.getAbsolutePath();

    Builder builder = new Builder(cacheName);

    builder.withCacheDataSegmentSize(segmentSize).withCacheMaximumSize(maxCacheSize)
        .withRecyclingSelector(MinAliveRecyclingSelector.class.getName())
        .withDataWriter(BaseDataWriter.class.getName())
        .withMemoryDataReader(BaseMemoryDataReader.class.getName())
        .withFileDataReader(BaseFileDataReader.class.getName())
        .withMainQueueIndexFormat(CompactBaseWithExpireIndexFormat.class.getName())
        .withCacheRootDirs(new String[] {rootDir});

    if (memory) {
      return builder.buildObjectMemoryCache();
    } else {
      return builder.buildObjectDiskCache();
    }
  }

  @Test
  public void testSingleThread() throws IOException {
    runTest(1);
  }

  @Test
  public void testMultiThreads() throws IOException {
    runTest(16);
  }

  private void runTest(int numThreads) throws IOException {
    this.failed = false;
    this.numThreads = numThreads;
    int numIterations = 100;

    Runnable r = () -> {
      for (int i = 0; i < numIterations; i++) {
        final int ii = i;
        Callable<Integer> call = new Callable<Integer>() {
          @Override
          public Integer call() throws Exception {
            Thread.sleep(10);
            return ii;
          }
        };
        try {
          Integer v = (Integer) cache.get(Integer.valueOf(i), call, 0);
          assertEquals(i, v.intValue());
        } catch (Throwable e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
          // It does not fail the test
          failed = true;
        }
      }
      for (int i = 0; i < numIterations; i++) {
        try {
          Integer v = (Integer) cache.get(Integer.valueOf(i));
          assertEquals(i, v.intValue());
        } catch (Throwable e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
          failed = true;
        }
      }
    };
    Thread[] workers = new Thread[numThreads];
    for (int i = 0; i < numThreads; i++) {
      workers[i] = new Thread(r);
      workers[i].start();
    }

    for (int i = 0; i < numThreads; i++) {
      try {
        workers[i].join();
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }
    if (failed) {
      fail();
    }
  }
}
