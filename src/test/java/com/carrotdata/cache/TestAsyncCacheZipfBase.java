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
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.math3.distribution.ZipfDistribution;
import org.junit.After;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotdata.cache.controllers.LRCRecyclingSelector;
import com.carrotdata.cache.controllers.RecyclingSelector;
import com.carrotdata.cache.eviction.EvictionPolicy;
import com.carrotdata.cache.eviction.LRUEvictionPolicy;
import com.carrotdata.cache.io.FutureResultByteArray;
import com.carrotdata.cache.io.FutureResultByteBuffer;
import com.carrotdata.cache.util.Percentile;
import com.carrotdata.cache.util.TestUtils;
import com.carrotdata.cache.util.UnsafeAccess;
import com.carrotdata.cache.util.Utils;

public abstract class TestAsyncCacheZipfBase {
  
  static enum Result {
    YES, NO, DONT_KNOW, FAILED;
  }
  private static final Logger LOG = LoggerFactory.getLogger(TestAsyncCacheZipfBase.class);

  protected Cache cache;

  protected boolean memory = true;

  protected boolean evictionDisabled = false;

  protected long segmentSize = 4 * 1024 * 1024;

  protected long maxCacheSize = 1000L * segmentSize;

  int scavengerInterval = 2; // seconds

  int scavNumberThreads = 1;

  double scavDumpBelowRatio = 1.0;

  double minActiveRatio = 0.90;

  protected int maxKeySize = 32;

  protected int maxValueSize = 10000;

  protected int numRecords = 10;

  protected int numIterations = 0;

  protected int numThreads = 1;

  protected Class<? extends EvictionPolicy> epClz = LRUEvictionPolicy.class;

  protected Class<? extends RecyclingSelector> rsClz = LRCRecyclingSelector.class;

  protected double zipfAlpha = 0.9;

  protected long testStartTime = System.currentTimeMillis();

  protected double aqStartRatio = 0.3;

  protected double pqStartRatio = 0.2;

  protected String parentCacheName = "default";

  protected String victimCacheName = "victim";

  protected int slruInsertionPoint = 7;

  protected long spinOnWaitTime;

  protected boolean hybridCacheInverseMode = false;
  
  protected int prQueueSize = 8;

  static class Pair<T> {
    int index;
    T buffer;
    long ptr;
    Pair(int index, T buffer){
      this.index = index;
      this.buffer = buffer;
    }
    Pair(int index, T buffer, long ptr){
      this.index = index;
      this.buffer = buffer;
      this.ptr = ptr;
    }
  }
  
  
  static class ResultPair {
    int index;
    Result result;
    ResultPair(int index, Result result){
      this.index = index;
      this.result = result;
    }
  }
  /**
   * Future result buffers - allocated per thread
   */
  static ThreadLocal<Queue<?>> buffers = new ThreadLocal<Queue<?>>();
  
  /**
   * Pending requests queue - per thread
   */
  static ThreadLocal<Queue<?>> prQueue = new ThreadLocal<Queue<?>>();
  @After
  public void tearDown() throws IOException {
    this.cache.printStats();
    Scavenger.printStats();
    this.cache.dispose();
    // Delete temp data
    TestUtils.deleteCacheFiles(cache);
    Scavenger.clear();
  }

  protected Cache createCache() throws IOException {
    String cacheName = parentCacheName;
    // Data directory
    Path rootDirPath = Files.createTempDirectory(null);
    String rootDir = rootDirPath.toFile().getAbsolutePath();
    Builder builder = new Builder(cacheName);

    builder.withCacheDataSegmentSize(segmentSize)
        .withCacheMaximumSize(maxCacheSize)
        .withScavengerRunInterval(scavengerInterval)
        .withScavengerDumpEntryBelowMin(scavDumpBelowRatio)
        .withCacheEvictionPolicy(epClz.getName())
        .withRecyclingSelector(rsClz.getName())
        .withCacheRootDirs(new String[] {rootDir})
        .withMinimumActiveDatasetRatio(minActiveRatio)
        .withAsyncIOPoolSize(numThreads * prQueueSize);
   
    builder = withAddedConfigurations(builder);
    Cache c = null;
    if (memory) {
      c = builder.buildMemoryCache();
    } else {
      c = builder.buildDiskCache();
    }
    return c;
  }

  /**
   * Subclasses may override
   * @param b builder instance
   * @return builder instance
   */
  protected Builder withAddedConfigurations(Builder b) {
    return b;
  }

  protected int safeBufferSize() {
    int bufSize = Utils.kvSize(maxKeySize, maxValueSize);
    return 2 * bufSize;
  }

  @SuppressWarnings("unchecked")
  protected void checkBuffersByteArray() {
    Queue<FutureResultByteArray> q = (Queue<FutureResultByteArray>) buffers.get();
    if (q == null) {
      q = new LinkedBlockingQueue<FutureResultByteArray>(prQueueSize);
      for (int i = 0; i < prQueueSize; i++) {
        byte[] buf = new byte[safeBufferSize()];
        q.add(new FutureResultByteArray(buf, 0));   
      }
      buffers.set(q);
    }
  }
  
  @SuppressWarnings("unchecked")
  protected void checkRequestQueueByteArray() {
    Queue<Pair<FutureResultByteArray>> q = (Queue<Pair<FutureResultByteArray>>) prQueue.get();
    if (q == null) {
      q = new LinkedBlockingQueue<Pair<FutureResultByteArray>>(prQueueSize);
      prQueue.set(q);
    }
  }
  
  protected FutureResultByteArray getFutureResultByteArray() {
    @SuppressWarnings("unchecked")
    Queue<FutureResultByteArray> q = (Queue<FutureResultByteArray>) buffers.get();
    if (q.isEmpty()) return null;
    return q.poll();
  }
  
  protected void returnFutureResultByteArray(FutureResultByteArray b) {
    @SuppressWarnings("unchecked")
    Queue<FutureResultByteArray> q = (Queue<FutureResultByteArray>) buffers.get();
    q.offer(b);
  }
  
  @SuppressWarnings("unchecked")
  protected void checkBuffersByteBuffer() {
    Queue<FutureResultByteBuffer> q = (Queue<FutureResultByteBuffer>) buffers.get();
    if (q == null) {
      q = new LinkedBlockingQueue<FutureResultByteBuffer>(prQueueSize);
      for (int i = 0; i < prQueueSize; i++) {
        ByteBuffer buf = ByteBuffer.allocateDirect(safeBufferSize());
        q.add(new FutureResultByteBuffer(buf, 0));   
      }
      buffers.set(q);
    }
  }
  
  @SuppressWarnings("unchecked")
  protected void checkRequestQueueByteBuffer() {
    Queue<Pair<FutureResultByteBuffer>> q = (Queue<Pair<FutureResultByteBuffer>>) prQueue.get();
    if (q == null) {
      q = new LinkedBlockingQueue<Pair<FutureResultByteBuffer>>(prQueueSize);
      prQueue.set(q);
    }
  }
  
  protected FutureResultByteBuffer getFutureResultByteBuffer() {
    @SuppressWarnings("unchecked")
    Queue<FutureResultByteBuffer> q = (Queue<FutureResultByteBuffer>) buffers.get();
    if (q.isEmpty()) return null;
    return q.poll();
  }
  
  protected void returnFutureResultByteBuffer(FutureResultByteBuffer b) {
    @SuppressWarnings("unchecked")
    Queue<FutureResultByteBuffer> q = (Queue<FutureResultByteBuffer>) buffers.get();
    q.offer(b);
  }
  
  protected void joinAll(Thread[] workers) {
    for (Thread t : workers) {
      try {
        t.join();
      } catch (Exception e) {
      }
    }
  }

  protected Thread[] startAll(Runnable r) {
    Thread[] workers = new Thread[numThreads];
    for (int i = 0; i < workers.length; i++) {
      workers[i] = new Thread(r);
      workers[i].start();
    }
    return workers;
  }

  protected long getExpireStream(long startTime, int n) {
    return startTime + 10000000L;
  }

  protected long getExpire(int n) {
    return System.currentTimeMillis() + 10000000L;
  }

  protected void runBytesStreamZipf(int total) throws IOException {
    checkBuffersByteArray();
    checkRequestQueueByteArray();
    int loaded = 0;
    int hits = 0;
    int failed = 0;
    ZipfDistribution dist = new ZipfDistribution(this.numRecords, this.zipfAlpha);
    Percentile perc = new Percentile(10000, total);
    for (int i = 0; i < total; i++) {
      int n = dist.sample();
      long start = System.nanoTime();
      List<ResultPair> results = verifyBytesStream(n);
      for (ResultPair rp : results) {
        Result result = rp.result;
        int index = rp.index;
        switch (result) {
          case YES:
            hits++;
            break;
          case NO:
            boolean res = loadBytesStream(index);
            if (res) {
              loaded++;
            } else {
              failed++;
            }
            break;
          case FAILED:
            failed++;
            break;
          case DONT_KNOW:
        }
      }
      long end = System.nanoTime();
      perc.add(end - start);
      if (i > 0 && i % 500000 == 0) {
        LOG.info("{} - i={} hit rate={}", Thread.currentThread().getName(), i,
          (float) cache.getOverallHitRate());
      }
    }
    LOG.info("{} - hit={} loaded={} failed={}", Thread.currentThread().getName(),
      (float) hits / total, loaded, failed);
    LOG.info("Thread={} latency: min={}ns max={}ns p50={}ns p90={}ns p99={}ns p999={}ns p9999={}ns",
      Thread.currentThread().getName(), perc.min(), perc.max(), perc.value(0.5), perc.value(0.9),
      perc.value(0.99), perc.value(0.999), perc.value(0.9999));
    // Clear thread locals
    buffers.get().clear();
    prQueue.get().clear();
  }

  protected void runBytesStreamZipfNative(int total) throws IOException {
    checkBuffersByteArray();
    checkRequestQueueByteArray();
    int loaded = 0;
    int hits = 0;
    int failed = 0;
    ZipfDistribution dist = new ZipfDistribution(this.numRecords, this.zipfAlpha);
    Percentile perc = new Percentile(10000, total);
    for (int i = 0; i < total; i++) {
      int n = dist.sample();
      long start = System.nanoTime();
      List<ResultPair> results = verifyBytesStreamNative(n);
      for (ResultPair rp : results) {
        Result result = rp.result;
        int index = rp.index;
        switch (result) {
          case YES:
            hits++;
            break;
          case NO:
            boolean res = loadBytesStream(index);
            if (res) {
              loaded++;
            } else {
              failed++;
            }
            break;
          case FAILED:
            failed++;
            break;
          case DONT_KNOW:
        }
      }
      long end = System.nanoTime();
      perc.add(end - start);
      if (i > 0 && i % 500000 == 0) {
        LOG.info("{} - i={} hit rate={}", Thread.currentThread().getName(), i,
          (float) cache.getOverallHitRate());
      }
    }
    LOG.info("{} - hit={} loaded={} failed={}", Thread.currentThread().getName(),
      (float) hits / total, loaded, failed);
    LOG.info("Thread={} latency: min={}ns max={}ns p50={}ns p90={}ns p99={}ns p999={}ns p9999={}ns",
      Thread.currentThread().getName(), perc.min(), perc.max(), perc.value(0.5), perc.value(0.9),
      perc.value(0.99), perc.value(0.999), perc.value(0.9999));
    // Clear thread locals
    buffers.get().clear();
    prQueue.get().clear();
  }
  
  protected boolean loadBytesStream(int n) throws IOException {
    long id = Thread.currentThread().getId();
    byte[] key = ("KEY:" + id + ":" + n).getBytes();
    // To improve performance
    byte[] value = getValue((long) n);//new byte[valueSize];
    long expire = getExpire(n);
    boolean result = this.cache.put(key, value, expire);
    return result;
  }

  protected byte[] getValue(long n) {
    long id = Thread.currentThread().getId();
    Random r = new Random(id + n * Integer.MAX_VALUE);
    int valueSize = r.nextInt(maxValueSize - 1) + 1;
    byte[] value = new byte[valueSize];
    r.nextBytes(value);
    return value;
  }
  
  @SuppressWarnings({ "unchecked" })
  protected List<ResultPair> verifyBytesStream(int n) throws IOException {
    List<ResultPair> retValue = new ArrayList<ResultPair>();
    FutureResultByteArray buffer = (FutureResultByteArray) buffers.get().poll();
    if (buffer == null) {
      //busy loop: wait for pending requests queue
      Queue<?> queue = prQueue.get();
      while(true) {
        Iterator<?> it = queue.iterator();
        Pair<?> found = null;
        while(it.hasNext()) {
          Pair<?> p =  (Pair<?>) it.next();
          if (((FutureResultByteArray) p.buffer).isDone()) {
            found = p;
            break;
          }
        }
        if (found == null) {
          continue;
        }
        queue.remove(found);
        buffer = (FutureResultByteArray) found.buffer;
        byte[] buf = buffer.getBuffer();
        int index = found.index;
        int valueSize = buffer.getResult();
        Result res = verifyBytes(index, buf, valueSize);
        buffer.reset();

        retValue.add(new ResultPair(index, res));
        break;
      }
    }
    long id = Thread.currentThread().getId();
    byte[] key = ("KEY:" + id + ":" + n).getBytes();
    this.cache.getAsync(key, 0, key.length, true, buffer);
    
    if (buffer.isDone()) {
      byte[] buf = buffer.getBuffer();
      int valueSize = buffer.getResult();
      Result res = verifyBytes(buf, n, valueSize);
      buffer.reset();
      ((Queue<FutureResultByteArray>)buffers.get()).offer(buffer);
      retValue.add(new ResultPair(n, res));
    } else {
     ((Queue<Pair<FutureResultByteArray>>) prQueue.get()).offer(new Pair<FutureResultByteArray>(n, buffer));
    }
    return retValue;
  }

  protected Result verifyBytes(byte[] buffer, int n, int valueSize) throws IOException {
    if (valueSize < 0) {
      return Result.NO;
    }
    byte[] value = getValue(n);
    int vSize = value.length;
    
    try {
      assertEquals(vSize, valueSize);
      assertTrue(Utils.compareTo(buffer, 0, valueSize, value, 0, vSize) == 0);
    } catch (AssertionError e) {
      e.printStackTrace();
      return Result.FAILED;
    }
    return Result.YES;
  }
    
  protected Result verifyBytes(int n, byte[] buffer, int valueSize) throws IOException {
    if (valueSize < 0) {
      return Result.NO;
    }
    return verifyBytes(buffer, n, valueSize);
  }

  @SuppressWarnings({ "unchecked" })
  protected List<ResultPair> verifyBytesStreamNative(int n) throws IOException {
    List<ResultPair> retValue = new ArrayList<ResultPair>();
    FutureResultByteArray buffer = (FutureResultByteArray) buffers.get().poll();
    if (buffer == null) {
      //busy loop: wait for pending requests queue
      Queue<?> queue = prQueue.get();
      while(true) {
        Iterator<?> it = queue.iterator();
        Pair<?> found = null;
        while(it.hasNext()) {
          Pair<?> p =  (Pair<?>) it.next();
          if (((FutureResultByteArray) p.buffer).isDone()) {
            found = p;
            break;
          }
        }
        if (found == null) {
          continue;
        }
        queue.remove(found);
        buffer = (FutureResultByteArray) found.buffer;
        byte[] buf = buffer.getBuffer();
        int index = found.index;
        int valueSize = buffer.getResult();
        Result res = verifyBytesNative(index, buf, valueSize);
        buffer.reset();
        if (found.ptr != 0) {
          UnsafeAccess.free(found.ptr);
        }
        retValue.add(new ResultPair(index, res));
        break;
      }
    }
    long id = Thread.currentThread().getId();
    byte[] key = ("KEY:" + id + ":" + n).getBytes();
    int keySize = key.length;
    long keyPtr = UnsafeAccess.allocAndCopy(key, 0, key.length);
    this.cache.getAsync(keyPtr, keySize, true, buffer);
    if (buffer.isDone()) {
      byte[] buf = buffer.getBuffer();
      int valueSize = buffer.getResult();
      Result res = verifyBytesNative(buf, n, valueSize); 
      buffer.reset();
      ((Queue<FutureResultByteArray>)buffers.get()).offer(buffer);
      retValue.add(new ResultPair(n, res));
      UnsafeAccess.free(keyPtr);
    } else {
     ((Queue<Pair<FutureResultByteArray>>) prQueue.get()).offer(new Pair<FutureResultByteArray>(n, buffer));
    }

    return retValue;
  }

  protected Result verifyBytesNative(byte[] buffer, int n, int valueSize) throws IOException {
    if (valueSize < 0) {
      return Result.NO;
    }
    byte[] value = getValue(n);
    int vSize = value.length;
    try {
      assertEquals(vSize, valueSize);

      boolean result = Utils.compareTo(buffer, 0, valueSize, value, 0, valueSize) == 0;
      assertTrue(result);

    } catch (AssertionError e) {
      e.printStackTrace();
      return Result.FAILED;
    }
    return Result.YES;
  }
    
  protected Result verifyBytesNative(int n, byte[] buffer, int valueSize) throws IOException {
    if (valueSize < 0) {
      return Result.NO;
    }
    Result r = verifyBytesNative(buffer, n, valueSize);
    return r;
  }

  protected void runMemoryStreamZipf(int total) throws IOException {
    checkBuffersByteBuffer();
    checkRequestQueueByteBuffer();
    int loaded = 0;
    int hits = 0;
    int failed = 0;
    ZipfDistribution dist = new ZipfDistribution(this.numRecords, this.zipfAlpha);
    Percentile perc = new Percentile(10000, total);
    for (int i = 0; i < total; i++) {
      int n = dist.sample();
      long start = System.nanoTime();
      List<ResultPair> results = verifyMemoryStream(n);
      for (ResultPair rp : results) {
        Result result = rp.result;
        int index = rp.index;
        switch (result) {
          case YES:
            hits++;
            break;
          case NO:
            boolean res = loadMemoryStream(index);
            if (res) {
              loaded++;
            } else {
              failed++;
            }
            break;
          case FAILED:
            failed++;
            break;
          case DONT_KNOW:
        }
      }
      long end = System.nanoTime();
      perc.add(end - start);
      if (i > 0 && i % 500000 == 0) {
        LOG.info("{} - i={} hit rate={}", Thread.currentThread().getName(), i,
          (float) cache.getOverallHitRate());
      }
    }
    LOG.info("{} - hit={} loaded={} failed={}", Thread.currentThread().getName(),
      (float) hits / total, loaded, failed);
    LOG.info("Thread={} latency: min={}ns max={}ns p50={}ns p90={}ns p99={}ns p999={}ns p9999={}ns",
      Thread.currentThread().getName(), perc.min(), perc.max(), perc.value(0.5), perc.value(0.9),
      perc.value(0.99), perc.value(0.999), perc.value(0.9999));
    // Clear thread locals
    buffers.get().clear();
    prQueue.get().clear();
  }
  

  protected boolean loadMemoryStream(int n) throws IOException {

    long id = Thread.currentThread().getId();
    String skey = ("KEY:" + id + ":" + n);
    int keySize = skey.length();
    long keyPtr = UnsafeAccess.allocAndCopy(skey, 0, keySize);
    byte[] value = getValue((long) n);
    int valueSize = value.length;
    long valuePtr = UnsafeAccess.allocAndCopy(value, 0, value.length);
    long expire = getExpire(n);
    boolean result = this.cache.put(keyPtr, keySize, valuePtr, valueSize, expire);
    UnsafeAccess.free(keyPtr);
    UnsafeAccess.free(valuePtr);
    return result;
  }

  @SuppressWarnings({ "unchecked" })
  protected List<ResultPair> verifyMemoryStream(int n) throws IOException {
    List<ResultPair> retValue = new ArrayList<ResultPair>();
    FutureResultByteBuffer buffer = (FutureResultByteBuffer) buffers.get().poll();
    if (buffer == null) {
      //busy loop: wait for pending requests queue
      Queue<?> queue = prQueue.get();
      while(true) {
        Iterator<?> it = queue.iterator();
        Pair<?> found = null;
        while(it.hasNext()) {
          Pair<?> p =  (Pair<?>) it.next();
          if (((FutureResultByteBuffer) p.buffer).isDone()) {
            found = p;
            break;
          }
        }
        if (found == null) continue;
        queue.remove(found);
        buffer = (FutureResultByteBuffer) found.buffer;
        ByteBuffer buf = buffer.getBuffer();
        int index = found.index;
        int valueSize = buffer.getResult();
        Result res = verifyMemory(buf, index, valueSize);
        buffer.reset();
        retValue.add(new ResultPair(index, res));
        break;
      }
    }
    long id = Thread.currentThread().getId();
    byte[] key = ("KEY:" + id + ":" + n).getBytes();
    int keySize = key.length;
    this.cache.getAsync(key, 0, keySize, true, buffer);
    if (buffer.isDone()) {
      ByteBuffer buf = buffer.getBuffer();
      int valueSize = buffer.getResult();
      Result res = verifyMemory(buf, n, valueSize);
      buffer.reset();
      ((Queue<FutureResultByteBuffer>)buffers.get()).offer(buffer);
      retValue.add(new ResultPair(n, res));
    } else {
     ((Queue<Pair<FutureResultByteBuffer>>) prQueue.get()).offer(new Pair<FutureResultByteBuffer>(n, buffer));
    }
    return retValue;
  }

  protected Result verifyMemory(ByteBuffer buffer, int n, int valueSize) throws IOException {
    if (valueSize < 0) {
      return Result.NO;
    }
    byte[] value = getValue(n);
    int vSize = value.length;
    
    try {
      assertEquals(vSize, valueSize);
      assertTrue(Utils.compareTo(buffer, valueSize, value, 0, vSize) == 0);

    } catch (AssertionError e) {
      e.printStackTrace();
      return Result.FAILED;
    }
    return Result.YES;
  }
  
  protected void runMemoryStreamZipfNative(int total) throws IOException {
    checkBuffersByteBuffer();
    checkRequestQueueByteBuffer();
    int loaded = 0;
    int hits = 0;
    int failed = 0;
    ZipfDistribution dist = new ZipfDistribution(this.numRecords, this.zipfAlpha);
    Percentile perc = new Percentile(10000, total);
    for (int i = 0; i < total; i++) {
      int n = dist.sample();
      long start = System.nanoTime();
      List<ResultPair> results = verifyMemoryStreamNative(n);
      for (ResultPair rp : results) {
        Result result = rp.result;
        int index = rp.index;
        switch (result) {
          case YES:
            hits++;
            break;
          case NO:
            boolean res = loadMemoryStream(index);
            if (res) {
              loaded++;
            } else {
              failed++;
            }
            break;
          case FAILED:
            failed++;
            break;
          case DONT_KNOW:
        }
      }
      long end = System.nanoTime();
      perc.add(end - start);
      if (i > 0 && i % 500000 == 0) {
        LOG.info("{} - i={} hit rate={}", Thread.currentThread().getName(), i,
          (float) cache.getOverallHitRate());
      }
    }
    LOG.info("{} - hit={} loaded={} failed={}", Thread.currentThread().getName(),
      (float) hits / total, loaded, failed);
    LOG.info("Thread={} latency: min={}ns max={}ns p50={}ns p90={}ns p99={}ns p999={}ns p9999={}ns",
      Thread.currentThread().getName(), perc.min(), perc.max(), perc.value(0.5), perc.value(0.9),
      perc.value(0.99), perc.value(0.999), perc.value(0.9999));
    // Clear thread locals
    buffers.get().clear();
    prQueue.get().clear();
  }
  
  @SuppressWarnings({ "unchecked" })
  protected List<ResultPair> verifyMemoryStreamNative(int n) throws IOException {
    List<ResultPair> retValue = new ArrayList<ResultPair>();
    FutureResultByteBuffer buffer = (FutureResultByteBuffer) buffers.get().poll();
    if (buffer == null) {
      //busy loop: wait for pending requests queue
      Queue<?> queue = prQueue.get();
      while(true) {
        Iterator<?> it = queue.iterator();
        Pair<?> found = null;
        while(it.hasNext()) {
          Pair<?> p =  (Pair<?>) it.next();
          if (((FutureResultByteBuffer) p.buffer).isDone()) {
            found = p;
            break;
          }
        }
        if (found == null) continue;
        queue.remove(found);
        buffer = (FutureResultByteBuffer) found.buffer;
        ByteBuffer buf = buffer.getBuffer();
        int index = found.index;
        int valueSize = buffer.getResult();
        Result res = verifyMemoryNative(buf, index, valueSize);
        buffer.reset();
        if (found.ptr != 0) {
          UnsafeAccess.free(found.ptr);
        }
        retValue.add(new ResultPair(index, res));
        break;
      }
    }
    long id = Thread.currentThread().getId();
    byte[] key = ("KEY:" + id + ":" + n).getBytes();
    int keySize = key.length;
    long keyPtr = UnsafeAccess.allocAndCopy(key, 0, keySize);
    this.cache.getAsync(key, 0, keySize, true, buffer);
    if (buffer.isDone()) {
      ByteBuffer buf = buffer.getBuffer();
      int valueSize = buffer.getResult();
      Result res = verifyMemoryNative(buf, n, valueSize);
      buffer.reset();
      ((Queue<FutureResultByteBuffer>)buffers.get()).offer(buffer);
      retValue.add(new ResultPair(n, res));
      UnsafeAccess.free(keyPtr);

    } else {
     ((Queue<Pair<FutureResultByteBuffer>>) prQueue.get()).offer(new Pair<FutureResultByteBuffer>(n, buffer));
    }
    return retValue;
  }

  protected Result verifyMemoryNative(ByteBuffer buffer, int n, int valueSize) throws IOException {
    if (valueSize < 0) {
      return Result.NO;
    }
    byte[] value = getValue(n);
    int vSize = value.length;
    try {
      assertEquals(vSize, valueSize);
      assertTrue(Utils.compareTo(buffer, valueSize, value, 0, vSize) == 0);
    } catch (AssertionError e) {
      e.printStackTrace();
      return Result.FAILED;
    }
    return Result.YES;
  }
    
  protected void testContinuosLoadBytesRun() throws IOException {

    // Create cache after setting is configuration parameters
    this.cache = createCache();
    this.numIterations = this.numIterations > 0 ? this.numIterations : this.numRecords;
    Runnable r = () -> {
      try {
        runBytesStreamZipf(this.numIterations);
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    };
    long start = System.currentTimeMillis();
    Thread[] all = startAll(r);
    joinAll(all);
    long stop = System.currentTimeMillis();
    Scavenger.printStats();
    LOG.info("Thread={} time={}ms rps={}", Thread.currentThread().getName(), stop - start,
      (this.numThreads * (long) this.numIterations) * 1000 / (stop - start));
  }

  protected void testContinuosLoadMemoryRun() throws IOException {
    // Create cache after setting is configuration parameters
    this.cache = createCache();
    this.numIterations = this.numIterations > 0 ? this.numIterations : this.numRecords;

    Runnable r = () -> {
      try {
        runMemoryStreamZipf(this.numIterations);
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    };
    long start = System.currentTimeMillis();
    Thread[] all = startAll(r);
    joinAll(all);
    long stop = System.currentTimeMillis();
    Scavenger.printStats();
    LOG.info("Thread={} time={}ms rps={}", Thread.currentThread().getName(), stop - start,
      (this.numThreads * (long) this.numIterations) * 1000 / (stop - start));
  }

  protected void testContinuosLoadBytesNativeRun() throws IOException {

    // Create cache after setting is configuration parameters
    this.cache = createCache();
    this.numIterations = this.numIterations > 0 ? this.numIterations : this.numRecords;
    Runnable r = () -> {
      try {
        runBytesStreamZipfNative(this.numIterations);
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    };
    long start = System.currentTimeMillis();
    Thread[] all = startAll(r);
    joinAll(all);
    long stop = System.currentTimeMillis();
    Scavenger.printStats();
    LOG.info("Thread={} time={}ms rps={}", Thread.currentThread().getName(), stop - start,
      (this.numThreads * (long) this.numIterations) * 1000 / (stop - start));
  }

  protected void testContinuosLoadMemoryNativeRun() throws IOException {
    // Create cache after setting is configuration parameters
    this.cache = createCache();
    this.numIterations = this.numIterations > 0 ? this.numIterations : this.numRecords;

    Runnable r = () -> {
      try {
        runMemoryStreamZipfNative(this.numIterations);
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    };
    long start = System.currentTimeMillis();
    Thread[] all = startAll(r);
    joinAll(all);
    long stop = System.currentTimeMillis();
    Scavenger.printStats();
    LOG.info("Thread={} time={}ms rps={}", Thread.currentThread().getName(), stop - start,
      (this.numThreads * (long) this.numIterations) * 1000 / (stop - start));
  }
  
}
