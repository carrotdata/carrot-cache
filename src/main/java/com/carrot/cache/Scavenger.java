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
package com.carrot.cache;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.text.DateFormat;
import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.carrot.cache.index.MemoryIndex;
import com.carrot.cache.index.MemoryIndex.Result;
import com.carrot.cache.index.MemoryIndex.ResultWithRankAndExpire;
import com.carrot.cache.io.IOEngine;
import com.carrot.cache.io.Segment;
import com.carrot.cache.io.SegmentScanner;
import com.carrot.cache.util.CarrotConfig;
import com.carrot.cache.util.Persistent;
import com.carrot.cache.util.Utils;

public class Scavenger extends Thread {

  public static interface Listener {
    
    /**
     * Start segment
     * @param s segment
     */
    public void startSegment(Segment s);
    
    /**
     * Finish segment
     * @param s segment
     */
    public void finishSegment(Segment s);
    
  }
  /** Logger */
  private static final Logger LOG = LogManager.getLogger(Scavenger.class);
  
  public static class Stats implements Persistent {
    
    /** Cache name */
    String cacheName;
    
    /** Total times scavenger ran */
    long totalRuns;

    /** Total empty segments */
    long totalEmptySegments;
    
    /** Total run time in ms; */
    long totalRunTimes;

    /** Total items scanned */
    long totalItemsScanned;

    /** Total items freed */
    long totalItemsFreed;

    /** Total items expired */
    long totalItemsExpired;

    /** Total bytes scanned */
    long totalBytesScanned;

    /** Total bytes freed */
    long totalBytesFreed;

    /** Total bytes expired */
    long totalBytesExpired;

    double dumpBelowRatioMin;
    
    double dumpBelowRatioMax;
    
    double dumpBelowRatio;

    double adjStep;
    
    double stopRatio;
    
    Stats(String cacheName) {
      this.cacheName = cacheName;
      CarrotConfig conf = CarrotConfig.getInstance();
      dumpBelowRatio = conf.getScavengerDumpEntryBelowStart(cacheName);
      dumpBelowRatioMin = dumpBelowRatio;
      dumpBelowRatioMax = conf.getScavengerDumpEntryBelowStop(cacheName);
      adjStep = conf.getScavengerDumpEntryBelowAdjStep(cacheName);
      stopRatio =  conf.getScavengerStopMemoryRatio(cacheName);
    }
    
    /**
     * Get total runs
     *
     * @return total runs
     */
    public long getTotalRuns() {
      return totalRuns;
    }

    /**
     * Get total empty segments
     * @return total number of empty segments
     */
    public long getTotalEmptySegments() {
      return totalEmptySegments;
    }
    
    /**
     * Get total run time
     *
     * @return total run time
     */
    public long getTotalRunTimes() {
      return totalRunTimes;
    }

    /**
     * Get total items scanned
     *
     * @return total items scanned
     */
    public long getTotalItemsScanned() {
      return totalItemsScanned;
    }

    /**
     * Get total items freed
     *
     * @return total items freed
     */
    public long getTotalItemsFreed() {
      return totalItemsFreed;
    }

    /**
     * Get total items expired
     *
     * @return total items expired
     */
    public long getTotalItemsExpired() {
      return totalItemsExpired;
    }

    /**
     * Get total bytes scanned
     *
     * @return total bytes scanned
     */
    public long getTotalBytesScanned() {
      return totalBytesScanned;
    }

    /**
     * Get total bytes freed
     *
     * @return total bytes freed
     */
    public long getTotalBytesFreed() {
      return totalBytesFreed;
    }

    /**
     * Get total bytes expired
     *
     * @return total bytes expired
     */
    public long getTotalBytesExpired() {
      return totalBytesExpired;
    }
    
    @Override
    public void save(OutputStream os) throws IOException {
      DataOutputStream dos = Utils.toDataOutputStream(os);
      dos.writeUTF(cacheName);
      dos.writeLong(totalBytesExpired);
      dos.writeLong(totalBytesFreed);
      dos.writeLong(totalBytesScanned);
      dos.writeLong(totalEmptySegments);
      dos.writeLong(totalItemsExpired);
      dos.writeLong(totalItemsFreed);
      dos.writeLong(totalItemsScanned);
      dos.writeLong(totalRuns);
      dos.writeLong(totalRunTimes);
      dos.writeDouble(dumpBelowRatioMin);
      dos.writeDouble(dumpBelowRatioMax);
      dos.writeDouble(dumpBelowRatio);
      dos.writeDouble(adjStep);
      dos.writeDouble(stopRatio);
    }
    
    @Override
    public void load(InputStream is) throws IOException {
      DataInputStream dis = Utils.toDataInputStream(is);
      cacheName = dis.readUTF();
      totalBytesExpired = dis.readLong();
      totalBytesFreed = dis.readLong();
      totalBytesScanned = dis.readLong();
      totalEmptySegments = dis.readLong();
      totalItemsExpired = dis.readLong();
      totalItemsFreed = dis.readLong();
      totalItemsScanned  = dis.readLong();
      totalRuns = dis.readLong();
      totalRunTimes = dis.readLong();
      dumpBelowRatioMin = dis.readDouble();
      dumpBelowRatioMax = dis.readDouble();
      dumpBelowRatio = dis.readDouble();
      adjStep = dis.readDouble();
      stopRatio = dis.readDouble();
    }
  }

  /**
   * Map keeps statistics per cache instance (by cache name)
   */
  static ConcurrentHashMap<String, Stats> statsMap = new ConcurrentHashMap<String, Stats>();

  /**
   * Get scavenger statistics
   *
   * @return scavenger statistics;
   */
  public static Stats getStatisticsForCache(String cacheName) {
    Stats stats = statsMap.get(cacheName);
    if (stats == null) {
      stats = new Stats(cacheName);
      Stats s = statsMap.putIfAbsent(cacheName, stats);
      if (s != null) {
        return s;
      }
    }
    return stats;
  }

  /**
   *  Set scavenger statistics
   * @param cacheName cache name
   * @param stats stats
   */
  public static void setStatisticsForCache(String cacheName, Stats stats){
    statsMap.put(cacheName,  stats);
  }
  
  public static void printStats() {
    for(Map.Entry<String,Stats> entry: statsMap.entrySet()) {
      String name = entry.getKey();
      Stats stats = entry.getValue();
      System.out.printf("Scavenger [%s]: runs=%d scanned=%d freed=%d written back=%d empty segments=%d\n", 
        name, stats.getTotalRuns(), stats.getTotalBytesScanned(), 
        stats.getTotalBytesFreed(), stats.getTotalBytesScanned() - stats.getTotalBytesFreed(),
        stats.getTotalEmptySegments());
    }
  }
  
  private Stats stats;
  
  private final Cache cache;
  
  private final CarrotConfig config;

  private volatile long runStartTime = System.currentTimeMillis();

  private double dumpBelowRatioMin;
  
  private double dumpBelowRatioMax;
  
  private double dumpBelowRatio = -1;

  private double adjStep = 0.05;
  
  private double stopRatio;
  
  private static AtomicInteger numInstances = new AtomicInteger();
  
  private static int maxInstances = 1; // TODO: make it configurable
  
  /* Clean deleted only items - do not purge low ranks*/
  private boolean cleanDeletedOnly = true;
  
  private long maxSegmentsBeforeStallDetected;
  
  private boolean evictionDisabledMode = false;
  
  public Scavenger(Cache cache) {
    super("c2 scavenger");
    this.cache = cache;
    this.config = CarrotConfig.getInstance();
    String cacheName = this.cache.getName();

    // Update stats
    stats = statsMap.get(cache.getName());
    
    if (stats == null) {
      stats = new Stats(cache.getName());
      statsMap.put(cache.getName(), stats);
      dumpBelowRatio = this.config.getScavengerDumpEntryBelowStart(cacheName);
      dumpBelowRatioMin = dumpBelowRatio;
      dumpBelowRatioMax = this.config.getScavengerDumpEntryBelowStop(cacheName);
      adjStep = this.config.getScavengerDumpEntryBelowAdjStep(cacheName);
      stopRatio =  this.config.getScavengerStopMemoryRatio(cacheName);
      stats.dumpBelowRatio = dumpBelowRatio;
      stats.dumpBelowRatioMin = dumpBelowRatioMin;
      stats.dumpBelowRatioMax = dumpBelowRatioMax;
      stats.adjStep = adjStep;
      stats.stopRatio = stopRatio;
    } else {
      dumpBelowRatio = stats.dumpBelowRatio;
      dumpBelowRatioMin = stats.dumpBelowRatioMin;
      dumpBelowRatioMax = stats.dumpBelowRatioMax;
      adjStep = stats.adjStep;
      stopRatio = stats.stopRatio;
    }
    this.maxSegmentsBeforeStallDetected = getScavengerMaxSegmentsBeforeStall();
    this.evictionDisabledMode = this.config.getEvictionDisabledMode(cache.getName());
    if (this.evictionDisabledMode) {
      // Disable all cold items cleaning - only deleted and updated ones
      dumpBelowRatio = -0.01;
      dumpBelowRatioMax = -0.01;
      dumpBelowRatioMin = -0.01;
      adjStep = 0.0;
    }
    stats.totalRuns++;
    
  }
  
  private long getScavengerMaxSegmentsBeforeStall() {
    if (this.evictionDisabledMode) {
      // Never stalls
      return Long.MAX_VALUE;
    }
    
    long maxCacheSize = this.cache.getMaximumCacheSize();
    long segmentSize = this.cache.getCacheConfig().getCacheSegmentSize(cache.getName());
    long maxSegments = maxCacheSize / segmentSize;
    double startRatio = this.cache.getCacheConfig().getScavengerStartMemoryRatio(cache.getName());
    double range = startRatio - stopRatio;
    // TODO on this value
    return (int) (2 * maxSegments * range);
    
  }
  
  /**
   * Used for testing
   */
  public static void clear() {
    statsMap.clear();
  }

  @Override
  public void run() {
    
    long runStart = System.currentTimeMillis();
    IOEngine engine = this.cache.getEngine();
    DateFormat format = DateFormat.getDateTimeInstance();
    Segment s = null;
    try {
      
      if (numInstances.incrementAndGet() > maxInstances) {
        numInstances.decrementAndGet();
        return;
      }
      LOG.info(
          "scavenger [{}] started at {} allocated storage={} maximum storage={}", cache.getName(),
          format.format(new Date()), engine.getStorageAllocated(), engine.getMaximumStorageSize());

      boolean finished = false;
      
      int segmentsProcessed = 0;
      
      while (!finished) {
        if (Thread.currentThread().isInterrupted()) {
          break;
        }
        s = engine.getSegmentForRecycling();
        if (s == null) {
          break;
        }
        if (shouldStopOn(s)) {
          break;
        }
        //TODO: make it more smarter, calculate maxSegmentsBeforeStallDetected using maximum number of segments
        // and other current cache configuration values
        if (segmentsProcessed >= maxSegmentsBeforeStallDetected) {
          // This should never happen if eviction is disabled
          dumpBelowRatio = 1.0;// improve scavenger's performance - dump everything
          cleanDeletedOnly = false;
        }
        engine.startRecycling(s);
        long maxExpire = s.getInfo().getMaxExpireAt();
        if (s.getInfo().getTotalActiveItems() == 0
            || (maxExpire < System.currentTimeMillis() && maxExpire > 0)) {
          stats.totalEmptySegments++;
        }
        try {
          finished = cleanSegment(s);
          if (finished && evictionDisabledMode) {
            // Means scavenger did not manage to clean any data
            // in the last segment - hence storage is FULL and
            // some data must deleted explicitly
            break;
          }
          if (finished && cleanDeletedOnly) {
            // continue with purging low rank elements
            cleanDeletedOnly = false;
            finished = false;
          }
          // Dispose segment
          engine.disposeDataSegment(s);
        } catch (IOException e) {
          LOG.error("Cache : %s segment id=%d offheap =%s sealed=%s", cache.getName(), 
            s == null? -1: s.getId(), s==null? null: Boolean.toString(s.isOffheap()), 
                s == null? null: Boolean.toString(s.isSealed()));
          LOG.error(e);
          return;
        }
        // Update admission controller statistics
        engine.finishRecycling(s);
        segmentsProcessed++;
      }
    } finally {
      numInstances.decrementAndGet();
      this.cache.finishScavenger(this);
    }
    long runEnd = System.currentTimeMillis();
    // Update stats
    stats.totalRunTimes += (runEnd - runStart);
    LOG.info(
        "scavenger [{}] finished at {} allocated storage={} maximum storage=%{}", cache.getName(),
        format.format(new Date()), engine.getStorageAllocated(), engine.getMaximumStorageSize());
  }

  private boolean shouldStopOn(Segment s) {
    long expire = s.getInfo().getMaxExpireAt();
    long n = s.getAliveItems();
    long currentTime = System.currentTimeMillis();
    IOEngine engine = cache.getEngine();
    if (engine.size() == 0) {
      return true;
    }
    if ((expire > 0 && expire <= currentTime) || n == 0) {
      return false;
    }
    double sratio = (double) s.getAliveItems() / s.getTotalItems();    
    double minActiveRatio = config.getMinimumActiveDatasetRatio(cache.getName());
 
    double usage = engine.getStorageAllocatedRatio();
    double activeRatio = engine.activeSizeRatio();
    if (!evictionDisabledMode) {
      if (sratio >= minActiveRatio) {
        cleanDeletedOnly = false;
      } else {
        cleanDeletedOnly = true;
      }
      cleanDeletedOnly = cleanDeletedOnly && usage < stopRatio;
    }
    return activeRatio >= minActiveRatio && usage < stopRatio;   
  }

  private boolean cleanSegment(Segment s) throws IOException {

    Segment.Info info = s.getInfo();
    long currentTime = System.currentTimeMillis();
    long maxExpire = info.getMaxExpireAt();
    boolean allExpired = maxExpire > 0 && maxExpire <= currentTime;
    boolean empty = !allExpired && info.getTotalActiveItems() == 0;
    if (allExpired || empty) {
      // We can dump it completely w/o asking memory index
      long dataSize = info.getSegmentDataSize();
      // Update stats
      stats.totalBytesFreed += dataSize;
      stats.totalBytesScanned += dataSize;
      return false; // not finished yet
    } else {
      return cleanSegmentInternal(s);
    }
  }

  private byte[] checkBuffer(byte[] buffer, int requiredSize, boolean isDirect) {
    if (isDirect) {
      return buffer;
    }
    if (buffer.length < requiredSize) {
      buffer = new byte[requiredSize];
    }
    return buffer;
  }
    
  private boolean cleanSegmentInternal(Segment s) throws IOException {
    IOEngine engine = this.cache.getEngine();
    MemoryIndex index = engine.getMemoryIndex();
    SegmentScanner sc = null;
    @SuppressWarnings("unused")
    int scanned = 0;
    int deleted = 0;
    int expired = 0;
    int notFound = 0;
    ResultWithRankAndExpire result = new ResultWithRankAndExpire();

    try {
      
      sc = engine.getScanner(s); // acquires read lock
      boolean isDirect =  sc.isDirect();
      //TODO Buffer reuse across all scavenger session
      byte[] keyBuffer = !isDirect? new byte[4096]: null;
      byte[] valueBuffer = !isDirect? new byte[4096]: null;
      
      while (sc.hasNext()) {
        final long keyPtr = sc.keyAddress();
        final int keySize = sc.keyLength();
        final long valuePtr = sc.valueAddress();
        final int valSize = sc.valueLength();
        final int totalSize = Utils.kvSize(keySize, valSize);
        stats.totalBytesScanned += totalSize;
        double ratio = cleanDeletedOnly? 0: dumpBelowRatio;
        if (isDirect) {
          result = index.checkDeleteKeyForScavenger(keyPtr, keySize, result, ratio);
        } else {
          keyBuffer = checkBuffer(keyBuffer, keySize, isDirect);
          sc.getKey(keyBuffer, 0);
          result = index.checkDeleteKeyForScavenger(keyBuffer, 0, keySize, result, ratio);
        }

        Result res = result.getResult();
        int rank = result.getRank();
        long expire = result.getExpire();
        scanned++;
        switch (res) {
          case EXPIRED:
            stats.totalBytesExpired += totalSize;
            stats.totalBytesFreed += totalSize;
            stats.totalItemsExpired += 1;
            expired++;
            break;
          case NOT_FOUND:
            stats.totalBytesFreed += totalSize;
            notFound++;
            break;
          case DELETED:
            // Update stats
            stats.totalBytesFreed += totalSize;
            // Return Item back to AQ
            // TODO: fix this code. We need to move data to a victim cache on
            // memory index eviction.
            deleted++;
            break;
          case OK:
            // Put value back into the cache - it has high popularity
            if (isDirect) {
              this.cache.put(keyPtr, keySize, valuePtr, valSize, expire, rank, true, true);
            } else {
              valueBuffer = checkBuffer(valueBuffer, valSize, isDirect);
              sc.getValue(valueBuffer, 0);
              this.cache.put(keyBuffer, 0, keySize, valueBuffer, 0, valSize, expire, rank, true, true);
            }
            break;
        }
        sc.next();
      }
    } finally {
      if (sc != null) {
        sc.close();
      }
    }
    // Returns true if could not clean anything
    // means Scavenger MUST stop and log warning
    // Mostly for testing - in a real application properly configured
    // should never happen
    return (deleted + expired + notFound) == 0;
  }

  @SuppressWarnings("unused")
  private void cleanIndexForSegment(Segment s) throws IOException {
    IOEngine engine = this.cache.getEngine();
    MemoryIndex index = engine.getMemoryIndex();
    SegmentScanner sc = null;

    try {
      sc = engine.getScanner(s); // acquires read lock
      boolean isDirect = sc.isDirect();
      byte[] buffer = !isDirect? new byte[4096]: null;
      while (sc.hasNext()) {
        final long keyPtr = sc.keyAddress();
        final int keySize = sc.keyLength();
        final int valSize = sc.valueLength();
        final int totalSize = Utils.kvSize(keySize, valSize);
        stats.totalBytesScanned += totalSize;
        if (!isDirect && buffer.length < keySize) {
          buffer = new byte[keySize];
        }
        if (isDirect) {
          index.delete(keyPtr, keySize);
        } else {
          sc.getKey(buffer, 0);
          index.delete(buffer, 0, keySize);
        }
        sc.next();
      }
    } finally {
      if (sc != null) {
        sc.close();
      }
    }
  }

  /**
   * Get current Scavenger average run rate
   * @return run rate
   */
  public long getScavengerRunRateAverage() {
    return (long) stats.totalBytesFreed * 1000 / (System.currentTimeMillis() - runStartTime); 
  }

  public static boolean decreaseThroughput(String cacheName) {
    Stats stats = statsMap.get(cacheName);
    if (stats == null) {
      return false;
    }
    if (stats.dumpBelowRatio + stats.adjStep > stats.dumpBelowRatioMax) {
      return false;
    }
    stats.dumpBelowRatio += stats.adjStep;
    return true;
  }

  public static boolean increaseThroughput(String cacheName) {
    Stats stats = statsMap.get(cacheName);
    if (stats == null) {
      return false;
    }
    if (stats.dumpBelowRatio - stats.adjStep < stats.dumpBelowRatioMin) {
      return false;
    }
    stats.dumpBelowRatio -= stats.adjStep;
    return true;  
  }
}
