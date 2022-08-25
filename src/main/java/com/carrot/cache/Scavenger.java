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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.carrot.cache.index.MemoryIndex;
import com.carrot.cache.index.MemoryIndex.Result;
import com.carrot.cache.index.MemoryIndex.ResultWithRankAndExpire;
import com.carrot.cache.io.IOEngine;
import com.carrot.cache.io.Segment;
import com.carrot.cache.io.SegmentScanner;
import com.carrot.cache.util.CacheConfig;
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
  
  static class Stats implements Persistent {

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

    Stats() {}
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
      dos.writeLong(totalBytesExpired);
      dos.writeLong(totalBytesFreed);
      dos.writeLong(totalBytesScanned);
      dos.writeLong(totalEmptySegments);
      dos.writeLong(totalItemsExpired);
      dos.writeLong(totalItemsFreed);
      dos.writeLong(totalItemsScanned);
      dos.writeLong(totalRuns);
      dos.writeLong(totalRunTimes);
    }
    
    @Override
    public void load(InputStream is) throws IOException {
      DataInputStream dis = Utils.toDataInputStream(is);
      totalBytesExpired = dis.readLong();
      totalBytesFreed = dis.readLong();
      totalBytesScanned = dis.readLong();
      totalEmptySegments = dis.readLong();
      totalItemsExpired = dis.readLong();
      totalItemsFreed = dis.readLong();
      totalItemsScanned  = dis.readLong();
      totalRuns = dis.readLong();
      totalRunTimes = dis.readLong();
    }
  }

  static Stats stats = new Stats();

  /**
   * Get scavenger statistics
   *
   * @return scavenger statistics;
   */
  public static Stats stats() {
    return stats;
  }

  private final Cache cache;
  
  private final CacheConfig config;

  private volatile long runStartTime = System.currentTimeMillis();

  private static double dumpBelowRatioMin;
  
  private static double dumpBelowRatioMax;
  
  private static double dumpBelowRatio = -1;

  private static double adjStep = 0.05;
  
  private static double stopRatio;
  
  /* Clean deleted only items - do not purge low ranks*/
  private boolean cleanDeletedOnly = true;
  
  public Scavenger(Cache cache) {
    super("c2 scavenger");
    this.cache = cache;
    this.config = CacheConfig.getInstance();
    // Update stats
    stats.totalRuns++;
    String cacheName = this.cache.getName();
    if (dumpBelowRatio < 0) {
      dumpBelowRatio = this.config.getScavengerDumpEntryBelowStart(cacheName);
      dumpBelowRatioMin = dumpBelowRatio;
      dumpBelowRatioMax = this.config.getScavengerDumpEntryBelowStop(cacheName);
      adjStep = this.config.getScavengerDumpEntryBelowAdjStep(cacheName);
    }
    stopRatio =  this.config.getScavengerStopMemoryRatio(cacheName);
  }
  
  /**
   * Used for testing
   */
  public static void clear() {
    dumpBelowRatio = -1;
  }
  
  @Override
  public void run() {
    long runStart = System.currentTimeMillis();
    IOEngine engine = this.cache.getEngine();
    DateFormat format = DateFormat.getDateInstance();
    LOG.info("Scavenger started at %s allocated storage=%d maximum storage=%d",
      format.format(new Date()), engine.getStorageAllocated(), engine.getMaximumStorageSize());
    
    boolean finished = false;
    
    while (!finished) {
      Segment s = engine.getSegmentForRecycling();
      if (s == null) {
        LOG.warn(Thread.currentThread().getName() + ": empty segment");
        return;
      }
      if (shouldStopOn(s)) {
        break;
      }
      engine.startRecycling(s);
      long maxExpire = s.getInfo().getMaxExpireAt();
      if (s.getInfo().getTotalActiveItems() == 0 || 
           (maxExpire < System.currentTimeMillis() && maxExpire > 0)) {
        stats.totalEmptySegments ++;
      }
      try {
        finished = cleanSegment(s); 
        if (finished && cleanDeletedOnly) {
          // continue with purging low rank elements
          cleanDeletedOnly = false;
          finished = false;
        }
        // Dispose segment
        engine.disposeDataSegment(s);    
      } catch (IOException e) {
        LOG.error(e);
        return;
      }
      // Update admission controller statistics 
      engine.finishRecycling(s);
    }
    long runEnd = System.currentTimeMillis();
    // Update stats
    stats.totalRunTimes += (runEnd - runStart);
    LOG.info("Scavenger finished at %s allocated storage=%d maximum storage=%d",
      format.format(new Date()), engine.getStorageAllocated(), engine.getMaximumStorageSize());
  }

  private boolean shouldStopOn(Segment s) {
    long expire = s.getInfo().getMaxExpireAt();
    long n = s.getAliveItems();
    long currentTime = System.currentTimeMillis();
    IOEngine engine = cache.getEngine();
    long size = engine.size();
    if (engine.size() == 0) {
      return true;
    }
    if ((expire > 0 && expire <= currentTime) || n == 0) {
      return false;
    }
    
    long activeSize = engine.activeSize();
    double activeRatio = (double) activeSize / size;
    double minActiveRatio = config.getMinimumActiveDatasetRatio(cache.getName());
    
    if (activeRatio >= minActiveRatio) {
      cleanDeletedOnly = false;
    } else {
      cleanDeletedOnly = true;
    }
    double usage = engine.getStorageAllocatedRatio();
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
      //*DEBUG*/ System.out.println(s.getId() + " is fully empty");
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
    int scanned = 0;
    int deleted = 0;
    int expired = 0;
    int notFound = 0;
    ResultWithRankAndExpire result = new ResultWithRankAndExpire();

    try {
      
      sc = engine.getScanner(s); // acquires read lock
      boolean isDirect =  sc.isDirect();
      
      byte[] keyBuffer = new byte[4096];
      byte[] valueBuffer = new byte[4096];
      
      while (sc.hasNext()) {
        // TODO: will it work for file based scanner? - FIXME
        final long keyPtr = sc.keyAddress();
        final int keySize = sc.keyLength();
        final long valuePtr = sc.valueAddress();
        final int valSize = sc.valueLength();
        final int totalSize = Utils.kvSize(keySize, valSize);
        stats.totalBytesScanned += totalSize;
        
        keyBuffer = checkBuffer(keyBuffer, keySize, isDirect);
        valueBuffer = checkBuffer(valueBuffer, valSize, isDirect);
        
        double ratio = cleanDeletedOnly? 0: dumpBelowRatio;
        
        if (isDirect) {
          result = index.checkDeleteKeyForScavenger(keyPtr, keySize, result, ratio);
        } else {
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
              this.cache.put(keyPtr, keySize, valuePtr, valSize, expire, rank, true);
            } else {
              sc.getValue(valueBuffer, 0);
              this.cache.put(keyBuffer, 0, keySize, valueBuffer, 0, valSize, expire, rank, true);
            }
            break;
        }
        sc.next();
      }
      /* DEBUG System.out.println(
          "id ="
              + s.getId()
              + " scanned="
              + scanned
              + " deleted="
              + deleted
              + " not found="
              + notFound
              + " expired="
              + expired
              + " usageRatio="
              + engine.getStorageAllocatedRatio());
      */
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

    byte[] buffer = new byte[4096];

    try {
      sc = engine.getScanner(s); // acquires read lock
      boolean isDirect = sc.isDirect();

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

  public static boolean decreaseThroughput() {
    if (dumpBelowRatio + adjStep > dumpBelowRatioMax) {
      return false;
    }
    dumpBelowRatio += adjStep;
    return true;
  }

  public static boolean increaseThroughput() {
    if (dumpBelowRatio - adjStep < dumpBelowRatioMin) {
      return false;
    }
    dumpBelowRatio -= adjStep;
    return true;  
  }
}
