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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.carrot.cache.index.IndexFormat;
import com.carrot.cache.index.MemoryIndex;
import com.carrot.cache.io.IOEngine;
import com.carrot.cache.io.Segment;
import com.carrot.cache.io.SegmentScanner;
import com.carrot.cache.util.CacheConfig;
import com.carrot.cache.util.Persistent;
import com.carrot.cache.util.UnsafeAccess;
import com.carrot.cache.util.Utils;

import static com.carrot.cache.index.MemoryIndex.ResultWithRankAndExpire;
import static com.carrot.cache.index.MemoryIndex.Result;

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

  private volatile long runStartTime = System.currentTimeMillis();

  private static double dumpBelowRatioMin;
  
  private static double dumpBelowRatioMax;
  
  private static double dumpBelowRatio = -1;
  
  private static double adjStep = 0.05;
  
  public Scavenger(Cache parent) {
    super("c2 scavenger");
    this.cache = parent;
    // Update stats
    stats.totalRuns++;
    String cacheName = this.cache.getName();
    if (dumpBelowRatio < 0) {
      dumpBelowRatio = this.cache.getCacheConfig().getScavengerDumpEntryBelowStart(cacheName);
      dumpBelowRatioMin = dumpBelowRatio;
      dumpBelowRatioMax = this.cache.getCacheConfig().getScavengerDumpEntryBelowStop(cacheName);
      adjStep = this.cache.getCacheConfig().getScavengerDumpEntryBelowAdjStep(cacheName);
    }
  }
  
  @Override
  public void run() {
    long runStart = System.currentTimeMillis();
    IOEngine engine = cache.getEngine();
    boolean finished = false;
    while (!finished) {
      Segment s = engine.getSegmentForRecycling();
      if (s == null) {
        LOG.error(Thread.currentThread().getName()+": empty segment");
        return;
      }
      this.cache.startSegment(s);
      if (s.getInfo().getTotalItems() == 0) {
        stats.totalEmptySegments ++;
      }
      try {
        finished = cleanSegment(s);
        // Release segment
        engine.releaseSegmentId(s);        
      } catch (IOException e) {
        LOG.error(e);
        return;
      }
      // Update admission controller statistics 
      this.cache.finishSegment(s);
    }
    long runEnd = System.currentTimeMillis();
    // Update stats
    stats.totalRunTimes += (runEnd - runStart);
  }

  // TODO: refactor this method
  private boolean cleanSegment(Segment s) throws IOException {
    // Check if segment is empty on active items
    CacheConfig config = this.cache.getCacheConfig();
    String cacheName = this.cache.getName();
    double stopRatio = config.getScavengerStopMemoryRatio(cacheName);
    Segment.Info info = s.getInfo();
    SegmentScanner sc = null;
    long indexBuf = 0;
    try {
      if (info.getTotalItems() == 0) {
        // We can dump it completely w/o asking memory index
        //
        long dataSize = info.getSegmentDataSize();
        cache.reportUsed(-dataSize);
        // Update stats
        stats.totalBytesFreed += dataSize;
      } else {
        IOEngine engine = cache.getEngine();
        MemoryIndex index = engine.getMemoryIndex();
        IndexFormat format = index.getIndexFormat();
        int indexEntrySize = format.indexEntrySize();
        indexBuf = UnsafeAccess.malloc(indexEntrySize);

        sc = engine.getScanner(s);

        while (sc.hasNext()) {
          final long keyPtr = sc.keyAddress();
          final int keySize = sc.keyLength();
          final long valuePtr = sc.valueAddress();
          final int valSize = sc.valueLength();
          final int totalSize = Utils.kvSize(keySize, valSize);
          
          stats.totalBytesScanned += totalSize;

          ResultWithRankAndExpire result = new ResultWithRankAndExpire();
          
          result = index.checkDeleteKeyForScavenger(keyPtr, keySize, result, dumpBelowRatio);
          
          Result res = result.getResult();
          int rank = result.getRank();
          long expire = result.getExpire();
          
          switch (res) {
            case EXPIRED:
              cache.reportUsed(-totalSize);
              stats.totalBytesExpired += totalSize;
              stats.totalBytesFreed += totalSize;
              stats.totalItemsExpired += 1;
              break;
            case NOT_FOUND:
              cache.reportUsed(-totalSize);
              stats.totalBytesFreed += totalSize;
              break;
            case DELETED:
              cache.reportUsed(-totalSize);
              // Update stats
              stats.totalBytesFreed += totalSize;
              // Return Item back to AQ
              // TODO: fix this code. We need to move data to a victim cache on
              // memory index eviction.
              break;
            case OK:
              // Put value back into the cache - it has high popularity
              engine.put(keyPtr, keySize, valuePtr, valSize, expire, rank);
              break;
          }
          sc.next();
        }
      }
      double usage = cache.getMemoryUsedPct();
      return usage < stopRatio;
    } finally {
      if (sc != null) {
        sc.close();
      }
      if (indexBuf > 0) {
        // Free buffer
        UnsafeAccess.free(indexBuf);
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
