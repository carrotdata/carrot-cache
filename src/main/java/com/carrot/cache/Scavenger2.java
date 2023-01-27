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

import java.io.IOException;
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
import com.carrot.cache.util.Utils;

public class Scavenger2 extends Scavenger {

  public Scavenger2(Cache cache) {
    super(cache);
    // TODO Auto-generated constructor stub
  }

//  
//  /** Logger */
//  private static final Logger LOG = LogManager.getLogger(Scavenger2.class);
//  
//    
//  
//  /* configurable */
//  protected double stopRatioForDeletedOnlyMode;
//  
//  /* configurable */
//  private double startRatioForDumpBelowRatioMax;
//  
//  /* not configurable */
//  private double startRatioForDumpBelowRatioMin; 
//  
//  /* Total number of segments scanned */
//  private int segmentsScanned;
//  
//  /* Maximum ata segments allowed in the cache*/
//  private int maxSegmentNumber;
//  
//  
//  /* 
//   * Clean deleted only items - do not purge low ranks
//   * Scavenger starts in this mode
//   * */
//  private boolean cleanDeletedOnly = true;
//  
//    
//  public Scavenger2(Cache cache) {
//    super(cache);
//    String cacheName = this.cache.getName();
//    this.dumpBelowRatioMin = this.config.getScavengerDumpEntryBelowMin(cacheName);
//    this.dumpBelowRatio = dumpBelowRatioMin;
//    this.dumpBelowRatioMax = this.config.getScavengerDumpEntryBelowMax(cacheName);
//    this.stopRatio = this.config.getScavengerStopMemoryRatio(cacheName);
//    this.stopRatioForDeletedOnlyMode = this.config.getScavengerStopRatioForDeletedOnlyMode(cacheName);
//    this.startRatioForDumpBelowRatioMax = this.config.getScavengerStartRatioForDumpBelowMax(cacheName);
//    this.maxSegmentNumber = (int) (this.cache.getMaximumCacheSize() / this.cache.getEngine().getSegmentSize());
//    
//    stats.dumpBelowRatio = dumpBelowRatio;
//    stats.dumpBelowRatioMin = dumpBelowRatioMin;
//    stats.dumpBelowRatioMax = dumpBelowRatioMax;
//    //stats.adjStep = adjStep;
//    stats.stopRatio = stopRatio;
//    /*
//     * } else { dumpBelowRatio = stats.dumpBelowRatio; dumpBelowRatioMin = stats.dumpBelowRatioMin;
//     * dumpBelowRatioMax = stats.dumpBelowRatioMax; adjStep = stats.adjStep; stopRatio =
//     * stats.stopRatio; }
//     */
//    //cache.maxSegmentsBeforeStallDetected = getScavengerMaxSegmentsBeforeStall();
//    stats.totalRuns.incrementAndGet();
//
//  }
//  
// 
//  
//  private double getCurrentDumpBelowRatio() {
//    if (this.cleanDeletedOnly) {
//      return 0.;
//    }
//    double segmentsScannedRatio = (double) this.segmentsScanned / this.maxSegmentNumber;
//    if (segmentsScannedRatio > this.startRatioForDumpBelowRatioMax) {
//      return this.dumpBelowRatioMax;
//    } else {
//      double x = segmentsScannedRatio;
//      double min = this.startRatioForDumpBelowRatioMin;
//      double max = this.startRatioForDumpBelowRatioMax;
//      double v1 = this.dumpBelowRatioMin;
//      double v2 = this.dumpBelowRatioMax;
//      return  (v1 * (max - x) + v2 * (x - min)) / (max - min);
//    }
//  }
//  
//  /**
//   * Used for testing
//   */
//  public static void clear() {
//    statsMap.clear();
//  }
//
//  @Override
//  public void run() {
//    
//    long runStart = System.currentTimeMillis();
//    IOEngine engine = this.cache.getEngine();
//    DateFormat format = DateFormat.getDateTimeInstance();
//    Segment s = null;
//    try {
//      
//      if (numInstances.incrementAndGet() > maxInstances) {
//        // Number of instances exceeded the maximum value
//        return;
//      }
//      LOG.debug(
//          "scavenger [{}] started at {} allocated storage={} maximum storage={}", cache.getName(),
//          format.format(new Date()), engine.getStorageAllocated(), engine.getMaximumStorageSize());
//
//      
//      while (true /*!finished*/) {
//        if (Thread.currentThread().isInterrupted()) {
//          break;
//        }
//        s = engine.getSegmentForRecycling();
//        if (s == null) {
//          break;
//        }
//        if (shouldStopOn(s)) {
//          s.setRecycling(false);
//          break;
//        }
//        engine.startRecycling(s);
//        long maxExpire = s.getInfo().getMaxExpireAt();
//        if (s.getInfo().getTotalActiveItems() == 0
//            || (maxExpire < System.currentTimeMillis() && maxExpire > 0)) {
//          stats.totalEmptySegments.incrementAndGet();
//        }
//        try {
//          cleanSegment(s);
//          // Dispose segment
//          engine.disposeDataSegment(s);
//        } catch (IOException e) {
//          /*DEBUG*/ e.printStackTrace();
//          LOG.error("Cache : %s segment id=%d offheap =%s sealed=%s", cache.getName(), 
//            s == null? -1: s.getId(), s==null? null: Boolean.toString(s.isOffheap()), 
//                s == null? null: Boolean.toString(s.isSealed()));
//          LOG.error(e);
//          return;
//        }
//        // Update admission controller statistics
//        engine.finishRecycling(s);
//      }
//    } finally {
//      numInstances.decrementAndGet();
//      //TODO: uncomment
//      this.cache.finishScavenger(this);
//    }
//    long runEnd = System.currentTimeMillis();
//    // Update stats
//    stats.totalRunTimes.addAndGet(runEnd - runStart);
//    LOG.debug(
//        "scavenger [{}] finished at {} allocated storage={} maximum storage=%{}", cache.getName(),
//        format.format(new Date()), engine.getStorageAllocated(), engine.getMaximumStorageSize());
//
//  }
//
//  protected boolean shouldStopOn(Segment s) {
//    long expire = s.getInfo().getMaxExpireAt();
//    long n = s.getAliveItems();
//    long currentTime = System.currentTimeMillis();
//    IOEngine engine = cache.getEngine();
//    if (engine.size() == 0) {
//      return true;
//    }
//    if ((expire > 0 && expire <= currentTime) || n == 0) {
//      return false;
//    }
////    double sratio = (double) s.getAliveItems() / s.getTotalItems();    
////    double minActiveRatio = config.getMinimumActiveDatasetRatio(cache.getName());
// 
//    double usage = engine.getStorageAllocatedRatio();
////    double activeRatio = engine.activeSizeRatio();
////    if (!evictionDisabledMode) {
////      if (sratio >= minActiveRatio) {
////        cleanDeletedOnly = false;
////      } else {
////        cleanDeletedOnly = true;
////      }
////      cleanDeletedOnly = cleanDeletedOnly && usage < stopRatio;
////    }
//    LOG.debug("should stop segment {} usage {} stopRatio{}", s.getId(), usage, stopRatio);
////    System.out.printf("should stop segment %d usage %f stopRatio %f dumpBelowRatio %f\n", 
////      s.getId(), usage, stopRatio, dumpBelowRatio);
//
//    return /*activeRatio >= minActiveRatio && */usage < stopRatio;   
//  }
//
//  protected boolean cleanSegment(Segment s) throws IOException {
//
//    Segment.Info info = s.getInfo();
//    long currentTime = System.currentTimeMillis();
//    long maxExpire = info.getMaxExpireAt();
//    boolean allExpired = maxExpire > 0 && maxExpire <= currentTime;
//    boolean empty = !allExpired && info.getTotalActiveItems() == 0;
//    if (allExpired || empty) {
//      // We can dump it completely w/o asking memory index
//      long dataSize = info.getSegmentDataSize();
//      // Update stats
//      stats.totalBytesFreed.addAndGet(dataSize);
//      stats.totalBytesScanned.addAndGet(dataSize);
//      stats.totalItemsFreed.addAndGet(s.getTotalItems());
//      stats.totalItemsScanned.addAndGet(s.getTotalItems());
//      return false; // not finished yet
//    } else {
//      return cleanSegmentInternal(s);
//    }
//  }
//
//  protected byte[] checkBuffer(byte[] buffer, int requiredSize, boolean isDirect) {
//    if (isDirect) {
//      return buffer;
//    }
//    if (buffer.length < requiredSize) {
//      buffer = new byte[requiredSize];
//    }
//    return buffer;
//  }
//    
//  protected boolean cleanSegmentInternal(Segment s) throws IOException {
//    IOEngine engine = this.cache.getEngine();
//    MemoryIndex index = engine.getMemoryIndex();
//    int groupRank = s.getInfo().getGroupRank();
//    SegmentScanner sc = null;
//    int scanned = 0;
//    int deleted = 0;
//    int expired = 0;
//    int notFound = 0;
//    ResultWithRankAndExpire result = new ResultWithRankAndExpire();
//
//    try {
//      
//      sc = engine.getScanner(s); // acquires read lock
//      boolean isDirect =  sc.isDirect();
//      //TODO Buffer reuse across all scavenger session
//      byte[] keyBuffer = !isDirect? new byte[4096]: null;
//      byte[] valueBuffer = !isDirect? new byte[4096]: null;
//      
//      this.dumpBelowRatio = getCurrentDumpBelowRatio();
//      
//      while (sc.hasNext()) {
//        final long keyPtr = sc.keyAddress();
//        final int keySize = sc.keyLength();
//        final long valuePtr = sc.valueAddress();
//        final int valSize = sc.valueLength();
//        final int totalSize = Utils.kvSize(keySize, valSize);
//        stats.totalBytesScanned.addAndGet(totalSize);
//        double ratio = cleanDeletedOnly? 0: dumpBelowRatio;
//        if (isDirect) {
//          result = index.checkDeleteKeyForScavenger(keyPtr, keySize, result, ratio);
//        } else {
//          keyBuffer = checkBuffer(keyBuffer, keySize, isDirect);
//          sc.getKey(keyBuffer, 0);
//          result = index.checkDeleteKeyForScavenger(keyBuffer, 0, keySize, result, ratio);
//        }
//
//        Result res = result.getResult();
//        int rank = result.getRank();
//        long expire = result.getExpire();
//        scanned++;
//        switch (res) {
//          
//          case EXPIRED:
//            stats.totalBytesExpired.addAndGet(totalSize);
//            stats.totalBytesFreed.addAndGet(totalSize);
//            stats.totalItemsExpired.incrementAndGet();
//            expired++;
//            break;
//          case NOT_FOUND:
//            stats.totalBytesFreed.addAndGet(totalSize);
//            notFound++;
//            break;
//          case DELETED:
//            // Update stats
//            stats.totalBytesFreed.addAndGet(totalSize);
//            // Return Item back to AQ
//            // TODO: fix this code. We need to move data to a victim cache on
//            // memory index eviction.
//            deleted++;
//            break;
//          case OK:
//            // Put value back into the cache - it has high popularity
//            if (isDirect) {
//              this.cache.put(keyPtr, keySize, valuePtr, valSize, expire, rank, groupRank, true, true);
//            } else {
//              valueBuffer = checkBuffer(valueBuffer, valSize, isDirect);
//              sc.getValue(valueBuffer, 0);
//              this.cache.put(keyBuffer, 0, keySize, valueBuffer, 0, valSize, expire, rank, groupRank, true, true);
//            }
//            break;
//        }
//        sc.next();
//      }
//    } finally {
//      if (sc != null) {
//        sc.close();
//      }
//    }
//    this.segmentsScanned ++;
//    // Returns true if could not clean anything
//    // means Scavenger MUST stop and log warning
//    // Mostly for testing - in a real application properly configured
//    // should never happen
//    stats.totalItemsExpired.addAndGet(expired);
//    stats.totalItemsFreed.addAndGet(deleted + expired + notFound);
//    stats.totalItemsScanned.addAndGet(scanned);
//    stats.totalItemsDeleted.addAndGet(deleted);
//    stats.totalItemsNotFound.addAndGet(notFound);
//    LOG.debug("cleanInternal segment {}, scanned {} deleted {} expired {} not found {}", 
//      s.getId(), deleted, expired, notFound);
////    System.out.printf("cleanInternal segment %d, scanned %d deleted %d expired %d not found %d\n", 
////      s.getId(),scanned, deleted, expired, notFound);
//    // Calculate efficiency of garbage collection;
//    double efficiency = (double)(deleted + expired + notFound) / scanned; 
//    if (this.cleanDeletedOnly && efficiency < this.stopRatioForDeletedOnlyMode) {
//      // Disable clean-only-deleted mode
//      this.cleanDeletedOnly = false;
//      this.startRatioForDumpBelowRatioMin = (double) this.segmentsScanned / this.maxSegmentNumber;
//      
//    }
//    return (deleted + expired + notFound) == 0;
//  }
//
// 
//  /**
//   * Get current Scavenger average run rate
//   * @return run rate
//   */
//  public long getScavengerRunRateAverage() {
//    return (long) stats.totalBytesFreed.get() * 1000 / (System.currentTimeMillis() - runStartTime); 
//  }
//
//  public static boolean decreaseThroughput(String cacheName) {
//    Stats stats = statsMap.get(cacheName);
//    if (stats == null) {
//      return false;
//    }
//    if (stats.dumpBelowRatio + stats.adjStep > stats.dumpBelowRatioMax) {
//      return false;
//    }
//    stats.dumpBelowRatio += stats.adjStep;
//    return true;
//  }
//
//  public static boolean increaseThroughput(String cacheName) {
//    Stats stats = statsMap.get(cacheName);
//    if (stats == null) {
//      return false;
//    }
//    if (stats.dumpBelowRatio - stats.adjStep < stats.dumpBelowRatioMin) {
//      return false;
//    }
//    stats.dumpBelowRatio -= stats.adjStep;
//    return true;  
//  }
//  
//  public static void waitForFinish() {
//    while(numInstances.get() > 0) {
//      try {
//        Thread.sleep(100);
//      } catch (InterruptedException e) {
//      }
//    }
//  }
}
