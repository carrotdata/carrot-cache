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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.carrot.cache.index.MemoryIndex;
import com.carrot.cache.io.IOEngine;
import com.carrot.cache.io.Segment;
import com.carrot.cache.io.SegmentScanner;
import com.carrot.cache.util.CacheConfig;
import com.carrot.cache.util.Utils;

public class Scavenger extends Thread {

  /** Logger */
  private static final Logger LOG = LogManager.getLogger(Scavenger.class);
  
  static interface Listener {
    
    /**
     * Reports evicted item 
     * @param key key buffer
     * @param keyOffset key offset
     * @param keySize key size
     * @param value value buffer
     * @param valueOffset value offset
     * @param valueSize
     */
    public void evicted(byte[] key, int keyOffset, int keySize, byte[] value, 
        int valueOffset, int valueSize, long expirationTime);
    
    /**
     * Reports expired key
     * @param key key buffer
     * @param off offset
     * @param keySize key size
     */
    public void expired(byte[] key, int off, int keySize);
    
  }
  static class Stats {

    /** Total times scavenger ran */
    long totalRuns;

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
  }

  private static Stats stats = new Stats();

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
  private volatile long totalFreedBytes;
  
  private Listener aListener;
  
  public Scavenger(Cache parent) {
    super("c2 scavenger");
    this.cache = parent;
    // Update stats
    stats.totalRuns++;
  }

  /**
   * Scavenger listener
   * @param l listener
   */
  public void setListener(Listener l) {
    this.aListener = l;
  }
  
  @Override
  public void run() {
    long runStart = System.currentTimeMillis();
    IOEngine engine = cache.getEngine();
    boolean finished = false;
    while (!finished) {
      Segment s = engine.getMinimumAvgRankSegment();
      if (s == null) {
        LOG.error(Thread.currentThread().getName()+": empty segment");
        return;
      }
      long creationTime = s.getInfo().getCreationTime();
      int rank = s.getInfo().getRank();
      try {
        finished = cleanSegment(s);
        // Release segment
        engine.releaseSegmentId(s);
        
      } catch (IOException e) {
        LOG.error(e);
        return;
      }
      // Update admission controller statistics 
      cache.getAdmissionController().registerSegmentTTL(rank, System.currentTimeMillis() - creationTime);
    }
    long runEnd = System.currentTimeMillis();
    // Update stats
    stats.totalRunTimes += (runEnd - runStart);
  }

  private boolean cleanSegment(Segment s) throws IOException{
    CacheConfig config = this.cache.getCacheConfig();
    String cacheName = this.cache.getName();    
    double stopRatio = config.getScavengerStopMemoryRatio(cacheName);
    double dumpBelowRatio = config.getScavengerDumpEntryBelowStart(cacheName);
    
    IOEngine engine = cache.getEngine();
    MemoryIndex index = engine.getMemoryIndex();

    SegmentScanner sc = engine.getScanner(s);
    
    byte[] key = null, val = null;
    
    while (sc.hasNext()) {
      long expire = sc.getExpire();
      int keySize = sc.keyLength();
      int valSize = sc.valueLength();
      key = checkBuffer(key, keySize);
     
      int keySizeSize = Utils.sizeUVInt(keySize);
      int valSizeSize = Utils.sizeUVInt(valSize);
      int totalSize = keySize + valSize + keySizeSize + valSizeSize;
      // Update stats
      stats.totalBytesScanned += totalSize;
      // Read key
      sc.getKey(key);
      // Check if it was expired
      boolean expired = expire > 0 && (System.currentTimeMillis() > expire);
      double p = index.popularity(key, 0, keySize);

      if (expired) {
        // Expire current item
        cache.reportUsed(-totalSize);
        totalFreedBytes += totalSize;
        // Update stats
        stats.totalBytesExpired += totalSize;
        stats.totalBytesFreed += totalSize;
        stats.totalItemsExpired += 1;
        // Report expiration
        if (aListener != null) {
          aListener.expired(key, 0, keySize);
        }
        // Delete from the index
        index.delete(key, 0, keySize);
      } else if (p < dumpBelowRatio) {
        // Dump current item as a low value
        cache.reportUsed(-totalSize);
        totalFreedBytes += totalSize;
        // Update stats
        stats.totalBytesFreed += totalSize;
        val = checkBuffer(val, valSize);
        sc.getValue(val);
        // Return Item back to AQ
        //TODO: fix this code. We need to move data to a victim cache on 
        // memory index eviction.
        if (aListener != null) {
          if (p > 0.0) {
            aListener.evicted(key, 0, keySize, val, 0, valSize, expire);
          } else {
            // p == 0.0 means, that item was previously evicted from the index
            aListener.evicted(key, 0, keySize, null, 0, 0, expire);
          }
        }
        // Delete from the index
        index.delete(key, 0, keySize);
      } else {
        // Read value
        val = checkBuffer(val, valSize);
        sc.getValue(val);
        // Delete from the index
        index.delete(key, 0, keySize);
        int rank = engine.popularityToRank(p);
        // Otherwise reinsert item back
        engine.put(key, 0, keySize, val, 0, valSize, expire, rank);
      }
      sc.next();
    }
    
    double usage = cache.getMemoryUsedPct();
    return usage < stopRatio;
  }
  
  /**
   * Check that memory is at least requiredSize
   * @param ptr memory buffer address
   * @param size current allocated size
   * @param requiredSize required size
   * @return new buffer address (or old one)
   */
  private byte[] checkBuffer(byte[] buf, int requiredSize) {
    if (buf != null && buf.length >= requiredSize) {
      return buf;
    }
    buf = new byte[requiredSize];
    return buf; 
  }

  /**
   * Get Scavenger rate
   *
   * @return rate
   */
  public double scavengingRate() {
    return (double) totalFreedBytes * 1000 / (System.currentTimeMillis() - runStartTime);
  }
}
