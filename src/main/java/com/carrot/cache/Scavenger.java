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
    
    /**
     * Reports evicted item 
     * @param keyPtr key address
     * @param keySize key size
     * @param valuePtr value address
     * @param valueSize value size
     */
    public void evicted(long keyPtr, int keySize, long valuePtr, int valueSize, long expirationTime);
    
    /**
     * Reports expired key
     * @param keyPtr key address
     * @param keySize key size
     */
    public void expired(long keyPtr, int keySize);
    
  }
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
      Segment s = engine.getSegmentForRecycling();
      if (s == null) {
        LOG.error(Thread.currentThread().getName()+": empty segment");
        return;
      }
      if (s.getInfo().getTotalItems() == 0) {
        stats.totalEmptySegments ++;
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

  // TODO: refactor this method
  private boolean cleanSegment(Segment s) throws IOException {
    // Check if segment is empty on active items
    CacheConfig config = this.cache.getCacheConfig();
    String cacheName = this.cache.getName();
    double stopRatio = config.getScavengerStopMemoryRatio(cacheName);
    Segment.Info info = s.getInfo();
    SegmentScanner sc = null;
    long indexBuf = 0;
    int sid = s.getId();
    try {
      if (info.getTotalItems() == 0) {
        // We can dump it completely w/o asking memory index
        //
        long dataSize = info.getSegmentDataSize();
        cache.reportUsed(-dataSize);
        totalFreedBytes += dataSize;
        // Update stats
        stats.totalBytesFreed += dataSize;
      } else {
        double dumpBelowRatio = config.getScavengerDumpEntryBelowStart(cacheName);
        IOEngine engine = cache.getEngine();
        MemoryIndex index = engine.getMemoryIndex();
        IndexFormat format = index.getIndexFormat();
        int indexEntrySize = format.indexEntrySize();
        indexBuf = UnsafeAccess.malloc(indexEntrySize);

        sc = engine.getScanner(s);

        while (sc.hasNext()) {
          long keyPtr = sc.keyAddress();
          int keySize = sc.keyLength();
          long offset = sc.getOffset();
          boolean exists = index.exists(keyPtr, keySize, sid, offset, indexBuf, sid);
          long expire = exists ? format.getExpire(indexBuf, indexEntrySize) : 0;
          long valuePtr = sc.valueAddress();
          int valSize = sc.valueLength();

          int keySizeSize = Utils.sizeUVInt(keySize);
          int valSizeSize = Utils.sizeUVInt(valSize);
          int totalSize = keySize + valSize + keySizeSize + valSizeSize;
          // Update stats
          stats.totalBytesScanned += totalSize;

          // Check if it was expired
          boolean expired = expire > 0 && (System.currentTimeMillis() > expire);
          double p = index.popularity(keyPtr, keySize);

          if (expired || !exists) {
            // Expire current item
            cache.reportUsed(-totalSize);
            totalFreedBytes += totalSize;
            // Update stats
            stats.totalBytesExpired += totalSize;
            stats.totalBytesFreed += totalSize;
            stats.totalItemsExpired += 1;
            // Report expiration
            if (aListener != null && expired) {
              aListener.expired(keyPtr, keySize);
            }
            if (exists) {
              // Delete from the index
              index.delete(keyPtr, keySize);
            }
          } else if (p < dumpBelowRatio) {
            // Dump current item as a low value
            cache.reportUsed(-totalSize);
            totalFreedBytes += totalSize;
            // Update stats
            stats.totalBytesFreed += totalSize;
            // Return Item back to AQ
            // TODO: fix this code. We need to move data to a victim cache on
            // memory index eviction.
            if (aListener != null) {
              if (p > 0.0) {
                aListener.evicted(keyPtr, keySize, valuePtr, valSize, expire);
              } else {
                // p == 0.0 means, that item was previously evicted from the index
                aListener.evicted(keyPtr, keySize, 0L, 0, expire);
              }
            }
            // Delete from the index
            index.delete(keyPtr, keySize);
          } else {
            // Delete from the index
            index.delete(keyPtr, keySize);
            int rank = engine.popularityToRank(p);
            // Otherwise reinsert item back
            engine.put(keyPtr, keySize, valuePtr, valSize, expire, rank);
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
    return (long) totalFreedBytes * 1000 / (System.currentTimeMillis() - runStartTime); 
  }
}
