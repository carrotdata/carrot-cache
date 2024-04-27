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
package com.onecache.core;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.onecache.core.index.MemoryIndex;
import com.onecache.core.index.MemoryIndex.Result;
import com.onecache.core.index.MemoryIndex.ResultWithRankAndExpire;
import com.onecache.core.io.IOEngine;
import com.onecache.core.io.Segment;
import com.onecache.core.io.SegmentScanner;
import com.onecache.core.util.CacheConfig;
import com.onecache.core.util.Persistent;
import com.onecache.core.util.Utils;

public class Scavenger implements Runnable {

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
  private static final Logger LOG = LoggerFactory.getLogger(Scavenger.class);
  
  public static class Stats implements Persistent {
    
    /** Cache name */
    String cacheName;
    
    /** Total times scavenger ran */
    AtomicLong totalRuns = new AtomicLong();

    /** Total empty segments */
    AtomicLong totalEmptySegments = new AtomicLong();
    
    /** Total run time in ms; */
    AtomicLong totalRunTimes= new AtomicLong();

    /** Total items scanned */
    AtomicLong totalItemsScanned = new AtomicLong();

    /** Total items freed */
    AtomicLong totalItemsFreed = new AtomicLong();

    /** Total items expired */
    AtomicLong totalItemsExpired = new AtomicLong();

    /** Total items scanned */
    AtomicLong totalItemsDeleted = new AtomicLong();

    /** Total items freed */
    AtomicLong totalItemsNotFound = new AtomicLong();
    
    /** Total bytes scanned */
    AtomicLong totalBytesScanned = new AtomicLong();

    /** Total bytes freed */
    AtomicLong totalBytesFreed = new AtomicLong();

    /** Total bytes expired */
    AtomicLong totalBytesExpired = new AtomicLong();

    double dumpBelowRatioMin;
    
    double dumpBelowRatioMax;
    
    double dumpBelowRatio;

    double adjStep;
    
    double stopRatio;
    
    Stats(String cacheName) {
      this.cacheName = cacheName;
      CacheConfig conf = CacheConfig.getInstance();
      dumpBelowRatio = conf.getScavengerDumpEntryBelowMax(cacheName);
      dumpBelowRatioMin = dumpBelowRatio;
      dumpBelowRatioMax = conf.getScavengerDumpEntryBelowMin(cacheName);
      adjStep = conf.getScavengerDumpEntryBelowAdjStep(cacheName);
      stopRatio =  conf.getScavengerStopMemoryRatio(cacheName);
    }
    
    /**
     * Get total runs
     *
     * @return total runs
     */
    public long getTotalRuns() {
      return totalRuns.get();
    }

    /**
     * Get total empty segments
     * @return total number of empty segments
     */
    public long getTotalEmptySegments() {
      return totalEmptySegments.get();
    }
    
    /**
     * Get total run time
     *
     * @return total run time
     */
    public long getTotalRunTimes() {
      return totalRunTimes.get();
    }

    /**
     * Get total items scanned
     *
     * @return total items scanned
     */
    public long getTotalItemsScanned() {
      return totalItemsScanned.get();
    }

    /**
     * Get total items freed
     *
     * @return total items freed
     */
    public long getTotalItemsFreed() {
      return totalItemsFreed.get();
    }

    /**
     * Get total items expired
     *
     * @return total items expired
     */
    public long getTotalItemsExpired() {
      return totalItemsExpired.get();
    }

    /**
     * Get total items deleted
     *
     * @return total items deleted
     */
    public long getTotalItemsDeleted() {
      return totalItemsDeleted.get();
    }

    /**
     * Get total items not found
     *
     * @return total items not found
     */
    public long getTotalItemsNotFound() {
      return totalItemsNotFound.get();
    }
    /**
     * Get total bytes scanned
     *
     * @return total bytes scanned
     */
    public long getTotalBytesScanned() {
      return totalBytesScanned.get();
    }

    /**
     * Get total bytes freed
     *
     * @return total bytes freed
     */
    public long getTotalBytesFreed() {
      return totalBytesFreed.get();
    }

    /**
     * Get total bytes expired
     *
     * @return total bytes expired
     */
    public long getTotalBytesExpired() {
      return totalBytesExpired.get();
    }
    
    @Override
    public void save(OutputStream os) throws IOException {
      DataOutputStream dos = Utils.toDataOutputStream(os);
      dos.writeUTF(cacheName);
      dos.writeLong(totalBytesExpired.get());
      dos.writeLong(totalBytesFreed.get());
      dos.writeLong(totalBytesScanned.get());
      dos.writeLong(totalEmptySegments.get());
      dos.writeLong(totalItemsExpired.get());
      dos.writeLong(totalItemsFreed.get());
      dos.writeLong(totalItemsScanned.get());
      dos.writeLong(totalItemsDeleted.get());
      dos.writeLong(totalItemsNotFound.get());

      dos.writeLong(totalRuns.get());
      dos.writeLong(totalRunTimes.get());
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
      totalBytesExpired.set(dis.readLong());
      totalBytesFreed.set(dis.readLong());
      totalBytesScanned.set(dis.readLong());
      totalEmptySegments.set(dis.readLong());
      totalItemsExpired.set(dis.readLong());
      totalItemsFreed.set(dis.readLong());
      totalItemsScanned.set(dis.readLong());
      totalItemsDeleted.set(dis.readLong());
      totalItemsNotFound.set(dis.readLong());
      
      totalRuns.set(dis.readLong());
      totalRunTimes.set(dis.readLong());
      dumpBelowRatioMin = dis.readDouble();
      dumpBelowRatioMax = dis.readDouble();
      dumpBelowRatio = dis.readDouble();
      adjStep = dis.readDouble();
      stopRatio = dis.readDouble();
    }
  }

  /**
   * Thread pool to run scavenger tasks
   */
  private static ConcurrentHashMap<String, ExecutorService> poolMap = 
      new ConcurrentHashMap<String, ExecutorService>();
  
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
  
  private static void initPoolForCacheName(final String cacheName) {
    // This method is called under parent cache lock
    if (poolMap.contains(cacheName)) {
      return;
    }
    CacheConfig config = CacheConfig.getInstance();
    int numThreads = config.getScavengerNumberOfThreads(cacheName);
    ExecutorService service = Executors.newFixedThreadPool(numThreads, new ThreadFactory() {

      @Override
      public Thread newThread(Runnable r) {
        String name = NAME + "-" + cacheName + "-" + rollingId.getAndIncrement();
        Thread t = new Thread(r);
        t.setName(name);
        return t;
      }
      
    });
    poolMap.put(cacheName, service);
  }
  
  private static void shutdownPools() {
    for (String name: poolMap.keySet()) {
      safeShutdown(name);
    }
  }
  
  public static void safeShutdown(String cacheName) {
    ExecutorService es = poolMap.get(cacheName);
    es.shutdownNow();
    boolean terminated = false;
    while (!terminated) {
      try {
        terminated = es.awaitTermination(10, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        //
      }
    }
  }
  
  public static void printStats() {
    for(Map.Entry<String,Stats> entry: statsMap.entrySet()) {
      String name = entry.getKey();
      Stats stats = entry.getValue();
      LOG.info("Scavenger [{}]: runs={} scanned={} freed={} written back={} empty segments={}", 
        name, stats.getTotalRuns(), stats.getTotalBytesScanned(), 
        stats.getTotalBytesFreed(), stats.getTotalBytesScanned() - stats.getTotalBytesFreed(),
        stats.getTotalEmptySegments());
    }
  }
  
  public final static String NAME = "occ-scavenger";
  
  private Stats stats;
  
  private final Cache cache;
  
  private final CacheConfig config;

  private volatile long runStartTime = System.currentTimeMillis();

  private double dumpBelowRatioMin;
  
  private double dumpBelowRatioMax;
  
  private double dumpBelowRatio = -1;

  private double adjStep = 0.05;
  
  private double stopRatio;
  
  private static Map<String, AtomicInteger> numInstancesMap = new ConcurrentHashMap<String, AtomicInteger> ();
  
  private static int maxInstances = 1; // TODO: make it configurable
  
  private static AtomicLong rollingId = new AtomicLong();
  
  /* Clean deleted only items - do not purge low ranks*/
  private boolean beforeStallDetected = true;
  
  private long maxSegmentsBeforeStallDetected;
    
  public Scavenger(Cache cache) {
    //super(NAME + rollingId.getAndIncrement());
    this.cache = cache;
    this.config = CacheConfig.getInstance();
    String cacheName = this.cache.getName();
    maxInstances = config.getScavengerNumberOfThreads(cacheName);
    // Update stats
    stats = statsMap.get(cache.getName());
    
    if (stats == null) {
      stats = new Stats(cache.getName());
      statsMap.put(cache.getName(), stats);
      dumpBelowRatio = this.config.getScavengerDumpEntryBelowMin(cacheName);
      dumpBelowRatioMin = dumpBelowRatio;
      dumpBelowRatioMax = this.config.getScavengerDumpEntryBelowMax(cacheName);
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
    stats.totalRuns.incrementAndGet();
    
  }
  
  public static void registerCache(String cacheName) {
    numInstancesMap.put(cacheName,  new AtomicInteger());
    initPoolForCacheName(cacheName);
  }
  
  public static int getActiveThreadsCount(String cacheName) {
    return numInstancesMap.get(cacheName).get();
  }
  
  private long getScavengerMaxSegmentsBeforeStall() {
    
    long maxCacheSize = this.cache.getMaximumCacheSize();
    long segmentSize = this.cache.getCacheConfig().getCacheSegmentSize(cache.getName());
    long maxSegments = maxCacheSize / segmentSize;
    double startRatio = this.cache.getCacheConfig().getScavengerStartMemoryRatio(cache.getName());
    double rangeToScan = startRatio - stopRatio;
    // TODO on this value. We expect that 50% of segment data will be cleaned on average
    return (int) (2 * maxSegments * rangeToScan);
    
  }
  
  public void start() {
    ExecutorService service = poolMap.get(this.cache.getName());
    service.submit(this);
  }
  
  /**
   * Used for testing
   */
  public static void clear() {
    statsMap.clear();
    numInstancesMap.clear();
    shutdownPools();
  }

  @Override
  public void run() {
    long runStart = System.currentTimeMillis();
    IOEngine engine = this.cache.getEngine();
    // DateFormat format = DateFormat.getDateTimeInstance();
    Segment s = null;
    try {
      AtomicInteger numInstances = numInstancesMap.get(cache.getName());
      if (numInstances.incrementAndGet() > maxInstances) {
        // Number of instances exceeded the maximum value
        //numInstances.decrementAndGet();
        return;
      }
      // LOG.debug(
      // "scavenger [{}] started at {} allocated storage={} maximum storage={}", cache.getName(),
      // format.format(new Date()), engine.getStorageAllocated(), engine.getMaximumStorageSize());

      boolean finished = false;

      int segmentsProcessed = 0;

      while (!finished) {
        if (Thread.interrupted()) {
          break;
        }
        s = engine.getSegmentForRecycling();
        if (s == null) {
          break;
        }
        if (shouldStopOn(s)) {
          s.setRecycling(false);
          break;
        }
        // TODO: make it more smarter, calculate maxSegmentsBeforeStallDetected using maximum number
        // of segments
        // and other current cache configuration values
        if (segmentsProcessed >= maxSegmentsBeforeStallDetected) {
          // This should never happen if eviction is disabled
          dumpBelowRatio = dumpBelowRatioMax;// improve scavenger's performance - dump everything
          beforeStallDetected = false;
        }
        engine.startRecycling(s);
        long maxExpire = s.getInfo().getMaxExpireAt();
        if (s.getInfo().getTotalActiveItems() == 0
            || (maxExpire < System.currentTimeMillis() && maxExpire > 0)) {
          stats.totalEmptySegments.incrementAndGet();
        }
        try {
          finished = cleanSegment(s);
          if (finished && beforeStallDetected) {
            // continue with purging low rank elements
            finished = false;
          } else if (finished && !shouldStopOn(s)) {
            finished = false;
            dumpBelowRatio = 1.0; // dump everything
          }
          // Dispose segment
          engine.disposeDataSegment(s);

        } catch (IOException e) {
          LOG.error("Cache : {} segment id={} offheap ={} sealed={}", cache.getName(),
            s == null ? -1 : s.getId(), s == null ? null : Boolean.toString(s.isOffheap()),
            s == null ? null : Boolean.toString(s.isSealed()));
          LOG.error("Error:", e);
          e.printStackTrace();
          return;
        }
        // Update admission controller statistics
        engine.finishRecycling(s);
        segmentsProcessed++;
      }
    } catch (Exception e) {
      /* DEBUG */e.printStackTrace();
    } finally {
      AtomicInteger numInstances = numInstancesMap.get(cache.getName());
      numInstances.decrementAndGet();
      this.cache.finishScavenger(this);
    }
    long runEnd = System.currentTimeMillis();
    // Update stats
    stats.totalRunTimes.addAndGet(runEnd - runStart);
    // LOG.debug(
    // "scavenger [{}] finished at {} allocated storage={} maximum storage=%{}", cache.getName(),
    // format.format(new Date()), engine.getStorageAllocated(), engine.getMaximumStorageSize());

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
 
    double usage = engine.getStorageAllocatedRatio();
//    double activeRatio = engine.activeSizeRatio();
//    if (!evictionDisabledMode) {
//      if (sratio >= minActiveRatio) {
//        cleanDeletedOnly = false;
//      } else {
//        cleanDeletedOnly = true;
//      }
//      cleanDeletedOnly = cleanDeletedOnly && usage < stopRatio;
//    }
    
    return /*activeRatio >= minActiveRatio && */usage < stopRatio;   
  }

  private boolean cleanSegment(Segment s) throws IOException {

    Segment.Info info = s.getInfo();
    long currentTime = System.currentTimeMillis();
    long maxExpire = info.getMaxExpireAt();
    boolean allExpired = maxExpire > 0 && maxExpire <= currentTime;
    boolean empty = !allExpired && info.getTotalActiveItems() == 0;
    boolean result = false;
    long dataSize = info.getSegmentDataSize();
    long uncompressedDataSize = info.getSegmentDataSizeUncompressed();
    if (allExpired || empty) {
      // We can dump it completely w/o asking memory index
      // Update stats
      stats.totalBytesFreed.addAndGet(dataSize);
      stats.totalBytesScanned.addAndGet(dataSize);
      stats.totalItemsFreed.addAndGet(s.getTotalItems());
      stats.totalItemsScanned.addAndGet(s.getTotalItems());
      result = false; // not finished yet
    } else {
      result = cleanSegmentInternal(s);
    }
    this.cache.getEngine().reportRawDataSize(-uncompressedDataSize);
    this.cache.getEngine().reportStorageUsed(-dataSize);
    return result;
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
    
  @SuppressWarnings("unused")
  private boolean cleanSegmentInternal(Segment s) throws IOException {
    IOEngine engine = this.cache.getEngine();
    MemoryIndex index = engine.getMemoryIndex();
    int groupRank = s.getInfo().getGroupRank();
    SegmentScanner sc = null;
    int scanned = 0;
    int deleted = 0;
    int expired = 0;
    int notFound = 0;
    int submitted = 0;
    ResultWithRankAndExpire result = new ResultWithRankAndExpire();
    try {

      sc = engine.getScanner(s); // acquires read lock
      boolean isDirect = sc.isDirect();
      // TODO Buffer reuse across all scavenger session
      byte[] keyBuffer = !isDirect ? new byte[4096] : null;
      byte[] valueBuffer = !isDirect ? new byte[4096] : null;
      while (sc.hasNext()) {
        final long keyPtr = sc.keyAddress();
        final int keySize = sc.keyLength();
        final long valuePtr = sc.valueAddress();
        final int valSize = sc.valueLength();
        final int totalSize = Utils.kvSize(keySize, valSize);
        stats.totalBytesScanned.addAndGet(totalSize);
        double ratio = dumpBelowRatio; // beforeStallDetected? 0: dumpBelowRatio;
        if (isDirect) {
          result = index.checkDeleteKeyForScavenger(s.getId(), keyPtr, keySize, result, ratio);
        } else {
          keyBuffer = checkBuffer(keyBuffer, keySize, isDirect);
          sc.getKey(keyBuffer, 0);
          result =
              index.checkDeleteKeyForScavenger(s.getId(), keyBuffer, 0, keySize, result, ratio);
        }
        Result res = result.getResult();
        int rank = result.getRank();
        long expire = result.getExpire();
        scanned++;
        switch (res) {

          case EXPIRED:
            stats.totalBytesExpired.addAndGet(totalSize);
            stats.totalBytesFreed.addAndGet(totalSize);
            stats.totalItemsExpired.incrementAndGet();
            expired++;
            break;
          case NOT_FOUND:// Actually deleted or overwritten
            stats.totalBytesFreed.addAndGet(totalSize);
            notFound++;
            break;
          case DELETED:
            // Update stats
            stats.totalBytesFreed.addAndGet(totalSize);
            // Return Item back to AQ
            // TODO: fix this code. We need to move data to a victim cache on
            // memory index eviction.
            deleted++;
            break;
          case OK:
            submitted++;
            break;
        }
        Cache c = res == Result.OK ? this.cache : this.cache.getVictimCache();
        // In case of OK resubmit back to the cache, in case of DELETED and victim cache is not null
        // submit to victim cache
        // sanity check
        if (c != null && (res == Result.OK || res == Result.DELETED)) {
          // Put value back into the cache or victim cache - it has high popularity
          if (isDirect) {
            c.put(keyPtr, keySize, valuePtr, valSize, expire, rank, groupRank, true, true);
          } else {
            valueBuffer = checkBuffer(valueBuffer, valSize, isDirect);
            sc.getValue(valueBuffer, 0);
            c.put(keyBuffer, 0, keySize, valueBuffer, 0, valSize, expire, rank, groupRank, true,
              true);
          }
        }
        // Update storage usage (uncompressed)
        int kvSize = Utils.kvSize(keySize, valSize);
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
    stats.totalItemsExpired.addAndGet(expired);
    stats.totalItemsFreed.addAndGet(deleted + expired + notFound);
    stats.totalItemsScanned.addAndGet(scanned);
    stats.totalItemsDeleted.addAndGet(deleted);
    stats.totalItemsNotFound.addAndGet(notFound);
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
        stats.totalBytesScanned.addAndGet(totalSize);
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
    return (long) stats.totalBytesFreed.get() * 1000 / (System.currentTimeMillis() - runStartTime); 
  }

  
  @SuppressWarnings("unused")
  private void debugSave(Segment s) throws IOException {
    File f = new File("./fault_segment.data");
    FileOutputStream fos = null;
    try {
      fos = new FileOutputStream(f);
      s.save(fos);
    } catch (FileNotFoundException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } finally {
      fos.close();
    }
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

  public static void waitForFinish() {
    for (AtomicInteger numInstances : numInstancesMap.values()) {
      while (numInstances.get() > 0) {
        try {
          Thread.sleep(100);
        } catch (InterruptedException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }
    }
  }
}
