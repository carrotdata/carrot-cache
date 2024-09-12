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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.impl.Log4jContextFactory;
import org.apache.logging.log4j.core.util.ShutdownCallbackRegistry;
import org.apache.logging.log4j.core.util.DefaultShutdownCallbackRegistry;

import org.apache.logging.log4j.spi.LoggerContextFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotdata.cache.Scavenger.Stats;
import com.carrotdata.cache.compression.CodecFactory;
import com.carrotdata.cache.compression.CompressionCodec;
import com.carrotdata.cache.controllers.AdmissionController;
import com.carrotdata.cache.controllers.PromotionController;
import com.carrotdata.cache.controllers.ThroughputController;
import com.carrotdata.cache.eviction.EvictionListener;
import com.carrotdata.cache.index.IndexFormat;
import com.carrotdata.cache.index.MemoryIndex;
import com.carrotdata.cache.io.CacheInputStream;
import com.carrotdata.cache.io.CacheOutputStream;
import com.carrotdata.cache.io.CompressedBlockBatchDataWriter;
import com.carrotdata.cache.io.CompressedBlockFileDataReader;
import com.carrotdata.cache.io.CompressedBlockMemoryDataReader;
import com.carrotdata.cache.io.FileIOEngine;
import com.carrotdata.cache.io.IOEngine;
import com.carrotdata.cache.io.IOEngine.IOEngineEvent;
import com.carrotdata.cache.io.MemoryIOEngine;
import com.carrotdata.cache.io.Segment;
import com.carrotdata.cache.io.SegmentScanner;
import com.carrotdata.cache.jmx.CacheJMXSink;
import com.carrotdata.cache.util.CacheConfig;
import com.carrotdata.cache.util.Epoch;
import com.carrotdata.cache.util.UnsafeAccess;
import com.carrotdata.cache.util.Utils;

/**
 * Main entry for off-heap/on-disk cache
 */
public class Cache implements IOEngine.Listener, EvictionListener {

  /** Logger */
  private static Logger LOG = LoggerFactory.getLogger(Cache.class);

  public static enum Type {
    MEMORY, DISK
  }

  /* Total number of accesses (GET) */
  private AtomicLong totalGets = new AtomicLong(0);

  /* Total gets size */
  private AtomicLong totalGetsSize = new AtomicLong(0);

  /* Total hits */
  private AtomicLong totalHits = new AtomicLong(0);

  /* Total writes */
  private AtomicLong totalWrites = new AtomicLong(0);

  /* Total writes size */
  private AtomicLong totalWritesSize = new AtomicLong(0);

  /* Total rejected writes */
  private AtomicLong totalRejectedWrites = new AtomicLong(0);

  /* Number of active requests */
  private AtomicInteger activeRequests = new AtomicInteger();
  
  /* Cache name */
  String cacheName;

  /** Cache configuration */
  CacheConfig conf;

  /** Maximum memory limit in bytes */
  long maximumCacheSize;

  /** Maximum key-value size to cache */
  int maximumKeyValueSize;

  /* IOEngine */
  IOEngine engine;

  /* Admission Controller - optional */
  AdmissionController admissionController;

  /* Promotion controller (from victim cache back to parent)- optional */
  PromotionController promotionController;

  /* Throughput controller - optional */
  ThroughputController throughputController;

  /* Periodic task runner */
  Timer timer;

  /* Victim cache */
  Cache victimCache;

  /* Parent cache */
  Cache parentCache;

  /* Cache epoch */
  Epoch epoch;

  Thread shutDownHook = new Thread(() -> {
    try {
      shutdown();
    } catch (IOException e) {
      LOG.error("Error:", e);
    }
  });

  /* Throughput controller enabled */
  boolean tcEnabled;

  /* Index embedding supported */
  boolean indexEmdeddingSupported;

  /* Index embedded size */
  int indexEmbeddedSize;

  /* Eviction disabled mode */
  boolean evictionDisabledMode;

  /* Scavenger start memory ratio */
  double scavengerStartMemoryRatio;

  /* Scavenger stop memory ratio */
  double scavengerStopMemoryRatio;

  /* Victim cache promote on hit */
  boolean victimCachePromoteOnHit;

  /* Victim cache promote threshold */
  double victimCachePromoteThreshold;

  /* Hybrid cache inverse mode */
  boolean hybridCacheInverseMode;

  /* Cache spin wait time */
  long spinWaitTimeNs;

  /** Maximum wait time to complete PUT operation in ms */
  long waitOnPutTimeMs;

  /* Cache type */
  Type type;

  /* shutdown is in progress */
  volatile boolean shutdownInProgress = false;

  /* Thread - local buffer */
  ThreadLocal<byte[]> tlsBuffer;

  /* Thread-local storage enabled */
  boolean tlsEnabled;

  /* TLS buffer maximum size in bytes */
  int tlsBufferMaxSize;

  /* Save cache on shutdown */
  boolean saveOnShutdown;

  /* Scavengers are disabled for safe SAVE operation */
  volatile boolean scavDisabled = false;

  /* Save operation is in progress */
  volatile boolean saveInProgress = false;

  volatile boolean cacheDisabled = false;

  int scavengerNoThreads;

  /*
   *  For testing only
   */
  boolean scavengerDisabled = false;
  
  /**
   * Constructor to use when loading cache from a storage set cache name after that
   */

  public Cache() {
  }

  /**
   * Sets cache name
   * @param name name
   */
  public void setName(String name) {
    this.cacheName = name;
  }

  /**
   * Constructor with cache name
   * @param name cache name
   * @throws IOException
   */
  public Cache(String name) throws IOException {
    this.cacheName = name;
    this.conf = CacheConfig.getInstance();
    preprocessConf();
    this.conf.sanityCheck(cacheName);
    this.engine = IOEngine.getEngineForCache(this);
    // set engine listener
    this.engine.setListener(this);
    this.type = engine instanceof MemoryIOEngine ? Type.MEMORY : Type.DISK;
    initAll();
  }

  private void preprocessConf() throws IOException {
    boolean compEnabled = this.conf.isCacheCompressionEnabled(cacheName);
    if (compEnabled) {
      this.conf.setDataWriter(cacheName, CompressedBlockBatchDataWriter.class.getName());
      this.conf.setMemoryDataReader(cacheName, CompressedBlockMemoryDataReader.class.getName());
      this.conf.setFileDataReader(cacheName, CompressedBlockFileDataReader.class.getName());
      this.conf.setCacheTLSSupported(cacheName, true);
      initCodec();
    }
  }
  
  private void initCodec() throws IOException {
    CodecFactory factory = CodecFactory.getInstance();
    CompressionCodec codec = factory.getCompressionCodecForCache(cacheName);
    if (codec == null) {
      factory.initCompressionCodecForCache(cacheName, null);
    }

  }
  
  public void startStatsTask(long interval) {
      if (timer == null) {
        timer = new Timer();
      }
      TimerTask r = new TimerTask() {
        @Override
        public void run() {
          printStats();
          Scavenger.printStats();
        }
      };
      timer.schedule(r, interval, interval);
  }
  
  public Cache(String name, CacheConfig conf) throws IOException {
    this.cacheName = name;
    this.conf = conf;
    preprocessConf();
    this.conf.sanityCheck(cacheName);
    this.engine = IOEngine.getEngineForCache(this);
    // set engine listener
    this.engine.setListener(this);
    this.type = engine instanceof MemoryIOEngine ? Type.MEMORY : Type.DISK;
    initAll();
  }

  Cache(CacheConfig conf, String cacheName) {
    this.cacheName = cacheName;
    this.conf = conf;
  }

  /**
   * Is memory cache
   * @return true or false
   */
  public boolean isMemoryCache() {
    return this.type == Type.MEMORY;
  }
  
  /**
   * Is disk cache
   * @return true or falsew
   */
  public boolean isDiskCache() {
    return this.type == Type.DISK;
  }
  
  void setIOEngine(IOEngine engine) {
    this.engine = engine;
    // set engine listener
    this.engine.setListener(this);
    this.type = engine instanceof MemoryIOEngine ? Type.MEMORY : Type.DISK;
  }

  private void initFromConfiguration() {

    this.indexEmdeddingSupported = this.conf.isIndexDataEmbeddedSupported(this.cacheName);
    this.indexEmbeddedSize = this.conf.getIndexDataEmbeddedSize(this.cacheName);
    this.evictionDisabledMode = this.conf.getEvictionDisabledMode(this.cacheName);
    this.scavengerStartMemoryRatio = this.conf.getScavengerStartMemoryRatio(this.cacheName);
    this.scavengerStopMemoryRatio = this.conf.getScavengerStopMemoryRatio(this.cacheName);
    this.spinWaitTimeNs = this.conf.getCacheSpinWaitTimeOnHighPressure(this.cacheName);
    this.waitOnPutTimeMs = this.conf.getCacheMaximumWaitTimeOnPut(cacheName);
    this.maximumCacheSize = this.conf.getCacheMaximumSize(cacheName);
    this.maximumKeyValueSize = this.conf.getKeyValueMaximumSize(cacheName);
    this.scavengerNoThreads = this.conf.getScavengerNumberOfThreads(cacheName);

    long segmentSize = this.conf.getCacheSegmentSize(cacheName);
    // check if it is larger than 2GB
    if (segmentSize > (2L * 1024 * 1024 * 1024)) {
      throw new IllegalArgumentException(
          String.format("Data segment size %d exceeds the limit of 2GB", segmentSize));
    }
    int maxSize = 1 << 28; // 256MB - VarINT size coding limit
    int min = Math.min(maxSize, (int) segmentSize);

    if (this.maximumKeyValueSize > min - 8) {
      LOG.warn("Maximum key-value size %d can not exceed 256MB and data segment size %d",
        segmentSize, min);
      this.maximumKeyValueSize = min - 8;
    } else if (this.maximumKeyValueSize == 0) {
      this.maximumKeyValueSize = min - 8;
    }
    this.saveOnShutdown = this.conf.isSaveOnShutdown(cacheName);
    if (this.saveOnShutdown) {
      addShutdownHook();
    }
    initTLS();
    Scavenger.registerCache(cacheName);
  }

  private void initTLS() {
    this.tlsEnabled = this.conf.isCacheTLSSupported(cacheName);
    if (this.tlsEnabled) {
      int size = this.conf.getCacheTLSInitialBufferSize(cacheName);
      this.tlsBufferMaxSize = this.conf.getCacheTLSMaxBufferSize(cacheName);
      byte[] buf = new byte[size];
      this.tlsBuffer = new ThreadLocal<byte[]>();
      this.tlsBuffer.set(buf);
    }
  }

  private byte[] ensureTLSBufferSize(int size) {
    if (!this.tlsEnabled || size > this.tlsBufferMaxSize) {
      return null;
    }
    byte[] buf = this.tlsBuffer.get();
    if (buf.length < size) {
      buf = new byte[size];
      this.tlsBuffer.set(buf);
    }
    return buf;
  }

  void initAll() throws IOException {
    initFromConfiguration();
    initAdmissionController();
    initPromotionController();
    initThroughputController();
    startThroughputController();
    startVacuumCleaner();
  }

  private void initAllDuringLoad() throws IOException {
    initFromConfiguration();
    this.engine = this.type == Type.MEMORY ? new MemoryIOEngine(this.cacheName)
        : new FileIOEngine(this.cacheName);
    // set engine listener
    this.engine.setListener(this);
    initAdmissionController();
    initPromotionController();
    initThroughputController();
  }

  /**
   * Add shutdown hook
   */
  public void addShutdownHook() {
    disableLog4j2ShutdownHook();
    Runtime r = Runtime.getRuntime();
    r.addShutdownHook(this.shutDownHook);
  }

  private void disableLog4j2ShutdownHook() {
    final LoggerContextFactory factory = LogManager.getFactory();
    if (factory instanceof Log4jContextFactory) {
        LOG.info("Deregister Log4j2 shutdown hook");
        Log4jContextFactory contextFactory = (Log4jContextFactory) factory;
        ShutdownCallbackRegistry registry = contextFactory.getShutdownCallbackRegistry();
        ((DefaultShutdownCallbackRegistry) registry).stop();
        LOG.info("Deregistering Log4j2 shutdown hook done.");
    }
  }
  
  /**
   * Remove shutdown hook
   */
  public void removeShutdownHook() {
    Runtime r = Runtime.getRuntime();
    r.removeShutdownHook(shutDownHook);
  }

  public void disableScavengers() {
    this.scavDisabled = true;
  }
  
  
  void startScavengers(boolean wait) {
    startScavengers(wait, false);
  }

  void startScavengers(boolean wait, boolean vacuum) {
    
    if (this.scavDisabled) {
      return;
    }
    if (vacuum || Scavenger.getActiveThreadsCount(this.cacheName) < this.scavengerNoThreads) {
        Scavenger scavenger = new Scavenger(this);
        scavenger.setVacuumMode(vacuum);
        scavenger.start();
        if (wait) {
          LockSupport.parkNanos(50000);
        }
    }
  }
  
  void stopScavengers() {
    Scavenger.safeShutdown(this.cacheName);
    while (Scavenger.getActiveThreadsCount(this.cacheName) > 0) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
      }
    }
  }

  void waitForScavengers() {
    while (Scavenger.getActiveThreadsCount(this.cacheName) > 0) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
      }
    }
  }

  void finishScavenger(Scavenger s) {
    // TODO
  }

  private void adjustThroughput() {
    boolean result = this.throughputController.adjustParameters();
    LOG.info("Adjusted throughput controller =" + result);
    this.throughputController.printStats();
  }

  private void reportThroughputController(long bytes) {
    if (!this.tcEnabled || this.throughputController == null) {
      return;
    }
    this.throughputController.record(bytes);
  }

  /**
   * Initialize admission controller
   * @throws IOException
   */
  private void initAdmissionController() throws IOException {
    try {
      this.admissionController = this.conf.getAdmissionController(cacheName);
    } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
      LOG.error("Error:", e);
      throw new RuntimeException(e);
    }
    if (this.admissionController == null) {
      return;
    }
    this.admissionController.setCache(this);
    LOG.info("Started Admission Controller [{}], cache={}",
      this.admissionController.getClass().getName(), getName());

  }

  /**
   * Initialize admission controller
   * @throws IOException
   */
  private void initPromotionController() throws IOException {
    try {
      this.promotionController = this.conf.getPromotionController(cacheName);
    } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
      LOG.error("Error:", e);
      throw new RuntimeException(e);
    }
    if (this.promotionController == null) {
      return;
    }
    LOG.info("Before Started Promotion Controller [{}]", this.admissionController.getClass().getName());

    this.promotionController.setCache(this);
    LOG.info("Started Promotion Controller [{}]", this.admissionController.getClass().getName());

  }

  /**
   * Initialize throughput controller
   * @throws IOException
   */
  private void initThroughputController() throws IOException {
    try {
      this.throughputController = this.conf.getThroughputController(cacheName);
    } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
      LOG.error("Error:", e);
      throw new RuntimeException(e);
    }

    if (this.throughputController == null) {
      return;
    }
    this.throughputController.setCache(this);

  }

  private void startThroughputController() {
    if (this.throughputController == null) {
      return;
    }
    TimerTask task = new TimerTask() {
      public void run() {
        adjustThroughput();
      }
    };
    if (this.timer == null) {
      this.timer = new Timer();
    }
    long interval = this.conf.getThroughputCheckInterval(this.cacheName);
    this.timer.scheduleAtFixedRate(task, interval, interval);
    LOG.info("Started throughput controller, interval={} sec", interval / 1000);
  }
  
  private void startVacuumCleaner() {
    TimerTask task = new TimerTask() {
      public void run() {
        startScavengers(false, true);
      }
    };
    long interval = this.conf.getVacuumCleanerInterval(this.cacheName);
    if (interval < 0) {
      LOG.info("Vacuum cleaner - disabled");
      return;
    }
    if (this.timer == null) {
      this.timer = new Timer();
    }
    
    this.timer.scheduleAtFixedRate(task, interval, interval);
    LOG.info("Started vacuum cleaner, interval={} sec", interval / 1000);
  
  }

  /**
   * Get cache type
   * @return cache type
   */
  public Type getCacheType() {
    if (this.engine instanceof MemoryIOEngine) {
      return Type.MEMORY;
    } else {
      return Type.DISK;
    }
  }

  /**
   * Get cache name
   * @return cache name
   */
  public String getName() {
    return this.cacheName;
  }

  /**
   * Get cache configuration object
   * @return cache configuration object
   */
  public CacheConfig getCacheConfig() {
    return this.conf;
  }

  /**
   * Get IOEngine
   * @return engine
   */
  public IOEngine getEngine() {
    return this.engine;
  }

  /**
   * Get total used memory (storage)
   * @return used memory
   */
  public long getRawDataSize() {
    return this.engine.getRawDataSize();
  }

  /**
   * Get total used memory (storage) when compression is on
   * @return used memory
   */
  public long getStorageUsedActual() {
    return this.engine.getStorageUsed();
  }

  /**
   * Get total allocated memory
   * @return total allocated memory for storage
   */
  public long getStorageAllocated() {
    return this.engine.getStorageAllocated();
  }

  /**
   * Get total allocated memory: storage + index + memory buffer pool
   * @return total allocated memory
   */
  public long getTotalAllocated() {
    return this.engine.getTotalAllocated();
  }
  
  /**
   * Get total used memory (storage + index + memory buffers) when compression is on
   * @return used memory
   */
  public long getTotalUsed() {
    return this.engine.getTotalUsed();
  }

  /**
   * Total number of cached items (accessible or not)
   * @return number
   */
  public long size() {
    return this.engine.size();
  }

  /**
   * Total number of active items (accessible)
   * @return active number
   */
  public long activeSize() {
    return this.engine.activeSize();
  }

  /**
   * Gets memory limit
   * @return memory limit in bytes
   */
  public long getMaximumCacheSize() {
    return this.maximumCacheSize;
  }

  /**
   * Get memory used as a fraction of memory limit
   * @return memory used fraction
   */
  public double getStorageAllocatedRatio() {
    if (this.maximumCacheSize == 0) return 0;
    return (double) getStorageUsedActual() / this.maximumCacheSize;
  }

  /**
   * Get memory used as a fraction of memory limit
   * @return memory used fraction
   */
  public double getTotalAllocatedRatio() {
    if (this.maximumCacheSize == 0) return 0;
    return (double) getTotalAllocated() / this.maximumCacheSize;
  }
  
  /**
   * Get admission controller
   * @return admission controller
   */
  public AdmissionController getAdmissionController() {
    return this.admissionController;
  }

  /**
   * Sets admission controller
   * @param ac admission controller
   */
  public void setAdmissionController(AdmissionController ac) {
    this.admissionController = ac;
  }

  /**
   * Get total gets
   * @return total gets
   */
  public long getTotalGets() {
    return this.totalGets.get();
  }

  /**
   * Get total failed gets
   * @return total failed gets
   */
  public long getTotalFailedGets() {
    return this.engine.getTotalFailedReads();
  }

  /**
   * Total gets size
   * @return size
   */
  public long getTotalGetsSize() {
    return this.totalGetsSize.get();
  }

  /**
   * Get total hits
   * @return total hits
   */
  public long getTotalHits() {
    return this.totalHits.get();
  }

  /**
   * Get total writes
   * @return total writes
   */
  public long getTotalWrites() {
    return this.totalWrites.get();
  }

  /**
   * Get total writes size
   * @return total writes size
   */
  public long getTotalWritesSize() {
    return this.totalWritesSize.get();
  }

  /**
   * Get total rejected writes
   * @return total rejected writes
   */
  public long getTotalRejectedWrites() {
    return this.totalRejectedWrites.get();
  }

  /**
   * Get cache hit rate
   * @return cache hit rate
   */
  public double getHitRate() {
    return (double) totalHits.get() / totalGets.get();
  }

  /**
   * For hybrid caches
   * @return hybrid cache hit rate
   */
  public double getOverallHitRate() {
    if (this.victimCache == null) {
      return getHitRate();
    }
    return (double) (totalHits.get() + this.victimCache.totalHits.get()) / totalGets.get();
  }

  private void access() {
    this.totalGets.incrementAndGet();
  }

  private void hit(long size) {
    this.totalHits.incrementAndGet();
    this.totalGetsSize.addAndGet(size);

  }

  /***********************************************************
   * Cache API
   */

  /* Put API */

  /**
   * Put item into the cache
   * @param keyPtr key address
   * @param keySize key size
   * @param valPtr value address
   * @param valSize value size
   * @param expire - expiration (0 - no expire)
   * @return true on success, false - otherwise
   */
  public boolean put(long keyPtr, int keySize, long valPtr, int valSize, long expire)
      throws IOException {
    int rank = getDefaultRankToInsert();
    // by default, groupRank == rank
    return put(keyPtr, keySize, valPtr, valSize, expire, rank, rank, false);
  }

  private boolean shouldAdmitToMainQueue(long keyPtr, int keySize, int valueSize, boolean force) {
    if (!force && this.admissionController != null) {
      return this.admissionController.admit(keyPtr, keySize, valueSize);
    }
    return true;
  }
  
  /**
   * Put item into a cache
   * 
   * @param keyPtr key address
   * @param keySize key size
   * @param valPtr value address
   * @param valSize value size
   * @param expire expiration time (absolute) in ms
   * @param rank popularity rank
   * @param groupRank grouping rank
   * @param force, if true - bypass admission controller
   * @return
   * @throws IOException
   */
  public boolean put(long keyPtr, int keySize, long valPtr, int valSize, long expire, int rank,
      int groupRank, boolean force) throws IOException {
    return put(keyPtr, keySize, valPtr, valSize, expire, rank, groupRank, force, false);
  }

  /**
   * Put item into the cache with grouping rank - API for new items
   * @param keyPtr key address
   * @param keySize key size
   * @param valPtr value address
   * @param valSize value size
   * @param expire expiration (0 - no expire)
   * @param groupRank grouping rank
   * @return true on success, false - otherwise
   */
  public boolean putWithGroupRank(long keyPtr, int keySize, long valPtr, int valSize, long expire,
      int groupRank) throws IOException {

    if (cacheDisabled) {
      return false;
    }
    try {
      activeRequests.incrementAndGet();
      int rank = getDefaultRankToInsert();

      if (this.victimCache != null && this.hybridCacheInverseMode) {
        return this.victimCache.put(keyPtr, keySize, valPtr, valSize, expire, rank, groupRank,
          false, false);
      } else {
        return putDirectly(keyPtr, keySize, valPtr, valSize, expire, rank, groupRank, false, false);
      }
    } finally {
      activeRequests.decrementAndGet();
    }
  }

  /**
   * Put item into the cache - API for new items
   * @param keyPtr key address
   * @param keySize key size
   * @param valPtr value address
   * @param valSize value size
   * @param expire expiration (0 - no expire)
   * @param rank popularity rank of the item
   * @param groupRank grouping rank
   * @param force if true - bypass admission controller
   * @return true on success, false - otherwise
   */
  public boolean put(long keyPtr, int keySize, long valPtr, int valSize, long expire, int rank,
      int groupRank, boolean force, boolean scavenger) throws IOException {

    if (cacheDisabled) {
      return false;
    }
    try {
      activeRequests.incrementAndGet();
      if (this.victimCache != null && this.hybridCacheInverseMode) {
        return this.victimCache.put(keyPtr, keySize, valPtr, valSize, expire, rank, groupRank,
          force, scavenger);
      } else {
        return putDirectly(keyPtr, keySize, valPtr, valSize, expire, rank, groupRank, force,
          scavenger);
      }
    } finally {
      activeRequests.decrementAndGet();
    }
  }

  private boolean putDirectly(long keyPtr, int keySize, long valPtr, int valSize, long expire,
      int rank, int groupRank, boolean force, boolean scavenger) throws IOException {

    if (cacheDisabled || isGreaterThanMaxSize(keySize, valSize)) {
      return false;
    }
    if (evictionDisabledMode && storageIsFull(keySize, valSize)) {
      return false;
    }
    if (!scavenger) {
      if (storageIsFull(keySize, valSize)) {
        startScavengers(true);
      }
      maybeWait();
    }
    // Check rank
    checkRank(rank);
    checkRank(groupRank);
    if (!scavenger && !shouldAdmitToMainQueue(keyPtr, keySize, valSize, force)) {
      return false;
    }

    // Adjust rank taking into account item's expiration time
    groupRank = adjustGroupRank(rank, groupRank, expire);
    expire = adjustExpirationTime(expire);
    // Add to the cache
    boolean result = false;
    boolean scavStarted = false;
    long start = System.currentTimeMillis();
    do  { 
      result = engine.put(keyPtr, keySize, valPtr, valSize, expire, rank, groupRank,
        scavenger);
      if (scavenger) {
        //Check scavengers if put failed - its victim cache
        if (!result) {
          startScavengers(false);
        }
        break;
      } else if (result) {
        reportThroughputController(Utils.kvSize(keySize, valSize));
        this.totalWrites.incrementAndGet();
        this.totalWritesSize.addAndGet(Utils.kvSize(keySize, valSize));
        break;
      } else if (!scavStarted){
        startScavengers(true);
        scavStarted = true;
      } else {
        maybeWait();
      }
    } while (!result && System.currentTimeMillis() - start <= this.waitOnPutTimeMs);
    if (!result && !scavenger) {
      this.totalRejectedWrites.incrementAndGet();
    }
    return result;
  }
  
  private void checkRank(int rank) {
    int maxRank = this.engine.getNumberOfRanks();
    if (rank < 0 || rank >= maxRank) {
      throw new IllegalArgumentException(String.format("Items rank %d is illegal", rank));
    }
  }

  private boolean shouldAdmitToMainQueue(byte[] key, int keyOffset, int keySize, int valueSize,
      boolean force) {
    if (!force && this.admissionController != null) {
      return this.admissionController.admit(key, keyOffset, keySize, valueSize);
    }
    return true;
  }

  private int adjustGroupRank(int popularityRank, int groupRank, long expire) {
    if (this.admissionController != null) {
      // Adjust rank taking into account item's expiration time
      groupRank = this.admissionController.adjustRank(popularityRank, groupRank, expire);
    }
    return groupRank;
  }

  private long adjustExpirationTime(long expire) {
    if (this.admissionController != null) {
      expire = this.admissionController.adjustExpirationTime(expire);
    }
    return expire;
  }

  public boolean put(byte[] key, int keyOffset, int keySize, byte[] value, int valOffset,
      int valSize, long expire) throws IOException {
    int rank = getDefaultRankToInsert();
    int groupRank = rank;
    return put(key, keyOffset, keySize, value, valOffset, valSize, expire, rank, groupRank, false,
      false);
  }

  public boolean put(byte[] key, int keyOffset, int keySize, byte[] value, int valOffset,
      int valSize, long expire, boolean force) throws IOException {
    int rank = getDefaultRankToInsert();
    int groupRank = rank;
    return put(key, keyOffset, keySize, value, valOffset, valSize, expire, rank, groupRank, force,
      false);
  }

  public boolean put(byte[] key, int keyOffset, int keySize, byte[] value, int valOffset,
      int valSize, long expire, int rank, boolean force) throws IOException {
    int groupRank = rank;
    return put(key, keyOffset, keySize, value, valOffset, valSize, expire, rank, groupRank, force,
      false);
  }

  /**
   * Put with group rank (group rank - aware AC)
   * @param key
   * @param keyOffset
   * @param keySize
   * @param value
   * @param valOffset
   * @param valSize
   * @param expire
   * @param groupRank
   * @return
   * @throws IOException
   */
  public boolean putWithGroupRank(byte[] key, int keyOffset, int keySize, byte[] value,
      int valOffset, int valSize, long expire, int groupRank) throws IOException {
    int rank = getDefaultRankToInsert();

    return put(key, keyOffset, keySize, value, valOffset, valSize, expire, rank, groupRank, false,
      false);
  }

  /**
   * Put item into the cache
   * @param key key
   * @param keyOffset key offset
   * @param keySize key size
   * @param value value
   * @param valOffset value offset
   * @param valSize value size
   * @param expire - expiration (0 - no expire)
   * @param rank item's popularity rank
   * @param groupRank - group rank
   * @param force if true - bypass admission controller
   * @return true on success, false - otherwise
   */
  public boolean put(byte[] key, int keyOffset, int keySize, byte[] value, int valOffset,
      int valSize, long expire, int rank, int groupRank, boolean force, boolean scavenger)
      throws IOException {

    if (cacheDisabled) {
      return false;
    }
    try {
      activeRequests.incrementAndGet();
      if (this.victimCache != null && this.hybridCacheInverseMode) {
        return this.victimCache.put(key, keyOffset, keySize, value, valOffset, valSize, expire,
          rank, groupRank, force, scavenger);
      } else {
        return putDirectly(key, keyOffset, keySize, value, valOffset, valSize, expire, rank,
          groupRank, force, scavenger);
      }
    } finally {
      activeRequests.decrementAndGet();
    }
  }

  private boolean storageIsFull(int keySize, int valueSize) {
    // OK, eviction is disabled
    // check used and maximum storage size
    long used = this.engine.getTotalUsed();
    long room = this.engine.getSegmentSize() * (this.scavengerNoThreads + 1) + 1;
    int size = Utils.kvSize(keySize, valueSize);
    return used + size > this.maximumCacheSize - room;
  }

  private boolean isGreaterThanMaxSize(int keySize, int valueSize) {
    long size = Utils.kvSizeL(keySize, valueSize);
    boolean res = size > this.maximumKeyValueSize;
    if (res) {
      LOG.warn("Unable to cache. Serialized size {} of key-value exceeds the limit of {}.", size,
        maximumKeyValueSize);
    }
    return res;
  }

  
  private void maybeWait() {
    if (Scavenger.getActiveThreadsCount(this.cacheName) == 0) {
      // If scavengers are not running - return immediately
      return;
    }
    //FIXME: how to make it adapt to a current write load?
    ThreadLocalRandom tlr = ThreadLocalRandom.current();
    if(tlr.nextDouble() > 0.5) {
      LockSupport.parkNanos(50_000);
    }
  }

  private boolean putDirectly(byte[] key, int keyOffset, int keySize, byte[] value, int valOffset,
      int valSize, long expire, int rank, int groupRank, boolean force, boolean scavenger)
      throws IOException {

    if (cacheDisabled || isGreaterThanMaxSize(keySize, valSize)) {
      return false;
    }
    if (evictionDisabledMode && storageIsFull(keySize, valSize)) {
      return false;
    }

    if (!scavenger) {
      if (storageIsFull(keySize, valSize)) {
        startScavengers(true);
      }
      maybeWait();
    }
    if (!scavenger && !shouldAdmitToMainQueue(key, keyOffset, keySize, valSize, force)) {
      return false;
    }

    // Check rank
    checkRank(rank);
    checkRank(groupRank);
    // Adjust rank
    groupRank = adjustGroupRank(rank, groupRank, expire);
    expire = adjustExpirationTime(expire);
    // Add to the cache
    boolean result = false;
    boolean scavStarted = false;
    long start = System.currentTimeMillis();
    do  { 
      result = engine.put(key, keyOffset, keySize, value, valOffset, valSize, expire, rank, groupRank,
        scavenger);
      if (scavenger) {
        //Scavenger failed - its parent's scavenger 
        // check scavengers for victim cache
        if (!result) {
          startScavengers(false);
        }
        break;
      } else if (result) {
        reportThroughputController(Utils.kvSize(keySize, valSize));
        this.totalWrites.incrementAndGet();
        this.totalWritesSize.addAndGet(Utils.kvSize(keySize, valSize));
        break;
      } else if (!scavStarted){
        startScavengers(true);
        scavStarted = true;
      } else {
        maybeWait();
      }
    } while (!result && System.currentTimeMillis() - start <= this.waitOnPutTimeMs);
    if (!result && !scavenger) {
      this.totalRejectedWrites.incrementAndGet();
    }
    return result;
  }

  private boolean put(byte[] buf, int off, long expire) throws IOException {
    int rank = getDefaultRankToInsert();
    int groupRank = rank;
    int keySize = Utils.readUVInt(buf, off);
    int kSizeSize = Utils.sizeUVInt(keySize);
    int valueSize = Utils.readUVInt(buf, off + kSizeSize);
    int vSizeSize = Utils.sizeUVInt(valueSize);
    return putDirectly(buf, off + kSizeSize + vSizeSize, keySize, buf,
      off + kSizeSize + vSizeSize + keySize, valueSize, expire, rank, groupRank, true, false);
  }

  private boolean put(long bufPtr, long expire) throws IOException {
    int rank = getDefaultRankToInsert();
    int groupRank = rank;

    int keySize = Utils.readUVInt(bufPtr);
    int kSizeSize = Utils.sizeUVInt(keySize);
    int valueSize = Utils.readUVInt(bufPtr + kSizeSize);
    int vSizeSize = Utils.sizeUVInt(valueSize);
    return putDirectly(bufPtr + kSizeSize + vSizeSize, keySize,
      bufPtr + kSizeSize + vSizeSize + keySize, valueSize, expire, rank, groupRank, true, false);
  }

  private boolean put(ByteBuffer buf, long expire) throws IOException {
    if (buf.hasArray()) {
      byte[] buffer = buf.array();
      int bufOffset = buf.position();
      return put(buffer, bufOffset, expire);
    } else {
      long ptr = UnsafeAccess.address(buf);
      int off = buf.position();
      return put(ptr + off, expire);
    }
  }

  /**
   * Put item into the cache
   * @param key key
   * @param value value
   * @param expire - expiration (0 - no expire)
   */
  public boolean put(byte[] key, byte[] value, long expire) throws IOException {
    int rank = getDefaultRankToInsert();
    return put(key, 0, key.length, value, 0, value.length, expire, rank, false);
  }

  /* Get API */

  /**
   * Get cached item (if any)
   * @param keyPtr key address
   * @param keySize key size
   * @param buffer buffer for item
   * @param bufOffset buffer offset
   * @return size of an item (-1 - not found), if is greater than bufSize - retry with a properly
   *         adjusted buffer
   * @throws IOException
   */
  public long getKeyValue(long keyPtr, int keySize, byte[] buffer, int bufOffset)
      throws IOException {
    return getKeyValue(keyPtr, keySize, true, buffer, bufOffset);
  }

  /**
   * Get cached item and key (if any)
   * @param keyPtr key address
   * @param keySize key size
   * @param hit if true - its a hit
   * @param buffer buffer for item
   * @param bufOffset buffer offset
   * @return size of an item (-1 - not found), if is greater than bufSize - retry with a properly
   *         adjusted buffer
   * @throws IOException
   */
  public long getKeyValue(long keyPtr, int keySize, boolean hit, byte[] buffer, int bufOffset)
      throws IOException {
    if (this.cacheDisabled) {
      return -1;
    }
    long result = -1;

    try {
      activeRequests.incrementAndGet();
      try {
        result = this.engine.get(keyPtr, keySize, hit, buffer, bufOffset);
      } catch (IOException e) {
        return result;
      }
      if (result <= buffer.length - bufOffset) {
        access();
        if (result >= 0) {
          hit(result);
        }
      }
      if (result >= 0 && result <= buffer.length - bufOffset) {
        if (this.admissionController != null) {
          this.admissionController.access(keyPtr, keySize);
        }
      }
      if (result < 0 && this.victimCache != null) {
        result = this.victimCache.getKeyValue(keyPtr, keySize, hit, buffer, bufOffset);
        if (this.victimCachePromoteOnHit && result >= 0 && result <= buffer.length - bufOffset) {
          // put k-v into this cache, remove it from the victim cache
          MemoryIndex mi = this.victimCache.getEngine().getMemoryIndex();
          if (this.promotionController == null) {
            // We promote item based on its popularity in the victim cache
            double popularity = mi.popularity(keyPtr, keySize);
            if (popularity > this.victimCachePromoteThreshold) {
              long expire = mi.getExpire(keyPtr, keySize);
              boolean res = put(buffer, bufOffset, expire);
              if (res) {
                this.victimCache.delete(keyPtr, keySize);
              }
            }
          } else {
            // verify with PC
            int valSize = Utils.getValueSize(buffer, bufOffset);
            if (this.promotionController.promote(keyPtr, keySize, valSize)) {
              long expire = mi.getExpire(keyPtr, keySize);
              boolean res = put(buffer, bufOffset, expire);
              if (res) {
                this.victimCache.delete(keyPtr, keySize);
              }
            }
          }
        }
      }
      return result;
    } finally {
      activeRequests.decrementAndGet();
    }
  }

  /**
   * Get cached item only (if any)
   * @param keyPtr key address
   * @param keySize key size
   * @param hit if true - its a hit
   * @param buffer buffer for item
   * @param bufOffset buffer offset
   * @return size of an item (-1 - not found), if is greater than bufSize - retry with a properly
   *         adjusted buffer
   * @throws IOException
   */
  public long get(long keyPtr, int keySize, boolean hit, byte[] buffer, int bufOffset)
      throws IOException {
    if (this.cacheDisabled) {
      return -1;
    }
    int rem = buffer.length - bufOffset;
    long result = getKeyValue(keyPtr, keySize, hit, buffer, bufOffset);
    if (result > 0 && result <= rem) {
      // Null value?
      result = Utils.extractValue(buffer, bufOffset);
    }
    return result;
  }

  /**
   * Get cached value range (if any)
   * @param keyPtr key address
   * @param keySize key size
   * @param rangeStart range start
   * @param rangeSize range size
   * @param hit if true - its a hit
   * @param buffer buffer for item
   * @param bufOffset buffer offset
   * @return size of an item (-1 - not found), if is greater than bufSize - retry with a properly
   *         adjusted buffer
   * @throws IOException
   */
  public long getRange(long keyPtr, int keySize, int rangeStart, int rangeSize, boolean hit,
      byte[] buffer, int bufOffset) throws IOException {
    if (this.cacheDisabled) {
      return -1;
    }
    try {
      activeRequests.incrementAndGet();
      long result = -1;
      try {
        result = engine.getRange(keyPtr, keySize, rangeStart, rangeSize, hit, buffer, bufOffset);
      } catch (IOException e) {
        // IOException is possible
        // TODO: better mitigation
        return result;
      }
      if (result <= buffer.length - bufOffset) {
        access();
        if (result >= 0) {
          hit(result);
        }
      }
      if (result >= 0 && result <= buffer.length - bufOffset) {
        if (this.admissionController != null) {
          this.admissionController.access(keyPtr, keySize);
        }
      }
      if (result < 0 && this.victimCache != null) {
        result = this.victimCache.getRange(keyPtr, keySize, rangeStart, rangeSize, hit, buffer,
          bufOffset);
        // For range queries we do not promote item to the parent cache
      }
      return result;
    } finally {
      activeRequests.decrementAndGet();
    }
  }

  /**
   * Get cached item (with hit == true)
   * @param key key buffer
   * @param keyOffset key offset
   * @param keySize key size
   * @param buffer buffer for item
   * @param bufOffset buffer offset
   * @return size of an item (-1 - not found), if is greater than bufSize - retry with a properly
   *         adjusted buffer
   * @throws IOException
   */
  public long getKeyValue(byte[] key, int keyOffset, int keySize, byte[] buffer, int bufOffset)
      throws IOException {
    return getKeyValue(key, keyOffset, keySize, true, buffer, bufOffset);
  }

  /**
   * Get cached item and key (if any)
   * @param key key buffer
   * @param keyOffset key offset
   * @param keySize key size
   * @param hit if true - its a hit
   * @param buffer buffer for item
   * @param bufOffset buffer offset
   * @return size of an item (-1 - not found), if is greater than bufSize - retry with a properly
   *         adjusted buffer
   * @throws IOException
   */
  public long getKeyValue(byte[] key, int keyOffset, int keySize, boolean hit, byte[] buffer,
      int bufOffset) throws IOException {

    if (this.cacheDisabled) {
      return -1;
    }
    try {
      activeRequests.incrementAndGet();
      long result = -1;
      try {
        result = engine.get(key, keyOffset, keySize, hit, buffer, bufOffset);
      } catch (IOException e) {
        // IOException is possible
        // TODO: better mitigation
        return result;
      }

      if (result <= buffer.length - bufOffset) {
        access();
        if (result >= 0) {
          hit(result);
        }
      }
      if (result >= 0 && result <= buffer.length - bufOffset) {
        if (this.admissionController != null) {
          this.admissionController.access(key, keyOffset, keySize);
        }
      }
      if (result < 0 && this.victimCache != null) {
        // TODO: optimize it
        // getWithExpire and getWithExpireAndDelete API
        // one call instead of three
        result = this.victimCache.getKeyValue(key, keyOffset, keySize, hit, buffer, bufOffset);
        if (this.victimCachePromoteOnHit && result >= 0 && result <= buffer.length - bufOffset) {
          // put k-v into this cache, remove it from the victim cache
          MemoryIndex mi = this.victimCache.getEngine().getMemoryIndex();
          if (this.promotionController == null) {
            double popularity = mi.popularity(key, keyOffset, keySize);
            // Promote only popular items
            if (popularity > this.victimCachePromoteThreshold) {
              long expire = mi.getExpire(key, keyOffset, keySize);
              boolean res = put(buffer, bufOffset, expire);
              if (res) {
                this.victimCache.delete(key, keyOffset, keySize);
              }
            }
          } else {
            // verify with PC
            int valSize = Utils.getValueSize(buffer, bufOffset);
            if (this.promotionController.promote(key, keyOffset, keySize, valSize)) {
              long expire = mi.getExpire(key, keyOffset, keySize);
              boolean res = put(buffer, bufOffset, expire);
              if (res) {
                this.victimCache.delete(key, keyOffset, keySize);
              }
            }
          }
        }
      }
      return result;
    } finally {
      activeRequests.decrementAndGet();
    }
  }

  /**
   * Stream API
   */

  /**
   * To write streams to the cache. Stream must be closed after using
   * @param key stream key
   * @param off offset
   * @param len length
   * @param expire stream expiration
   * @return output stream to write data to
   */
  public OutputStream getOutputStream(byte[] key, int off, int len, long expire) {
    if (storageIsFull(0, 0)) {
      this.totalRejectedWrites.incrementAndGet();
      return null;
    }
    return new CacheOutputStream(this, key, off, len, expire);
  }

  /**
   * To read stream data from cache. Stream must be closed after using
   * @param key stream's key
   * @param off key offset
   * @param len key length
   * @return input stream to read data from
   * @throws IOException
   */
  public InputStream getInputStream(byte[] key, int off, int len) throws IOException {
    return CacheInputStream.openStream(this, key, off, len);
  }

  /**
   * Get cached value only (if any)
   * @param key key buffer
   * @return value or null
   * @throws IOException
   */
  public byte[] get(byte[] key) throws IOException {
    return get(key, 0, key.length, true);
  }

  /**
   * Get cached value only (if any)
   * @param key key buffer
   * @param keyOffset key offset
   * @param keySize key size
   * @return value or null
   * @throws IOException
   */
  public byte[] get(byte[] key, int keyOffset, int keySize) throws IOException {
    return get(key, keyOffset, keySize, true);
  }

  /**
   * Get cached value only (if any)
   * @param key key buffer
   * @param keyOffset key offset
   * @param keySize key size
   * @param hit if true - its a hit
   * @return value or null
   * @throws IOException
   */
  public byte[] get(byte[] key, int keyOffset, int keySize, boolean hit) throws IOException {

    if (this.cacheDisabled) {
      return null;
    }
    byte[] buffer = this.tlsEnabled ? this.tlsBuffer.get() : new byte[0];
    long result = getKeyValue(key, keyOffset, keySize, hit, buffer, 0);
    if (result < 0) {
      return null;
    }
    if (result > buffer.length) {
      while (true) {

        buffer = ensureTLSBufferSize((int) result);
        if (buffer == null) {
          buffer = new byte[(int) result];
        }
        result = getKeyValue(key, keyOffset, keySize, hit, buffer, 0);
        if (result < 0) return null;
        if (result <= buffer.length) {
          break;
        }
      }
    }
    int pos = 0;
    int kSize = Utils.readUVInt(buffer, pos);
    int kSizeSize = Utils.sizeUVInt(kSize);
    pos += kSizeSize;
    int vSize = Utils.readUVInt(buffer, pos);
    int vSizeSize = Utils.sizeUVInt(vSize);
    pos += vSizeSize + kSize;
    byte[] buf = new byte[vSize];
    System.arraycopy(buffer, pos, buf, 0, vSize);
    return buf;
  }

  /**
   * Get cached key-value pair (if any) returned value must be processed in the same thread
   * @param key key buffer
   * @return key - value pair
   * @throws IOException
   */
  public byte[] getKeyValue(byte[] key) throws IOException {
    return getKeyValue(key, 0, key.length, true);
  }

  /**
   * Get cached key-value pair (if any) returned value must be processed in the same thread
   * @param key key buffer
   * @param keyOffset key offset
   * @param keySize key size
   * @return key - value pair
   * @throws IOException
   */
  public byte[] getKeyValue(byte[] key, int keyOffset, int keySize) throws IOException {
    return getKeyValue(key, keyOffset, keySize, true);
  }

  /**
   * Get cached key-value pair (if any) returned value must be processed in the same thread
   * @param key key buffer
   * @param keyOffset key offset
   * @param keySize key size
   * @param hit if true - its a hit
   * @return key - value pair
   * @throws IOException
   */
  public byte[] getKeyValue(byte[] key, int keyOffset, int keySize, boolean hit)
      throws IOException {

    if (this.cacheDisabled) {
      return null;
    }
    try {
      activeRequests.incrementAndGet();
      byte[] buffer = this.tlsEnabled ? this.tlsBuffer.get() : new byte[0];
      long result = getKeyValue(key, keyOffset, keySize, hit, buffer, 0);
      if (result < 0) {
        return null;
      }
      if (result > buffer.length) {
        while (true) {
          buffer = ensureTLSBufferSize((int) result);
          if (buffer == null) {
            buffer = new byte[(int) result];
          }
          result = getKeyValue(key, keyOffset, keySize, hit, buffer, 0);
          if (result < 0) return null;
          if (result <= buffer.length) {
            break;
          }
        }
      }
      int size = 0;
      int off = 0;
      int kSize = Utils.readUVInt(buffer, off);
      int kSizeSize = Utils.sizeUVInt(kSize);
      off += kSizeSize;
      int vSize = Utils.readUVInt(buffer, off);
      int vSizeSize = Utils.sizeUVInt(vSize);
      size = kSize + vSize + kSizeSize + vSizeSize;
      if (this.tlsEnabled || size < buffer.length) {
        byte[] buf = new byte[(int) size];
        System.arraycopy(buffer, 0, buf, 0, buf.length);
        return buf;
      }
      return buffer;
    } finally {
      activeRequests.decrementAndGet();
    }
  }

  /**
   * Get cached value only (if any)
   * @param keyPtr key buffer address
   * @param keySize key size
   * @return value or null
   * @throws IOException
   */
  public byte[] get(long keyPtr, int keySize) throws IOException {
    return get(keyPtr, keySize, true);
  }

  /**
   * Get cached value only (if any)
   * @param keyPtr key buffer address
   * @param keySize key size
   * @param hit if true - its a hit
   * @return value or null
   * @throws IOException
   */
  public byte[] get(long keyPtr, int keySize, boolean hit) throws IOException {

    if (this.cacheDisabled) {
      return null;
    }
    byte[] buffer = this.tlsEnabled ? this.tlsBuffer.get() : new byte[0];
    long result = getKeyValue(keyPtr, keySize, hit, buffer, 0);
    if (result < 0) {
      return null;
    }
    if (result > buffer.length) {
      while (true) {
        buffer = ensureTLSBufferSize((int) result);
        if (buffer == null) {
          buffer = new byte[(int) result];
        }
        result = getKeyValue(keyPtr, keySize, hit, buffer, 0);
        if (result < 0) return null;
        if (result <= buffer.length) {
          break;
        }
      }
    }
    int pos = 0;
    int kSize = Utils.readUVInt(buffer, pos);
    int kSizeSize = Utils.sizeUVInt(kSize);
    pos += kSizeSize;
    int vSize = Utils.readUVInt(buffer, pos);
    int vSizeSize = Utils.sizeUVInt(vSize);
    pos += vSizeSize + kSize;
    byte[] buf = new byte[vSize];
    System.arraycopy(buffer, pos, buf, 0, vSize);
    return buf;
  }

  /**
   * Get cached key-value pair (if any) returned value must be processed in the same thread
   * @param keyPtr key buffer address
   * @param keySize key size
   * @return key - value pair
   * @throws IOException
   */
  public byte[] getKeyValue(long keyPtr, int keySize) throws IOException {
    return getKeyValue(keyPtr, keySize, true);
  }

  /**
   * Get cached key-value pair (if any) returned value must be processed in the same thread
   * @param key key buffer
   * @param keyOffset key offset
   * @param keySize key size
   * @param hit if true - its a hit
   * @return key - value pair
   * @throws IOException
   */
  public byte[] getKeyValue(long keyPtr, int keySize, boolean hit) throws IOException {

    if (this.cacheDisabled) {
      return null;
    }
    byte[] buffer = this.tlsEnabled ? this.tlsBuffer.get() : new byte[0];
    long result = getKeyValue(keyPtr, keySize, hit, buffer, 0);
    if (result < 0) {
      return null;
    }
    if (result > buffer.length) {
      while (true) {
        buffer = ensureTLSBufferSize((int) result);
        if (buffer == null) {
          buffer = new byte[(int) result];
        }
        result = getKeyValue(keyPtr, keySize, hit, buffer, 0);
        if (result < 0) return null;
        if (result <= buffer.length) {
          break;
        }
      }
    }
    int size = 0;
    int off = 0;
    int kSize = Utils.readUVInt(buffer, off);
    int kSizeSize = Utils.sizeUVInt(kSize);
    off += kSizeSize;
    int vSize = Utils.readUVInt(buffer, off);
    int vSizeSize = Utils.sizeUVInt(vSize);
    size = kSize + vSize + kSizeSize + vSizeSize;
    if (this.tlsEnabled || size < buffer.length) {
      byte[] buf = new byte[(int) size];
      System.arraycopy(buffer, 0, buf, 0, buf.length);
      return buf;
    }
    return buffer;
  }

  /**
   * Get cached item only (if any)
   * @param key key buffer
   * @param keyOffset key offset
   * @param keySize key size
   * @param buffer buffer
   * @param bufOffset buffer offset
   * @return size of a value
   * @throws IOException
   */
  public long get(byte[] key, int keyOffset, int keySize, byte[] buffer, int bufOffset)
      throws IOException {
    return get(key, keyOffset, keySize, true, buffer, bufOffset);
  }

  /**
   * Get cached item only (if any)
   * @param key key buffer
   * @param keyOffset key offset
   * @param keySize key size
   * @param hit if true - its a hit
   * @param buffer buffer for item
   * @param bufOffset buffer offset
   * @return size of an item (-1 - not found), if is greater than bufSize - retry with a properly
   *         adjusted buffer
   * @throws IOException
   */
  public long get(byte[] key, int keyOffset, int keySize, boolean hit, byte[] buffer, int bufOffset)
      throws IOException {
    if (this.cacheDisabled) {
      return -1;
    }

    // TODO: compare other method (get into temp buffer, then copy to destination)
    int rem = buffer.length - bufOffset;
    long result = getKeyValue(key, keyOffset, keySize, hit, buffer, bufOffset);
    if (result > 0 && result <= rem) {
      result = Utils.extractValue(buffer, bufOffset);
    }
    return result;
  }

  /**
   * Get cached value range
   * @param key key buffer
   * @param keyOffset key offset
   * @param keySize key size
   * @param rangeStart range start
   * @param rangeSize range size
   * @param hit if true - its a hit
   * @param buffer buffer for item
   * @param bufOffset buffer offset
   * @return size of an item (-1 - not found), if is greater than bufSize - retry with a properly
   *         adjusted buffer
   * @throws IOException
   */
  public long getRange(byte[] key, int keyOffset, int keySize, int rangeStart, int rangeSize,
      boolean hit, byte[] buffer, int bufOffset) throws IOException {

    if (this.cacheDisabled) {
      return -1;
    }
    try {
      activeRequests.incrementAndGet();
      long result = -1;
      try {
        result =
            engine.getRange(key, keyOffset, keySize, rangeStart, rangeSize, hit, buffer, bufOffset);
      } catch (IOException e) {
        // IOException is possible
        // TODO: better mitigation
        return result;
      }

      if (result <= buffer.length - bufOffset) {
        access();
        if (result >= 0) {
          hit(result);
        }
      }
      if (result >= 0 && result <= buffer.length - bufOffset) {
        if (this.admissionController != null) {
          this.admissionController.access(key, keyOffset, keySize);
        }
      }
      if (result < 0 && this.victimCache != null) {
        result = this.victimCache.getRange(key, keyOffset, keySize, rangeStart, rangeSize, hit,
          buffer, bufOffset);
        // For range queries we do not promote item to the parent cache
      }
      return result;
    } finally {
      activeRequests.decrementAndGet();
    }
  }

  /**
   * Get cached item (if any)
   * @param key key buffer
   * @param keyOff key offset
   * @param keySize key size
   * @param buffer byte buffer for item
   * @return size of an item (-1 - not found), if is greater than bufSize - retry with a properly
   *         adjusted buffer
   * @throws IOException
   */
  public long getKeyValue(byte[] key, int keyOff, int keySize, ByteBuffer buffer)
      throws IOException {
    return getKeyValue(key, keyOff, keySize, true, buffer);
  }

  /**
   * Get cached item and key (if any)
   * @param key key buffer
   * @param keyOff key offset
   * @param keySize key size
   * @param hit if true - its a hit
   * @param buffer byte buffer for item
   * @return size of an item (-1 - not found), if is greater than bufSize - retry with a properly
   *         adjusted buffer
   * @throws IOException
   */
  public long getKeyValue(byte[] key, int keyOff, int keySize, boolean hit, ByteBuffer buffer)
      throws IOException {
    if (this.cacheDisabled) {
      return -1;
    }
    try {

      activeRequests.incrementAndGet();
      int rem = buffer.remaining();
      long result = -1;
      try {
        result = this.engine.get(key, keyOff, keySize, hit, buffer);
      } catch (IOException e) {
        return result;
      }
      if (result <= rem) {
        access();
        if (result >= 0) {
          hit(result);
        }
      }
      if (result >= 0 && result <= rem) {
        if (this.admissionController != null) {
          this.admissionController.access(key, keyOff, keySize);
        }
      }
      if (result < 0 && this.victimCache != null) {
        result = this.victimCache.getKeyValue(key, keyOff, keySize, hit, buffer);
        if (this.victimCachePromoteOnHit && result >= 0 && result <= rem) {
          // put k-v into this cache, remove it from the victim cache
          MemoryIndex mi = this.victimCache.getEngine().getMemoryIndex();
          if (this.promotionController == null) {
            double popularity = mi.popularity(key, keyOff, keySize);
            if (popularity > this.victimCachePromoteThreshold) {
              long expire = mi.getExpire(key, keyOff, keySize);
              boolean res = put(buffer, expire);
              if (res) {
                this.victimCache.delete(key, keyOff, keySize);
              }
            }
          } else {
            // verify with PC
            int valSize = Utils.getValueSize(buffer);
            if (this.promotionController.promote(key, keyOff, keySize, valSize)) {
              long expire = mi.getExpire(key, keyOff, keySize);
              boolean res = put(buffer, expire);
              if (res) {
                this.victimCache.delete(key, keyOff, keySize);
              }
            }
          }
        }
      }
      return result;
    } finally {
      activeRequests.decrementAndGet();
    }
  }

  /**
   * Get cached item value only (if any)
   * @param key key buffer
   * @param keyOff key offset
   * @param keySize key size
   * @param hit if true - its a hit
   * @param buffer byte buffer for item
   * @return size of an item (-1 - not found), if is greater than bufSize - retry with a properly
   *         adjusted buffer
   * @throws IOException
   */
  public long get(byte[] key, int keyOff, int keySize, boolean hit, ByteBuffer buffer)
      throws IOException {
    if (this.cacheDisabled) {
      return -1;
    }

    int rem = buffer.remaining();
    long result = getKeyValue(key, keyOff, keySize, hit, buffer);
    if (result > 0 && result <= rem) {
      result = Utils.extractValue(buffer);
    }
    return result;
  }

  /**
   * Get cached value range (if any)
   * @param key key buffer
   * @param keyOffset key offset
   * @param keySize key size
   * @param rangeStart range start
   * @param rangeSize range size
   * @param hit if true - its a hit
   * @param buffer byte buffer for item
   * @return size of an item (-1 - not found), if is greater than bufSize - retry with a properly
   *         adjusted buffer
   * @throws IOException
   */
  public long getRange(byte[] key, int keyOffset, int keySize, int rangeStart, int rangeSize,
      boolean hit, ByteBuffer buffer) throws IOException {
    if (this.cacheDisabled) {
      return -1;
    }
    try {
      activeRequests.incrementAndGet();
      long result = -1;
      int rem = buffer.remaining();
      try {
        result = engine.getRange(key, keyOffset, keySize, rangeStart, rangeSize, hit, buffer);
      } catch (IOException e) {
        // IOException is possible
        // TODO: better mitigation
        return result;
      }
      if (result <= rem) {
        access();
        if (result >= 0) {
          hit(result);
        }
      }
      if (result >= 0 && result <= rem) {
        if (this.admissionController != null) {
          this.admissionController.access(key, keyOffset, keySize);
        }
      }
      if (result < 0 && this.victimCache != null) {
        result =
            this.victimCache.getRange(key, keyOffset, keySize, rangeStart, rangeSize, hit, buffer);
        // For range queries we do not promote item to the parent cache
      }
      return result;
    } finally {
      activeRequests.decrementAndGet();
    }
  }

  /**
   * Get cached item (if any)
   * @param keyPtr key address
   * @param keySize key size
   * @param buffer byte buffer for item
   * @return size of an item (-1 - not found), if is greater than bufSize - retry with a properly
   *         adjusted buffer
   * @throws IOException
   */
  public long getKeyValue(long keyPtr, int keySize, ByteBuffer buffer) throws IOException {
    return getKeyValue(keyPtr, keySize, true, buffer);
  }

  /**
   * Get cached item and key (if any)
   * @param keyPtr key address
   * @param keySize key size
   * @param hit if true - its a hit
   * @param buffer byte buffer for item
   * @return size of an item (-1 - not found), if is greater than bufSize - retry with a properly
   *         adjusted buffer
   * @throws IOException
   */
  public long getKeyValue(long keyPtr, int keySize, boolean hit, ByteBuffer buffer)
      throws IOException {
    if (this.cacheDisabled) {
      return -1;
    }
    try {
      activeRequests.incrementAndGet();
      int rem = buffer.remaining();
      long result = -1;
      try {
        result = this.engine.get(keyPtr, keySize, hit, buffer);
      } catch (IOException e) {
        return result;
      }
      if (result <= rem) {
        access();
        if (result >= 0) {
          hit(result);
        }
      }
      if (result >= 0 && result <= rem) {
        if (this.admissionController != null) {
          this.admissionController.access(keyPtr, keySize);
        }
      }
      if (result < 0 && this.victimCache != null) {
        result = this.victimCache.getKeyValue(keyPtr, keySize, hit, buffer);
        if (this.victimCachePromoteOnHit && result >= 0 && result <= rem) {
          // put k-v into this cache, remove it from the victim cache
          MemoryIndex mi = this.victimCache.getEngine().getMemoryIndex();
          if (this.promotionController == null) {
            double popularity = mi.popularity(keyPtr, keySize);
            if (popularity > this.victimCachePromoteThreshold) {
              long expire = mi.getExpire(keyPtr, keySize);
              boolean res = put(buffer, expire);
              if (res) {
                this.victimCache.delete(keyPtr, keySize);
              }
            }
          } else {
            // verify with PC
            int valSize = Utils.getValueSize(buffer);
            if (this.promotionController.promote(keyPtr, keySize, valSize)) {
              long expire = mi.getExpire(keyPtr, keySize);
              boolean res = put(buffer, expire);
              if (res) {
                this.victimCache.delete(keyPtr, keySize);
              }
            }
          }
        }
      }
      return result;
    } finally {
      activeRequests.decrementAndGet();
    }
  }

  /**
   * Get cached item only (if any)
   * @param keyPtr key address
   * @param keySize key size
   * @param hit if true - its a hit
   * @param buffer byte buffer for item
   * @return size of an item (-1 - not found), if is greater than bufSize - retry with a properly
   *         adjusted buffer
   * @throws IOException
   */
  public long get(long keyPtr, int keySize, boolean hit, ByteBuffer buffer) throws IOException {
    if (this.cacheDisabled) {
      return -1;
    }
    int rem = buffer.remaining();

    long result = getKeyValue(keyPtr, keySize, hit, buffer);
    if (result <= rem && result > 0) {
      result = Utils.extractValue(buffer);
    }
    return result;
  }

  /**
   * Get cached value range (if any)
   * @param keyPtr key address
   * @param keySize key size
   * @param rangeStart range start
   * @param rangeSize range size
   * @param hit if true - its a hit
   * @param buffer byte buffer for item
   * @return size of an item (-1 - not found), if is greater than bufSize - retry with a properly
   *         adjusted buffer
   * @throws IOException
   */
  public long getRange(long keyPtr, int keySize, int rangeStart, int rangeSize, boolean hit,
      ByteBuffer buffer) throws IOException {
    if (this.cacheDisabled) {
      return -1;
    }
    try {
      activeRequests.incrementAndGet();
      long result = -1;
      int rem = buffer.remaining();
      try {
        result = engine.getRange(keyPtr, keySize, rangeStart, rangeSize, hit, buffer);
      } catch (IOException e) {
        // IOException is possible
        // TODO: better mitigation
        return result;
      }

      if (result <= rem) {
        access();
        if (result >= 0) {
          hit(result);
        }
      }
      if (result >= 0 && result <= rem) {
        if (this.admissionController != null) {
          this.admissionController.access(keyPtr, keySize);
        }
      }

      if (result < 0 && this.victimCache != null) {
        result = this.victimCache.getRange(keyPtr, keySize, rangeStart, rangeSize, hit, buffer);
        // For range queries we do not promote item to the parent cache
      }
      return result;
    } finally {
      activeRequests.decrementAndGet();
    }
  }

  /* Delete API */

  /**
   * Delete cached item
   * @param keyPtr key address
   * @param keySize key size
   * @return true - success, false - does not exist
   * @throws IOException
   */
  public boolean delete(long keyPtr, int keySize) throws IOException {
    if (this.cacheDisabled) {
      return false;
    }
    try {
      activeRequests.incrementAndGet();
      boolean result = engine.delete(keyPtr, keySize);
      if (!result && this.victimCache != null) {
        return this.victimCache.delete(keyPtr, keySize);
      }
      return result;
    } finally {
      activeRequests.decrementAndGet();
    }
  }

  /**
   * Delete cached item
   * @param key key
   * @param keyOffset key offset
   * @param keySize key size
   * @return true - success, false - does not exist
   * @throws IOException
   */
  public boolean delete(byte[] key, int keyOffset, int keySize) throws IOException {
    if (this.cacheDisabled) {
      return false;
    }
    try {
      activeRequests.incrementAndGet();
      boolean result = engine.delete(key, keyOffset, keySize);
      if (!result && this.victimCache != null) {
        return this.victimCache.delete(key, keyOffset, keySize);
      }
      return result;
    } finally {
      activeRequests.decrementAndGet();
    }
  }

  /**
   * Delete cached item
   * @param key key buffer
   * @return true - success, false - does not exist
   * @throws IOException
   */
  public boolean delete(byte[] key) throws IOException {
    return delete(key, 0, key.length);
  }

  /**
   * Expire cached item
   * @param keyPtr key address
   * @param keySize key size
   * @return true - success, false - does not exist
   * @throws IOException
   */
  public boolean expire(long keyPtr, int keySize) throws IOException {
    return delete(keyPtr, keySize);
  }

  /**
   * Expire cached item
   * @param key key
   * @param keyOffset key offset
   * @param keySize key size
   * @return true - success, false - does not exist
   * @throws IOException
   */
  public boolean expire(byte[] key, int keyOffset, int keySize) throws IOException {
    return delete(key, keyOffset, keySize);
  }

  /**
   * Expire cached item
   * @param key key
   * @return true - success, false - does not exist
   * @throws IOException
   */
  public boolean expire(byte[] key) throws IOException {
    return delete(key, 0, key.length);
  }

  /**
   * Get expiration time of a key
   * @param key key buffer
   * @param off offset
   * @param size key size
   * @return time, 0 - no expire, -1 - does not exists or not supported
   */
  public long getExpire(byte[] key, int off, int size) {
    if (this.cacheDisabled) {
      return -1;
    }
    try {
      activeRequests.incrementAndGet();
      long expire = this.engine.getMemoryIndex().getExpire(key, off, size);
      return expire;
    } finally {
      activeRequests.decrementAndGet();
    }
  }

  /**
   * Get expiration time
   * @param keyPtr key address
   * @param keySize key size
   * @return time in ms since you know when, -1 not supported or does not exists
   */
  public long getExpire(long keyPtr, int keySize) {
    if (this.cacheDisabled) {
      return -1;
    }
    try {
      activeRequests.incrementAndGet();
      return this.engine.getMemoryIndex().getExpire(keyPtr, keySize);
    } finally {
      activeRequests.decrementAndGet();
    }
  }

  /**
   * Get and set expiration time of a key
   * @param key key buffer
   * @param off offset
   * @param size key size
   * @return time, 0 - no expire, -1 - does not exists or not supported
   */
  public long getAndSetExpire(byte[] key, int off, int size, long expire) {
    if (this.cacheDisabled) {
      return -1;
    }
    try {
      activeRequests.incrementAndGet();
      return this.engine.getMemoryIndex().getAndSetExpire(key, off, size, expire);
    } finally {
      activeRequests.decrementAndGet();
    }
  }

  /**
   * Get and set expiration time
   * @param keyPtr key address
   * @param keySize key size
   * @return time in ms since you know when, -1 not supported or does not exists
   */
  public long getAndSetExpire(long keyPtr, int keySize, long expire) {
    if (this.cacheDisabled) {
      return -1;
    }
    try {
      activeRequests.incrementAndGet();
      return this.engine.getMemoryIndex().getAndSetExpire(keyPtr, keySize, expire);
    } finally {
      activeRequests.decrementAndGet();
    }
  }

  /**
   * Does key exist in the cache
   * @param key key
   * @return true or false
   */
  public boolean exists(byte[] key) {
    return exists(key, 0, key.length);
  }

  /**
   * Does key exist (false positive are possible, but not false negatives)
   * @param key key buffer
   * @param off key offset
   * @param size key size
   * @return true or false
   */
  public boolean exists(byte[] key, int off, int size) {
    if (this.cacheDisabled) {
      return false;
    }
    try {
      activeRequests.incrementAndGet();
      return this.engine.getMemoryIndex().exists(key, off, size);
    } finally {
      activeRequests.decrementAndGet();
    }
  }

  /**
   * Does key exist (exact)
   * @param key key buffer
   * @param off key offset
   * @param size key size
   * @return true or false
   * @throws IOException
   */
  public boolean existsExact(byte[] key, int off, int size) throws IOException {
    if (this.cacheDisabled) {
      return false;
    }
    try {
      activeRequests.incrementAndGet();
      byte[] buf = new byte[16];
      long result = get(key, off, size, false, buf, 0);
      return result >= 0;
    } finally {
      activeRequests.decrementAndGet();
    }
  }

  /**
   * Does key exist (exact)
   * @param key key buffer
   * @param off key offset
   * @param size key size
   * @return true or false
   * @throws IOException
   */
  public boolean existsExact(long keyPtr, int size) throws IOException {
    if (this.cacheDisabled) {
      return false;
    }
    try {
      activeRequests.incrementAndGet();
      byte[] buf = new byte[16];
      long result = get(keyPtr, size, false, buf, 0);
      return result >= 0;
    } finally {
      activeRequests.decrementAndGet();
    }
  }

  /**
   * Touches the key
   * @param key key buffer
   * @param off key offset
   * @param size key size
   * @return true or false (key does not exist)
   */
  public boolean touch(byte[] key, int off, int size) {
    return this.engine.getMemoryIndex().touch(key, off, size);
  }

  /**
   * Get victim cache
   * @return victim cache or null
   */
  public Cache getVictimCache() {
    return this.victimCache;
  }

  /**
   * Sets victim cache
   * @param c victim cache
   */
  public void setVictimCache(Cache c) {
    if (getCacheType() == Type.DISK) {
      throw new IllegalArgumentException("Victim cache is not supported for DISK type cache");
    }
    this.victimCache = c;
    this.victimCachePromoteOnHit = this.conf.getVictimCachePromotionOnHit(c.getName());
    this.victimCachePromoteThreshold = this.conf.getVictimPromotionThreshold(c.getName());
    this.hybridCacheInverseMode = this.conf.getCacheHybridInverseMode(cacheName);
    this.victimCache.setParentCache(this);
  }

  /**
   * Sets parent cache
   * @param parent cache
   */
  public void setParentCache(Cache parent) {
    this.parentCache = parent;
  }

  /**
   * Gets parent cache
   * @return parent cache
   */
  public Cache getParentCache() {
    return this.parentCache;
  }

  @SuppressWarnings("unused")
  //TODO: remove this code  
  private boolean processPromotion(long ptr, long $ptr) {
    if (this.parentCache == null) {
      return false;
    }

    IndexFormat indexFormat = this.engine.getMemoryIndex().getIndexFormat();
    int size = indexFormat.fullEntrySize($ptr);
    try {
      // Check embedded mode
      if (this.indexEmdeddingSupported) {
        if (size <= this.indexEmbeddedSize) {
          transferEmbeddedToCache(this.parentCache, ptr, $ptr);
          return true;
        }
      }
      // else - not embedded
      // transfer item to victim cache
      transferToCache(this.parentCache, ptr, $ptr);
    } catch (IOException e) {
      // TODO:
      LOG.error("Error:", e);
    }
    return true;
  }

  @SuppressWarnings("unused")
  //TODO: remove this
  private void processEviction(long ptr, long $ptr) {
    if (this.victimCache == null) {
      return;
    }
    if (this.admissionController != null
        && !this.admissionController.shouldEvictToVictimCache(ptr, $ptr)) {
      return;
    }
    IndexFormat indexFormat = this.engine.getMemoryIndex().getIndexFormat();
    int size = indexFormat.fullEntrySize($ptr);
    try {
      // Check embedded mode
      // FIXME: this calls are expensive
      // FIXME: this code is wrong
      if (this.indexEmdeddingSupported) {
        if (size <= this.indexEmbeddedSize) {
          transferEmbeddedToCache(this.victimCache, ptr, $ptr);
          return;
        }
      }
      // else - not embedded
      // transfer item to victim cache
      transferToCache(this.victimCache, ptr, $ptr);
    } catch (IOException e) {
      LOG.error("Error:", e);
    }
  }

  /**
   * Transfer cached item to a victim cache
   * @param ibPtr index block pointer
   * @param indexPtr item pointer
   * @throws IOException
   */
  private void transferToCache(Cache c, long ibPtr, long indexPtr) throws IOException {
    if (getCacheType() == Type.DISK) {
      LOG.error("Attempt to transfer cached item from cache type = DISK");
      throw new IllegalArgumentException("Victim cache is not supported for DISK type cache");
    }
    boolean isScavenger = Thread.currentThread().getName().startsWith(Scavenger.NAME);
    // Cache is off-heap
    IndexFormat format = this.engine.getMemoryIndex().getIndexFormat();
    long expire = format.getExpire(ibPtr, indexPtr);
    int rank = this.victimCache.getDefaultRankToInsert();
    int sid = (int) format.getSegmentId(indexPtr);
    long offset = format.getOffset(indexPtr);

    Segment s = this.engine.getSegmentById(sid);
    if (s == null) {
      return;
    }
    int groupRank = s.getInfo().getGroupRank();
    // TODO : check segment
    try {
      s.readLock();
      if (s.isMemory()) {
        long ptr = s.getAddress();
        ptr += offset;
        int keySize = Utils.readUVInt(ptr);
        int kSizeSize = Utils.sizeUVInt(keySize);
        ptr += kSizeSize;
        int valueSize = Utils.readUVInt(ptr);
        int vSizeSize = Utils.sizeUVInt(valueSize);
        ptr += vSizeSize;
        // Do not force PUT, let victim's cache admission controller work
        this.victimCache.put(ptr, keySize, ptr + keySize, valueSize, expire, rank, groupRank, false,
          isScavenger);
      } else {
        // not supported yet
      }
    } finally {
      s.readUnlock();
    }
  }

  private int getDefaultRankToInsert() {
    return this.engine.getMemoryIndex().getEvictionPolicy().getDefaultRankForInsert();
  }

  /**
   * Transfer cached item to a victim cache
   * @param ibPtr index block pointer
   * @param indexPtr item pointer
   * @throws IOException
   */
  private void transferEmbeddedToCache(Cache c, long ibPtr, long indexPtr) throws IOException {
    if (getCacheType() == Type.DISK) {
      LOG.error("Attempt to transfer cached item from cache type = DISK");
      throw new IllegalArgumentException("Victim cache is not supported for DISK type cache");
    }
    if (this.victimCache == null) {
      LOG.error("Attempt to transfer cached item when victim cache is null");
      return;
    }
    // Cache is off-heap
    IndexFormat format = this.engine.getMemoryIndex().getIndexFormat();
    long expire = format.getExpire(ibPtr, indexPtr);
    int rank = this.victimCache.getDefaultRankToInsert();

    int off = format.getEmbeddedOffset();
    indexPtr += off;
    int kSize = Utils.readUVInt(indexPtr);
    int kSizeSize = Utils.sizeUVInt(kSize);
    indexPtr += kSizeSize;
    int vSize = Utils.readUVInt(indexPtr);
    int vSizeSize = Utils.sizeUVInt(vSize);
    indexPtr += vSizeSize;
    // Do not force PUT, let victim's cache admission controller to work
    // TODO: groupRank?
    c.put(indexPtr, kSize, indexPtr + kSize, vSize, expire, rank, rank, false);
  }

  // IOEngine.Listener
  @Override
  public void onEvent(IOEngine e, IOEngineEvent evt) {
    if (evt == IOEngineEvent.DATA_SIZE_CHANGED) {
      double used = this.engine.getStorageAllocatedRatio();
      // TODO: performance
      double max = this.conf.getScavengerStartMemoryRatio(this.cacheName);
      double min = this.conf.getScavengerStopMemoryRatio(this.cacheName);
      if (this.evictionDisabledMode) {
        return;
      }
      if (used >= max) {
        if (this.victimCache == null) {
          // Enable eviction only if we do not have victim cache to avoid deadlock
          // this.engine.setEvictionEnabled(true);
        }
        this.tcEnabled = true;
        // FIXME: do we need to start scavengers here?
        // startScavengers();
      } else if (used < min) {
        // this.engine.setEvictionEnabled(false);
      }
    }
  }

  // Persistence section

  /**
   * Loads cache meta data
   * @throws IOException
   */
  private void loadCache() throws IOException {
    CacheConfig conf = CacheConfig.getInstance();
    String snapshotDir = conf.getSnapshotDir(this.cacheName);
    String file = CacheConfig.CACHE_SNAPSHOT_NAME;
    Path p = Paths.get(snapshotDir, file);
    if (Files.exists(p)) {
      FileInputStream fis = new FileInputStream(p.toFile());
      DataInputStream dis = new DataInputStream(fis);
      this.cacheName = dis.readUTF();
      this.type = Type.values()[dis.readInt()];
      this.totalGets.set(dis.readLong());
      this.totalGetsSize.set(dis.readLong());
      this.totalHits.set(dis.readLong());
      this.totalWrites.set(dis.readLong());
      this.totalWritesSize.set(dis.readLong());
      this.totalRejectedWrites.set(dis.readLong());
      Epoch.setEpochStartTime(dis.readLong());
      this.tcEnabled = dis.readBoolean();
      this.evictionDisabledMode = dis.readBoolean();
      // Load configuration
      Properties props = new Properties();
      props.load(dis);
      CacheConfig.merge(props);
      this.conf = CacheConfig.getInstance();
      dis.close();
    } else {
      throw new IOException(
          String.format("Can not load cache. Path %s does not exists", p.toString()));
    }
  }

  /**
   * Saves cache meta data
   * @throws IOException
   */
  private void saveCache() throws IOException {
    String snapshotDir = this.conf.getSnapshotDir(this.cacheName);
    String file = CacheConfig.CACHE_SNAPSHOT_NAME;
    Path p = Paths.get(snapshotDir, file);
    FileOutputStream fos = new FileOutputStream(p.toFile());
    DataOutputStream dos = new DataOutputStream(fos);
    dos.writeUTF(this.cacheName);
    dos.writeInt(this.type.ordinal());
    dos.writeLong(this.totalGets.get());
    dos.writeLong(this.totalGetsSize.get());
    dos.writeLong(this.totalHits.get());
    dos.writeLong(this.totalWrites.get());
    dos.writeLong(this.totalWritesSize.get());
    dos.writeLong(this.totalRejectedWrites.get());
    dos.writeLong(Epoch.getEpochStartTime());
    dos.writeBoolean(this.tcEnabled);
    dos.writeBoolean(this.evictionDisabledMode);
    this.conf.save(dos);
    dos.close();
  }

  /**
   * Loads admission controller data
   * @throws IOException
   */
  private void loadAdmissionControlller() throws IOException {
    if (this.admissionController == null) {
      return;
    }
    String snapshotDir = this.conf.getSnapshotDir(this.cacheName);
    String file = CacheConfig.ADMISSION_CONTROLLER_SNAPSHOT_NAME;
    Path p = Paths.get(snapshotDir, file);
    if (Files.exists(p) && Files.size(p) > 0) {
      FileInputStream fis = new FileInputStream(p.toFile());
      DataInputStream dis = new DataInputStream(fis);
      this.admissionController.load(dis);
      dis.close();
    }
  }

  /**
   * Saves admission controller data
   * @throws IOException
   */
  private void saveAdmissionController() throws IOException {
    if (this.admissionController == null) {
      return;
    }
    String snapshotDir = this.conf.getSnapshotDir(this.cacheName);
    String file = CacheConfig.ADMISSION_CONTROLLER_SNAPSHOT_NAME;
    Path p = Paths.get(snapshotDir, file);
    FileOutputStream fos = new FileOutputStream(p.toFile());
    DataOutputStream dos = new DataOutputStream(fos);
    this.admissionController.save(dos);
    dos.close();
  }

  /**
   * Loads throughput controller data
   * @throws IOException
   */
  private void loadThroughputControlller() throws IOException {
    if (this.throughputController == null) {
      return;
    }
    String snapshotDir = this.conf.getSnapshotDir(this.cacheName);
    String file = CacheConfig.THROUGHPUT_CONTROLLER_SNAPSHOT_NAME;
    Path p = Paths.get(snapshotDir, file);
    if (Files.exists(p) && Files.size(p) > 0) {
      FileInputStream fis = new FileInputStream(p.toFile());
      DataInputStream dis = new DataInputStream(fis);
      this.throughputController.load(dis);
      dis.close();
    }
  }

  /**
   * Saves throughput controller data
   * @throws IOException
   */
  private void saveThroughputController() throws IOException {
    if (this.throughputController == null) {
      return;
    }
    String snapshotDir = this.conf.getSnapshotDir(this.cacheName);
    String file = CacheConfig.THROUGHPUT_CONTROLLER_SNAPSHOT_NAME;
    Path p = Paths.get(snapshotDir, file);
    FileOutputStream fos = new FileOutputStream(p.toFile());
    DataOutputStream dos = new DataOutputStream(fos);
    this.throughputController.save(dos);
    dos.close();
  }

  /**
   * Loads scavenger statistics data
   * @throws IOException
   */
  private void loadScavengerStats() throws IOException {
    String snapshotDir = this.conf.getSnapshotDir(this.cacheName);
    String file = CacheConfig.SCAVENGER_STATS_SNAPSHOT_NAME;
    Path p = Paths.get(snapshotDir, file);
    if (Files.exists(p) && Files.size(p) > 0) {
      FileInputStream fis = new FileInputStream(p.toFile());
      DataInputStream dis = new DataInputStream(fis);
      Stats stats = new Stats(this.cacheName);
      stats.load(dis);
      Scavenger.setStatisticsForCache(this.cacheName, stats);
      dis.close();
    }
  }

  /**
   * Saves scavenger statistics data
   * @throws IOException
   */
  private void saveScavengerStats() throws IOException {
    String snapshotDir = this.conf.getSnapshotDir(this.cacheName);
    String file = CacheConfig.SCAVENGER_STATS_SNAPSHOT_NAME;
    Path p = Paths.get(snapshotDir, file);
    FileOutputStream fos = new FileOutputStream(p.toFile());
    DataOutputStream dos = new DataOutputStream(fos);
    Stats stats = Scavenger.getStatisticsForCache(this.cacheName);
    stats.save(dos);
    dos.close();
  }

  /**
   * Loads engine data
   * @throws IOException
   */
  private void loadEngine() throws IOException {
    String snapshotDir = this.conf.getSnapshotDir(this.cacheName);
    String file = CacheConfig.CACHE_ENGINE_SNAPSHOT_NAME;
    Path p = Paths.get(snapshotDir, file);
    if (Files.exists(p) && Files.size(p) > 0) {
      FileInputStream fis = new FileInputStream(p.toFile());
      DataInputStream dis = new DataInputStream(fis);
      this.engine.load(dis);
      dis.close();
    }
  }

  /**
   * Saves engine data
   * @throws IOException
   */
  private void saveEngine() throws IOException {
    String snapshotDir = this.conf.getSnapshotDir(this.cacheName);
    String file = CacheConfig.CACHE_ENGINE_SNAPSHOT_NAME;
    Path p = Paths.get(snapshotDir, file);
    FileOutputStream fos = new FileOutputStream(p.toFile());
    DataOutputStream dos = new DataOutputStream(fos);
    this.engine.save(dos);
    dos.close();
  }

  /**
   * Save cache data and meta-data
   * @throws IOException
   */
  public void save() throws IOException {
    LOG.info("Started saving cache ...");
    long startTime = System.currentTimeMillis();
    saveCache();
    saveAdmissionController();
    saveThroughputController();
    saveEngine();
    saveScavengerStats();
    if (victimCache != null) {
      victimCache.save();
    }
    long endTime = System.currentTimeMillis();
    LOG.info("Cache saved in {}ms", endTime - startTime);
  }

  /**
   * Load cache data and meta-data from a file system
   * @throws IOException
   */
  public void load() throws IOException {

    try {
      LOG.info("Started loading cache ...");
      long startTime = System.currentTimeMillis();
      loadCache();
      initAllDuringLoad();
      loadAdmissionControlller();
      loadThroughputControlller();
      loadEngine();
      loadScavengerStats();
      startThroughputController();
      startVacuumCleaner();
      long endTime = System.currentTimeMillis();
      LOG.info("Cache loaded in {}ms", endTime - startTime);
    } catch (IOException e) {
      LOG.error("Failed to load cache \"" + this.cacheName +"\" delete cache data directory and retry", e);
      removeShutdownHook();
      throw e;
    }
  }

  /**
   * Load cache from a given directory
   * @param baseDirPath
   * @throws IOException
   */
  public void load(String baseDirPath) throws IOException {
    CacheConfig conf = CacheConfig.getInstance();
    conf.setGlobalCacheRootDir(baseDirPath);
    load();
  }

  // EvictionListener
  @Override
  public void onEviction(long ibPtr, long ptr) {
    // processEviction(ibPtr, ptr);
  }

  @Override
  public boolean onPromotion(long ibPtr, long ptr) {
    // do nothing yet
    // There are several approaches on how to promote items from
    // victim cache back to the parent cache:
    // 1. Promote on every access (GET) - this is what currently done
    // 2. Promote only high ranking items
    // 3. Do not promote back at all
    // 4. Promote randomly
    // ???
    // return processPromotion(ibPtr, ptr);
    return false;
  }

  /**
   * Dispose cache
   */
  public void dispose() {
    removeShutdownHook();
    stopScavengers();
    this.engine.dispose();
    if (this.victimCache != null) {
      this.victimCache.dispose();
    }
  }

  public void printStats() {
    double compRatio = (double) getRawDataSize() / getTotalAllocated();
    double compRatioReal = (double) getRawDataSize() / getStorageUsedActual();
    LOG.info(
      "Cache[{}]: storage size={} data size={} index size={} comp ratio={} comp real={} items={} active items={}"+
    " hit rate={}, gets={}, failed gets={}, puts={}, rejected puts={} bytes written={}",
       this.cacheName, getTotalAllocated(), getRawDataSize(), this.engine.getMemoryIndex().getAllocatedMemory(), compRatio, compRatioReal, size(),
      activeSize(), getHitRate(), getTotalGets(), getTotalFailedGets(), getTotalWrites(), getTotalRejectedWrites(), getTotalWritesSize());
    if (this.victimCache != null) {
      this.victimCache.printStats();
    }
  }

  public void registerJMXMetricsSink() {
    String domainName = this.conf.getJMXMetricsDomainName();
    registerJMXMetricsSink(domainName);
  }

  public void registerJMXMetricsSink(String domainName) {
    MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
    ObjectName name;
    try {
      name = new ObjectName(String.format("%s:type=cache,name=%s", domainName, getName()));
      CacheJMXSink mbean = new CacheJMXSink(this);
      mbs.registerMBean(mbean, name);
    } catch (Exception e) {
      LOG.error("Error:", e);
    }
    if (this.victimCache != null) {
      victimCache.registerJMXMetricsSink(domainName);
    }
  }

  public void registerJMXMetricsSink(String domainName, String type) {
    MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
    ObjectName name;
    try {
      name = new ObjectName(String.format("%s:type=%s,name=%s", domainName, type, getName()));
      CacheJMXSink mbean = new CacheJMXSink(this);
      mbs.registerMBean(mbean, name);
    } catch (Exception e) {
      LOG.error("Error:", e);
    }
    if (this.victimCache != null) {
      victimCache.registerJMXMetricsSink(domainName, type);
    }
  }

  String shutdownStatusMsg;

  public String shutdownStatusMsg() {
    return this.shutdownStatusMsg;
  }

  public long shutdown() throws IOException {

    synchronized (this) {
      if (this.shutdownInProgress) {
        LOG.warn("Shutdown is already in progress");
        return -1;
      }
      this.shutdownInProgress = true;
    }
        
    // Disable writes/reads
    shutdownStatusMsg =
        String.format("Shutting down cache [%s], save data=%s", cacheName, saveOnShutdown);
    LOG.info(shutdownStatusMsg);
    long size = getStorageUsedActual();
    size += this.engine.getMemoryIndex().getAllocatedMemory();
    long start = System.currentTimeMillis();
    stopScavengers();
    // Disable cache
    this.cacheDisabled = true;
    waitForActiveRequestsFinished();
    // stop IOEngine
    this.engine.shutdown();
    if (this.saveOnShutdown) {
      save();
    }
    if (this.victimCache != null) {
      size += this.victimCache.shutdown();
    }
    long end = System.currentTimeMillis();
    if (saveOnShutdown) {
      shutdownStatusMsg =
          String.format("Cache [%s] saved %d bytes in %d ms. Shutdown complete", cacheName, size, (end - start));
    } else {
      shutdownStatusMsg += "Shutdown complete";
    }
    LOG.info(shutdownStatusMsg);
    if (this.saveOnShutdown) {
      // Shutdown LogManager, flush buffered writes
      LogManager.shutdown();
    }
    return size;
  }
  
  private void waitForActiveRequestsFinished() {
    long timeout = 1000;
    long start = System.currentTimeMillis();  
    while(System.currentTimeMillis() - start < timeout && activeRequests.get() > 0) {
      LockSupport.park(100000);
    }
  }
  
  public static Cache loadCache(String cacheName) throws IOException {
    CacheConfig conf = CacheConfig.getInstance();
    String snapshotDir = conf.getSnapshotDir(cacheName);
    Path p = Paths.get(snapshotDir);

    if (Files.notExists(p)) {
      return null;
    }

    if (Files.list(p).count() == 0) {
      return null;
    }
    // Check that all needed files are present in the snapshot directory
    // at least cache, engine and scavenger statistics
    String file = CacheConfig.CACHE_SNAPSHOT_NAME;
    Path cachePath = Paths.get(snapshotDir, file);
    if (Files.notExists(cachePath)) {
      LOG.warn(String.format("Cache snapshot file is missing in %s, creating new instance", p.toString()));
      return null;
    }

    file = CacheConfig.CACHE_ENGINE_SNAPSHOT_NAME;
    Path enginePath = Paths.get(snapshotDir, file);
    if (Files.notExists(enginePath)) {
      LOG.warn(String.format("IOEngine snapshot file is missing in %s, creating new instance", p.toString()));
      return null;
    }

    file = CacheConfig.SCAVENGER_STATS_SNAPSHOT_NAME;
    Path statsPath = Paths.get(snapshotDir, file);
    if (Files.notExists(statsPath)) {
      LOG.warn(String.format("Scavenger statistics snapshot file is missing in %s", p.toString()));
      return null;
    }

    // Ideally we need to check number of files at least
    // TODO: later more stricter verification of a saved cache data
    Cache cache = new Cache();
    cache.setName(cacheName);
    cache.load();

    conf = cache.getCacheConfig();
    // TODO: check if it will work
    String victimCacheName = conf.getVictimCacheName(cacheName);
    if (victimCacheName != null) {
      Cache victimCache = new Cache();
      victimCache.setName(victimCacheName);
      victimCache.load();
      cache.setVictimCache(victimCache);
    }
    return cache;
  }

  public static Cache loadCache(String rootDir, String cacheName) throws IOException {
    CacheConfig conf = CacheConfig.getInstance();
    conf.setGlobalCacheRootDir(rootDir);
    return loadCache(cacheName);
  }
}
