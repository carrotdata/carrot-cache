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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import javax.management.MBeanServer;
import javax.management.ObjectName;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.carrot.cache.Scavenger.Stats;
import com.carrot.cache.controllers.AdmissionController;
import com.carrot.cache.controllers.ThroughputController;
import com.carrot.cache.eviction.EvictionListener;
import com.carrot.cache.index.IndexFormat;
import com.carrot.cache.index.MemoryIndex;
import com.carrot.cache.io.CacheInputStream;
import com.carrot.cache.io.CacheOutputStream;
import com.carrot.cache.io.FileIOEngine;
import com.carrot.cache.io.IOEngine;
import com.carrot.cache.io.IOEngine.IOEngineEvent;
import com.carrot.cache.io.OffheapIOEngine;
import com.carrot.cache.io.Segment;
import com.carrot.cache.jmx.CacheJMXSink;
import com.carrot.cache.util.CarrotConfig;
import com.carrot.cache.util.Epoch;
import com.carrot.cache.util.UnsafeAccess;
import com.carrot.cache.util.Utils;

/**
 * Main entry for off-heap/on-disk cache
 *
 * <p>Memory Index is a dynamic hash table, which implements smart incremental rehashing technique
 * to avoid large pauses during operation.
 *
 * <p>Size of a table is always power of 2'. It starts with size = 64K (configurable)
 *
 * <p>1. Addressing
 *
 * <p>1.1 Each key is hashed into 8 bytes value 
 * <p>1.2 First N bits (2**N is a size of a table) are
 * used to identify a slot number. 
 * <p>1.3 Each slot is 8 bytes and keeps address to an index buffer IB
 * (dynamically sized between 256 bytes and 4KB) 
 * <p>1.4 Each index buffer keeps cached item indexes.
 * First 2 bytes keeps # of indexes
 *
 * <p>0 -> IB(0) 1 -> IB(1)
 *
 * <p>...
 *
 * <p>64535 -> IB (65535)
 *
 * <p>IB(x) -> number indexes (2 bytes) index1 (16 bytes) index2 (16 bytes) index3(16 bytes)
 *
 * <p>1.5 Index is 16 bytes, first 8 bytes is hashed key, second 8 bytes is address of a cached item
 * in a special format: 
 * <p>1.5.1 first 2 bytes - reserved for future eviction algorithms 
 * <p>1.5.2 next 2 bytes - Memory buffer ID (total maximum number of buffers is 64K) 
 * <p>1.5.3 last 4 bytes is offset in
 * a memory buffer (maximum memory buffer size is 4GB)
 *
 * <p>Memory buffer is where cached data is stored. Each cached item has the following format:
 *
 * <p>expiration (8 bytes) key (variable) item (variable)
 *
 * <p>2. Incremental rehashing
 *
 * <p>When some index buffer is filled up (size is 4KB and keeps 255 indexes each 16 bytes long) and
 * no maximum memory limit is reached yet, rehashing process starts. 
 * <p>2.1 N -> N+1 we increment size
 * of a table by factor of 2 
 * <p>2.2 We rehash full slots one - by one once slot reaches its capacity
 * <p>2.3 Slot #K is rehashed into 2 slots in a new hash table: 'K0' and 'K1'
 *
 * <p>Example: let suppose that N = 8 and we rehash slot with ID = 255 (11111111) This slot will be
 * rehashed into 2 slots in a new table: 111111110 and 111111111. ALl keys for these two slots come
 * from a slot 255 from and old table 
 * <p>2.4 System set 'rehashInProgress' and two tables now co-exists
 * <p>2.5 We keep track on how many slots have been rehashed already and based on this number we set
 * the priority on probing both tables as following: old -> new when number of rehashed slots <
 * size(old); new -> old - otherwise 
 * <p>2.6 when rehash finishes, we set old_table = new_table and set
 * rehashInProgress to 'false' 2.6 If during rehashing some slot in a new table reaches maximum
 * capacity - all cache operations are put on hold until old table rehash is finished. This is
 * highly unlikely event, but in theory is possible
 *
 * <p>3. Eviction algorithms
 *
 * <p>3.1 CSLRU - Concurrent Segmented LRU
 *
 * <p>SLOT X: Item1, Item2, ...Item100, ... ItemX (X <= 255)
 *
 * <p>3.1.1 Eviction and item promotion (on hit) happens in a particular table's slot. 
 * <p>3.1.2 Because we have at least 64K slots - there are literally no contention on both: read and insert. We use
 * write locking of a slot. Multiple concurrent threads can read/write index at the same time 
 * <p>3.1.3 When item is hit, his position in a slot memory buffer is changed - it is moved closer to the
 * head of a buffer, how far depends on how many virtual segments in a slot we have. Default is 8.
 * So. if item is located in a segment 6, it will be moved to a head of a segment 5. Item in a
 * segment 1 will be moved to a head of a segment 1.
 *
 * <p>Example:
 *
 * <p>A. We found ITEM in a Slot Y. ITEM has position 100, total number of items in a slot - 128.
 * Therefore virtual segment size is 128/8 = 16. Item is located in segment 7, it will be promoted
 * to a head of segment 6 and its position will change from 100 to 80
 *
 * <p>3.2 CSLRU-WP (with weighted promotion)
 *
 * <p>We take into account accumulated 'importance' of a cached entry when making decision on how
 * large will item promotion be. 2 bytes in a 8 byte index entry are access counters. Every time
 * item is accessed we increment counter. On saturation (if it will ever happen) we reset counter to
 * 0 (?)
 *
 * <p>All cached items are divided by groups: VERY HOT, HOT, WARM, COLD based on a values of their
 * counters. For VERY HOT item promotion on access is going to be larger than for COLD item.
 *
 * <p>This info can be used during item eviction from RAM cache to SSD cache. For example, during
 * eviction COLD items from RAM will be discarded completely, all others will be evicted to SSD
 * cache.
 *
 * <p>4. Insertion point To prevent cache from trashing on scan-like workload, we insert every new
 * item into the head of a segment 7, which is approximately 25% (quarter) distance from a memory
 * buffer tail.
 *
 * <p>5. Handling TTL and freeing the space
 *
 * <p>5.1 Special Scavenger thread is running periodically to clean TTL-expired items and remove
 * cold items to free space for a new items 
 * <p>5.2 All memory buffers are ordered BUF(0) -> BUF(1) ->
 * ... -> BUF(K) in a circular buffer. For the sake of simplicity, let BUF(0) be a head, where new
 * items are inserted into right now. 
 * <p>5.3 Scavenger scans buffers starting from BUF(1) (the least
 * recent ones). It skips TTL-expired items and inserts other ones into BUF(0) ONLY if they are
 * popular enough (if you remember, we separated ALL cached items into 8 segments, where Segment 1
 * is most popular and Segment 8 is the least popular). By default we dump all items which belongs
 * to segments 7 and 8. 
 * <p>5.4 There are two configuration parameters which control Scavenger :
 * minimum_start_capacity (95% by default) and stop_capacity (90%). Scavengers starts running when
 * cache reaches minimum_start_capacity and stops when cache size gets down to stop_capacity. 
 * <p>5.5 During scavenger run, the special rate limiter controls incoming data rate and sets its limit to
 * 90% of a Scavenger cleaning data rate to guarantee that we won't exceed cache maximum capacity
 */
public class Cache implements IOEngine.Listener, EvictionListener {


  
  /** Logger */
  private static final Logger LOG = LogManager.getLogger(Cache.class);
  
  public static enum Type {
    MEMORY, DISK
  }

  /* Total number of accesses (GET)*/
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
  
  /* Cache name */
  String cacheName;
  
  /** Cache configuration */
  CarrotConfig conf;

  /** Maximum memory limit in bytes */
  long maximumCacheSize;

  /** Cache scavenger */
  AtomicReference<Scavenger> scavenger = new AtomicReference<Scavenger>();
  
  /* IOEngine */
  IOEngine engine;
    
  /* Admission Controller - optional */
  AdmissionController admissionController;
  
  /* Throughput controller - optional */
  ThroughputController throughputController;
  
  /* Periodic task runner */
  Timer timer;
  
  /* Victim cache */
  Cache victimCache;
  
  /* Parent cache */
  Cache parentCache;
  
  Epoch epoch;
    
  /* Throughput controller enabled */
  boolean tcEnabled;
  
  /* Index embedding supported */
  boolean indexEmdeddingSupported;
  
  /* Index embedded size */
  int indexEmbeddedSize;
  
  /* Eviction disabled mode */
  boolean evictionDisabledMode;
  
  /* Wait writes threshold */
  double writesMaxWaitTime;
  
  /* Scavenger start memory ratio*/
  double scavengerStartMemoryRatio;
  
  /* Scavenger stop memory ratio*/
  double scavengerStopMemoryRatio;
  
  /* Victim cache promote on hit */
  boolean victimCachePromoteOnHit;
  
  /* Victim cache promote threshold */
  double victimCachePromoteThreshold;
  
  /* Hybrid cache inverse mode */
  boolean hybridCacheInverseMode;
  
  /* Cache spin wait time */
  long spinWaitTime;

  /* Cache type*/
  Type type;
  
  volatile boolean shutdownInProgress = false;
  
  /**
   *  Constructor to use 
   *  when loading cache from a storage
   *  
   *  set cache name after that
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
   * Constructor with configuration
   * 
   * @param conf configuration
   * @throws IOException 
   */
  public Cache(String name) throws IOException {
    this.cacheName = name;
    this.conf = CarrotConfig.getInstance();
    this.engine = IOEngine.getEngineForCache(this);
    // set engine listener
    this.engine.setListener(this);
    this.type = engine instanceof OffheapIOEngine? Type.MEMORY: Type.DISK;
    initAll();
  }

  public Cache(String name, CarrotConfig conf) throws IOException {
    this.cacheName = name;
    this.conf = conf;
    this.engine = IOEngine.getEngineForCache(this);
    // set engine listener
    this.engine.setListener(this);
    this.type = engine instanceof OffheapIOEngine? Type.MEMORY: Type.DISK;
    initAll();
  }
  
  Cache(CarrotConfig conf, String cacheName) {
    this.cacheName = cacheName;
    this.conf = conf;
  }
  
  void setIOEngine(IOEngine engine) {
    this.engine = engine;
    // set engine listener
    this.engine.setListener(this);
    this.type = engine instanceof OffheapIOEngine? Type.MEMORY: Type.DISK;
  }
  
  private void initFromConfiguration() {
    this.indexEmdeddingSupported = 
        this.conf.isIndexDataEmbeddedSupported(this.cacheName);
    this.indexEmbeddedSize = this.conf.getIndexDataEmbeddedSize(this.cacheName);
    this.evictionDisabledMode = this.conf.getEvictionDisabledMode(this.cacheName);
    this.writesMaxWaitTime = this.conf.getCacheWritesMaxWaitTime(this.cacheName);
    this.scavengerStartMemoryRatio = this.conf.getScavengerStartMemoryRatio(this.cacheName);
    this.scavengerStopMemoryRatio = this.conf.getScavengerStopMemoryRatio(this.cacheName);
    this.spinWaitTime = this.conf.getCacheSpinWaitTimeOnHighPressure(this.cacheName);
  }
  
  void initAll() throws IOException {
    initFromConfiguration();
    updateMaxCacheSize();
    initAdmissionController();
    initThroughputController();
    startThroughputController();
    initScavenger();
    // Set eviction listener
    this.engine.getMemoryIndex().setEvictionListener(this);
  }

  private void initAllDuringLoad() throws IOException {
    initFromConfiguration();
    this.engine = this.type == Type.MEMORY?
        new OffheapIOEngine(this.cacheName): new FileIOEngine(this.cacheName);
    // set engine listener
    this.engine.setListener(this);
    updateMaxCacheSize();
    initAdmissionController();
    initThroughputController();
    this.engine.getMemoryIndex().setEvictionListener(this);
  }
  
  public void addShutdownHook() {
    Runtime r = Runtime.getRuntime();
    r.addShutdownHook( new Thread(() -> {
      try {
        shutdown();
      } catch (IOException e) {
        LOG.error(e);
      }
    }));
  }
  
  private void initScavenger() {
    long interval = this.conf.getScavengerRunInterval(this.cacheName) * 1000;
    LOG.info("Started Scavenger, interval=%d sec", interval /1000);
    TimerTask task = new TimerTask() {
      public void run() {
        Scavenger scavenger = Cache.this.scavenger.get();
        if (scavenger != null && scavenger.isAlive()) {
          return;
        }
        // Scavenger MUST be null here, because we first set scavenger to NULL then exit Scavenger thread
        scavenger = new Scavenger(Cache.this);
        if (!Cache.this.scavenger.compareAndSet(null, scavenger)) {
          return;
        }
        scavenger.start();
      }
    };
    if (this.timer == null) {
      this.timer = new Timer();
    }
    this.timer.scheduleAtFixedRate(task, interval, interval);
  }
  
  void startScavenger() {
    Scavenger scavenger = Cache.this.scavenger.get();
    if (scavenger != null && scavenger.isAlive()) {
      return;
    }
    // Scavenger MUST be null here, because we first set scavenger to NULL then exit Scavenger thread
    scavenger = new Scavenger(this);
    if (!this.scavenger.compareAndSet(null, scavenger)) {
      return;
    }
    scavenger.start();
  }
  
  void stopScavenger() {
    Scavenger scavenger = this.scavenger.get();
    if (scavenger != null && scavenger.isAlive()) {
      scavenger.interrupt();
      try {
        scavenger.join();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      }
    }
  }
  
  void finishScavenger(Scavenger s) {
    this.scavenger.compareAndSet(s, null);
  }
  
  private void adjustThroughput() {
    boolean result = this.throughputController.adjustParameters();
    LOG.info("Adjusted throughput controller =" + result);
    this.throughputController.printStats();
  }
  
  private void reportThroughputController(long bytes) {
    if (!this.tcEnabled  || this.throughputController == null) {
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
      LOG.error(e);
      throw new RuntimeException(e);
    }
    if (this.admissionController == null) {
      return;
    }
    this.admissionController.setCache(this);
    LOG.info("Started Admission Controller [%s]", this.admissionController.getClass().getName());

  }

  /**
   * Initialize throughput controller
   *
   * @throws IOException
   */
  private void initThroughputController() throws IOException {
    try {
      this.throughputController = this.conf.getThroughputController(cacheName);
    } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
      LOG.error(e);
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
    TimerTask task =
        new TimerTask() {
          public void run() {
            adjustThroughput();
          }
        };
    if (this.timer == null) {    
      this.timer = new Timer();
    }
    long interval = this.conf.getThroughputCheckInterval(this.cacheName);
    this.timer.scheduleAtFixedRate(task, interval, interval);
    LOG.info("Started throughput controller, interval=%d sec", interval /1000);
  }
  
  private void updateMaxCacheSize() {
    this.maximumCacheSize = this.engine.getMaximumStorageSize();
  }

  /**
   * Get cache type
   * @return cache type
   */
  public Type getCacheType() {
    if (this.engine instanceof OffheapIOEngine) {
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
  public CarrotConfig getCacheConfig() {
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
   *
   * @return used memory
   */
  public long getStorageUsed() {
    return this.engine.getStorageUsed();
  }
  
  /**
   * Get total allocated memory
   *
   * @return total allocated memory
   */
  public long getStorageAllocated() {
    return this.engine.getStorageAllocated();
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
   *
   * @return memory used fraction
   */
  public double getStorageAllocatedRatio() {
    if (this.maximumCacheSize == 0) return 0;
    return (double) getStorageUsed() / this.maximumCacheSize;
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
   *
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
    return put(keyPtr, keySize, valPtr, valSize, expire, rank, false);
  }

  private boolean shouldAdmitToMainQueue(long keyPtr, int keySize, int valueSize,  boolean force) {
    if (!force && this.admissionController != null) {
      return this.admissionController.admit(keyPtr, keySize, valueSize);
    }
    return true;
  }
    
  private boolean isScavengerActive() {
    Scavenger scav = this.scavenger.get();
    return scav != null && scav.isAlive();
  }
  
  private void spinWaitOnHighPressure(boolean scavenger) {
    if (scavenger) {
      return;
    }
    double storageUsed = this.engine.getStorageAllocatedRatio();
    if (storageUsed < this.scavengerStopMemoryRatio || !isScavengerActive()) {
      return;
    }
    Utils.onSpinWait(this.spinWaitTime);
  }
  
  public boolean put(
      long keyPtr, int keySize, long valPtr, int valSize, long expire, int rank, boolean force) throws IOException {
    return put(keyPtr, keySize, valPtr, valSize, expire, rank, force, false);
  }
  
  /**
   * Put item into the cache - API for new items
   *
   * @param keyPtr key address
   * @param keySize key size
   * @param valPtr value address
   * @param valSize value size
   * @param expire expiration (0 - no expire)
   * @param rank rank of the item
   * @param force if true - bypass admission controller
   * @return true on success, false - otherwise
   */
  public boolean put(
      long keyPtr, int keySize, long valPtr, int valSize, long expire, int rank, boolean force, boolean scavenger)
      throws IOException {

    if(shutdownInProgress) {
      return false;
    }
    if (this.victimCache != null && this.hybridCacheInverseMode) {
      return this.victimCache.put(keyPtr, keySize, valPtr, valSize, expire, rank, force, scavenger);
    } else {
      return putDirectly(keyPtr, keySize, valPtr, valSize, expire, rank, force, scavenger);
    }
  }

  private boolean putDirectly(long keyPtr, int keySize, long valPtr, int valSize, long expire,
      int rank, boolean force, boolean scavenger) throws IOException {
    
    if(shutdownInProgress) {
      return false;
    }
    if (storageIsFull(keySize, valSize)) {
      this.totalRejectedWrites.incrementAndGet();
      return false;
    }
    // Check rank
    checkRank(rank);
    if (!shouldAdmitToMainQueue(keyPtr, keySize, valSize, force)) {
      return false;
    }
    spinWaitOnHighPressure(scavenger);
    this.totalWrites.incrementAndGet();
    this.totalWritesSize.addAndGet(Utils.kvSize(keySize, valSize));

    // Adjust rank taking into account item's expiration time
    rank = adjustRank(rank, expire);
    expire = adjustExpirationTime(expire);
    // Add to the cache
    boolean result = false;
    result = engine.put(keyPtr, keySize, valPtr, valSize, expire, rank);
    if (result) {
      reportThroughputController(Utils.kvSize(keySize, valSize));
    }
    return result;
  }

  private void checkRank(int rank) {
    int maxRank = this.engine.getNumberOfRanks();
    if (rank < 0 || rank >= maxRank) {
      throw new IllegalArgumentException(String.format("Items rank %d is illegal"));
    }
  }

  private boolean shouldAdmitToMainQueue(byte[] key, int keyOffset, int keySize, int valueSize, boolean force) {
    if (!force && this.admissionController != null) {
      return this.admissionController.admit(key, keyOffset, keySize, valueSize);
    }
    return true;
  }
  
  private int adjustRank(int rank, long expire) {
    if (this.admissionController != null) {
      // Adjust rank taking into account item's expiration time
      rank = this.admissionController.adjustRank(rank, expire);
    }
    return rank;
  }
  
  private long adjustExpirationTime(long expire) {
    if (this.admissionController != null) {
      expire = this.admissionController.adjustExpirationTime(expire);
    }
    return expire;
  }
  
  public boolean put(
      byte[] key,
      int keyOffset,
      int keySize,
      byte[] value,
      int valOffset,
      int valSize,
      long expire
   )
      throws IOException {
    int rank = getDefaultRankToInsert();
    return put(key, keyOffset, keySize, value, valOffset, valSize, expire, rank, false, false);
  }
  
  public boolean put(
      byte[] key,
      int keyOffset,
      int keySize,
      byte[] value,
      int valOffset,
      int valSize,
      long expire,
      boolean force
   )
      throws IOException {
    int rank = getDefaultRankToInsert();
    return put(key, keyOffset, keySize, value, valOffset, valSize, expire, rank, force, false);
  }
  
  public boolean put(
      byte[] key,
      int keyOffset,
      int keySize,
      byte[] value,
      int valOffset,
      int valSize,
      long expire,
      int rank,
      boolean force)
      throws IOException {
    return put(key, keyOffset, keySize, value, valOffset, valSize, expire, rank, force, false);
  }
  
  /**
   * Put item into the cache
   *
   * @param key key
   * @param keyOffset key offset
   * @param keySize key size
   * @param value value
   * @param valOffset value offset
   * @param valSize value size
   * @param expire - expiration (0 - no expire)
   * @param force if true - bypass admission controller
   * @return true on success, false - otherwise
   */
  public boolean put(
      byte[] key,
      int keyOffset,
      int keySize,
      byte[] value,
      int valOffset,
      int valSize,
      long expire,
      int rank,
      boolean force,
      boolean scavenger)
      throws IOException {
    
    if(shutdownInProgress) {
      return false;
    }
    
    if (this.victimCache != null && this.hybridCacheInverseMode) {
      return this.victimCache.put(key, keyOffset, keySize, value, valOffset, valSize, expire, rank, force, scavenger);
    } else {
      return putDirectly(key, keyOffset, keySize, value, valOffset, valSize, expire, rank, force, scavenger);
    }
    
  }

  private boolean storageIsFull(int keySize, int valueSize) {
    // OK, eviction is disabled
    // check used and maximum storage size
    long used = getStorageUsed();
    long max = getMaximumCacheSize();
    int size = Utils.kvSize(keySize, valueSize);
    return used + size > max;
  }
  
  private boolean putDirectly(
      byte[] key,
      int keyOffset,
      int keySize,
      byte[] value,
      int valOffset,
      int valSize,
      long expire,
      int rank,
      boolean force,
      boolean scavenger)
      throws IOException {

    if(shutdownInProgress) {
      return false;
    }
    if (storageIsFull(keySize, valSize)) {
      this.totalRejectedWrites.incrementAndGet();
      return false;
    }
    if (!shouldAdmitToMainQueue(key, keyOffset, keySize, valSize, force)) {
      return false;
    }
    spinWaitOnHighPressure(scavenger);
    this.totalWrites.incrementAndGet();
    this.totalWritesSize.addAndGet(Utils.kvSize(keySize, valSize));
    // Check rank
    checkRank(rank);
      // Adjust rank
    rank = adjustRank(rank, expire);
    expire = adjustExpirationTime(expire);
    // Add to the cache
    boolean result = false;
    result = engine.put(key, keyOffset, keySize, value, valOffset, valSize, expire, rank);
    if (result) {
      reportThroughputController(Utils.kvSize(keySize, valSize));
    }
    return result;
  }

  
  private boolean put (byte[] buf, int off, long expire) throws IOException {
    int rank = getDefaultRankToInsert();
    int keySize = Utils.readUVInt(buf, off);
    int kSizeSize = Utils.sizeUVInt(keySize);
    int valueSize = Utils.readUVInt(buf, off + kSizeSize);
    int vSizeSize = Utils.sizeUVInt(valueSize);
    return putDirectly(buf, off + kSizeSize + vSizeSize, keySize, buf, 
      off + kSizeSize + vSizeSize + keySize, valueSize, expire, rank, true, false);
  }
  
  private boolean put(long bufPtr, long expire) throws IOException {
    int rank = getDefaultRankToInsert();
    int keySize = Utils.readUVInt(bufPtr);
    int kSizeSize = Utils.sizeUVInt(keySize);
    int valueSize = Utils.readUVInt(bufPtr + kSizeSize);
    int vSizeSize = Utils.sizeUVInt(valueSize);
    return putDirectly(bufPtr + kSizeSize + vSizeSize, keySize, bufPtr 
      + kSizeSize + vSizeSize + keySize, valueSize, expire, rank, true, false);
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
   *
   * @param key key
   * @param value value
   * @param expire - expiration (0 - no expire)
   */
  public boolean put(byte[] key, byte[] value, long expire) throws IOException {
    int rank = getDefaultRankToInsert();
    return put(key, 0, key.length, value, 0, value.length, expire, rank, false);
  }

  /* Get API*/
  
  /**
   * Get cached item (if any)
   *
   * @param keyPtr key address
   * @param keySize key size
   * @param buffer buffer for item
   * @param bufOffset buffer offset
   * @return size of an item (-1 - not found), if is greater than bufSize - retry with a properly
   *     adjusted buffer
   * @throws IOException 
   */
  public long getKeyValue(long keyPtr, int keySize, byte[] buffer, int bufOffset) throws IOException {
    return getKeyValue(keyPtr, keySize, true, buffer, bufOffset);
  }

  /**
   * Get cached item and key (if any)
   *
   * @param keyPtr key address
   * @param keySize key size
   * @param hit if true - its a hit
   * @param buffer buffer for item
   * @param bufOffset buffer offset
   * @return size of an item (-1 - not found), if is greater than bufSize - retry with a properly
   *     adjusted buffer
   * @throws IOException 
   */
  public long getKeyValue(long keyPtr, int keySize, boolean hit, byte[] buffer, int bufOffset) throws IOException {
    long result = -1;
    
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
    if(result < 0 && this.victimCache != null) {
      result = this.victimCache.getKeyValue(keyPtr, keySize, hit, buffer, bufOffset);
      if (this.victimCachePromoteOnHit && result >=0 && result <= buffer.length - bufOffset) {
        // put k-v into this cache, remove it from the victim cache
        MemoryIndex mi = this.victimCache.getEngine().getMemoryIndex();
        double popularity = mi.popularity(keyPtr, keySize);
        if (popularity > this.victimCachePromoteThreshold) {
          long expire = mi.getExpire(keyPtr, keySize);
          boolean res = put(buffer, bufOffset, expire);
          if (res) {
            this.victimCache.delete(keyPtr, keySize);
          }
        }
      } 
    }
    return result;
  }

  /**
   * Get cached item only (if any)
   *
   * @param keyPtr key address
   * @param keySize key size
   * @param hit if true - its a hit
   * @param buffer buffer for item
   * @param bufOffset buffer offset
   * @return size of an item (-1 - not found), if is greater than bufSize - retry with a properly
   *     adjusted buffer
   * @throws IOException 
   */
  public long get(long keyPtr, int keySize, boolean hit, byte[] buffer, int bufOffset) throws IOException {
    int rem = buffer.length - bufOffset;
    long result = getKeyValue(keyPtr, keySize, hit, buffer, bufOffset);
    if (result > 0 && result <= rem) {
      result = Utils.extractValue(buffer, bufOffset);
    }
    return result;
  }
  
  /**
   * Get cached value range (if any)
   *
   * @param keyPtr key address
   * @param keySize key size
   * @param rangeStart range start
   * @param rangeSize range size
   * @param hit if true - its a hit
   * @param buffer buffer for item
   * @param bufOffset buffer offset
   * @return size of an item (-1 - not found), if is greater than bufSize - retry with a properly
   *     adjusted buffer
   * @throws IOException 
   */
  public long getRange(long keyPtr, int keySize, int rangeStart, int rangeSize, boolean hit, 
      byte[] buffer, int bufOffset) throws IOException {
    long result = -1;
    try {
      result = engine.getRange(keyPtr, keySize, rangeStart, rangeSize, hit, buffer, bufOffset);
    } catch (IOException e) {
      // IOException is possible
      //TODO: better mitigation 
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

    if(result < 0 && this.victimCache != null) {
      result = this.victimCache.getRange(keyPtr, keySize, rangeStart, rangeSize, hit, buffer, bufOffset);
      // For range queries we do not promote item to the parent cache
    }
    return result;
  }
  
  /**
   * Get cached item (with hit == true)
   * @param key key buffer
   * @param keyOffset key offset
   * @param keySize key size
   * @param buffer buffer for item
   * @param bufOffset buffer offset
   * @return size of an item (-1 - not found), if is greater than bufSize - retry with a properly
   *     adjusted buffer
   * @throws IOException
   */
  public long getKeyValue(byte[] key, int keyOffset, int keySize, byte[] buffer, int bufOffset) 
      throws IOException {
    return getKeyValue(key, keyOffset, keySize, true, buffer, bufOffset);
  }
    
  /**
   * Get cached item and key (if any)
   *
   * @param key key buffer
   * @param keyOfset key offset
   * @param keySize key size
   * @param hit if true - its a hit
   * @param buffer buffer for item
   * @param bufSize buffer offset
   * @return size of an item (-1 - not found), if is greater than bufSize - retry with a properly
   *     adjusted buffer
   * @throws IOException 
   */
  public long getKeyValue(byte[] key, int keyOffset, int keySize, boolean hit, byte[] buffer, int bufOffset) 
      throws IOException {
    
    long result = -1;
    try {
      result = engine.get(key, keyOffset, keySize, hit, buffer, bufOffset);
    } catch (IOException e) {
      // IOException is possible
      //TODO: better mitigation 
      return result;
    }
    
    if (result <= buffer.length - bufOffset) {
      access();
      if (result >= 0) {
        hit(result);
      }
    }
    if (result >=0 && result <= buffer.length - bufOffset) {
      if (this.admissionController != null) {
        this.admissionController.access(key, keyOffset, keySize);
      }
    }
    if(result < 0 && this.victimCache != null) {
      //TODO: optimize it
      // getWithExpire and getWithExpireAndDelete API
      // one call instead of three
      result = this.victimCache.getKeyValue(key, keyOffset, keySize, hit, buffer, bufOffset);
      if (this.victimCachePromoteOnHit && result >= 0 && result <= buffer.length - bufOffset) {
        // put k-v into this cache, remove it from the victim cache
        MemoryIndex mi = this.victimCache.getEngine().getMemoryIndex();
        double popularity = mi.popularity(key, keyOffset, keySize);
        // Promote only popular items
        if (popularity > this.victimCachePromoteThreshold) {
          long expire = mi.getExpire(key, keyOffset, keySize);
          boolean res = put(buffer, bufOffset, expire);
          if (res) {
            this.victimCache.delete(key, keyOffset, keySize);
          }
        }
      } 
    }
    return result;
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
    return  CacheInputStream.openStream(this, key, off, len);
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
   *
   * @param key key buffer
   * @param keyOfset key offset
   * @param keySize key size
   * @param hit if true - its a hit
   * @param buffer buffer for item
   * @param bufSize buffer offset
   * @return size of an item (-1 - not found), if is greater than bufSize - retry with a properly
   *     adjusted buffer
   * @throws IOException 
   */
  public long get(byte[] key, int keyOffset, int keySize, boolean hit, byte[] buffer, int bufOffset) 
      throws IOException {
    
    int rem = buffer.length - bufOffset;
    long result = getKeyValue(key, keyOffset, keySize, hit, buffer, bufOffset);
    if (result > 0 && result <= rem) {
      result = Utils.extractValue(buffer, bufOffset);
    }
    return result;
  }

  /**
   * Get cached value range
   *
   * @param key key buffer
   * @param keyOfset key offset
   * @param keySize key size
   * @param rangeStart range start
   * @param rangeSize range size
   * @param hit if true - its a hit
   * @param buffer buffer for item
   * @param bufSize buffer offset
   * @return size of an item (-1 - not found), if is greater than bufSize - retry with a properly
   *     adjusted buffer
   * @throws IOException 
   */
  public long getRange(byte[] key, int keyOffset, int keySize,
      int rangeStart, int rangeSize, boolean hit, byte[] buffer, int bufOffset) 
      throws IOException {
    
    long result = -1;
    try {
      result = engine.getRange(key, keyOffset, keySize, rangeStart, rangeSize, hit, buffer, bufOffset);
    } catch (IOException e) {
      // IOException is possible
      //TODO: better mitigation 
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

    if(result < 0 && this.victimCache != null) {
      result = this.victimCache.getRange(key, keyOffset, keySize, rangeStart, rangeSize, hit, buffer, bufOffset);
      // For range queries we do not promote item to the parent cache
    }
    return result;
  }
  
  /**
   * Get cached item (if any)
   *
   * @param key key buffer
   * @param keyOff key offset
   * @param keySize key size
   * @param hit if true - its a hit
   * @param buffer byte buffer for item
   * @return size of an item (-1 - not found), if is greater than bufSize - retry with a properly
   *     adjusted buffer
   * @throws IOException 
   */
  public long getKeyValue(byte[] key, int keyOff, int keySize, ByteBuffer buffer) 
      throws IOException {
    return getKeyValue(key, keyOff, keySize, true, buffer);
  }
  
  /**
   * Get cached item and key (if any)
   *
   * @param key key buffer
   * @param keyOff key offset
   * @param keySize key size
   * @param hit if true - its a hit
   * @param buffer byte buffer for item
   * @return size of an item (-1 - not found), if is greater than bufSize - retry with a properly
   *     adjusted buffer
   * @throws IOException 
   */
  public long getKeyValue(byte[] key, int keyOff, int keySize, boolean hit, ByteBuffer buffer) 
      throws IOException {
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
    if(result < 0 && this.victimCache != null) {
      result = this.victimCache.getKeyValue(key, keyOff, keySize, hit, buffer);
      if (this.victimCachePromoteOnHit && result >= 0 && result <= rem) {
        // put k-v into this cache, remove it from the victim cache
        MemoryIndex mi = this.victimCache.getEngine().getMemoryIndex();
        double popularity = mi.popularity(key, keyOff, keySize);
        if (popularity > this.victimCachePromoteThreshold) {
          long expire = mi.getExpire(key, keyOff, keySize);
          boolean res = put(buffer, expire);
          if (res) {
            this.victimCache.delete(key, keyOff, keySize);
          }
        }
      } 
    }
    return result;
  }

  /**
   * Get cached item value only (if any)
   *
   * @param key key buffer
   * @param keyOff key offset
   * @param keySize key size
   * @param hit if true - its a hit
   * @param buffer byte buffer for item
   * @return size of an item (-1 - not found), if is greater than bufSize - retry with a properly
   *     adjusted buffer
   * @throws IOException 
   */
  public long get(byte[] key, int keyOff, int keySize, boolean hit, ByteBuffer buffer) 
      throws IOException {
    int rem = buffer.remaining();
    long result = getKeyValue(key, keyOff, keySize, hit, buffer);
    if (result > 0 && result <= rem) {
      result = Utils.extractValue(buffer);
    }
    return result;
  }
  
  /**
   * Get cached value range (if any)
   *
   * @param key key buffer
   * @param keyOff key offset
   * @param keySize key size
   * @param rangeStart range start
   * @param rangeSize range size
   * @param hit if true - its a hit
   * @param buffer byte buffer for item
   * @return size of an item (-1 - not found), if is greater than bufSize - retry with a properly
   *     adjusted buffer
   * @throws IOException 
   */
  public long getRange(byte[] key, int keyOffset, int keySize, int rangeStart, int rangeSize, boolean hit, ByteBuffer buffer) 
      throws IOException {
    long result = -1;
    int rem = buffer.remaining();
    try {
      result = engine.getRange(key, keyOffset, keySize, rangeStart, rangeSize, hit, buffer);
    } catch (IOException e) {
      // IOException is possible
      //TODO: better mitigation 
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

    if(result < 0 && this.victimCache != null) {
      result = this.victimCache.getRange(key, keyOffset, keySize, rangeStart, rangeSize, hit, buffer);
      // For range queries we do not promote item to the parent cache
    }
    return result;
  }
  
  /**
   * Get cached item (if any)
   *
   * @param keyPtr key address
   * @param keySize key size
   * @param buffer byte buffer for item
   * @return size of an item (-1 - not found), if is greater than bufSize - retry with a properly
   *     adjusted buffer
   * @throws IOException 
   */
  public long getKeyValue(long keyPtr, int keySize,  ByteBuffer buffer) 
      throws IOException  {
    return getKeyValue(keyPtr, keySize, true, buffer);
  }
  
  /**
   * Get cached item and key (if any)
   *
   * @param keyPtr key address
   * @param keySize key size
   * @param hit if true - its a hit
   * @param buffer byte buffer for item
   * @return size of an item (-1 - not found), if is greater than bufSize - retry with a properly
   *     adjusted buffer
   * @throws IOException 
   */
  public long getKeyValue(long keyPtr, int keySize, boolean hit, ByteBuffer buffer) 
      throws IOException {
    int rem = buffer.remaining();
    
    long result = -1;
    
    try {
      result = this.engine.get(keyPtr, keySize, hit, buffer);
    } catch(IOException e) {
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
    if(result < 0 && this.victimCache != null) {
      result = this.victimCache.getKeyValue(keyPtr, keySize, hit, buffer);
      if (this.victimCachePromoteOnHit && result >=0 && result <= rem) {
        // put k-v into this cache, remove it from the victim cache
        MemoryIndex mi = this.victimCache.getEngine().getMemoryIndex();
        double popularity = mi.popularity(keyPtr, keySize);
        if (popularity > this.victimCachePromoteThreshold) {
          long expire = mi.getExpire(keyPtr, keySize);
          boolean res = put(buffer, expire);
          if (res) {
            this.victimCache.delete(keyPtr, keySize);
          }
        }
      } 
    }
    return result;
  }

  /**
   * Get cached item only (if any)
   *
   * @param keyPtr key address
   * @param keySize key size
   * @param hit if true - its a hit
   * @param buffer byte buffer for item
   * @return size of an item (-1 - not found), if is greater than bufSize - retry with a properly
   *     adjusted buffer
   * @throws IOException 
   */
  public long get(long keyPtr, int keySize, boolean hit, ByteBuffer buffer) 
      throws IOException {
    int rem = buffer.remaining();
    
    long result = getKeyValue(keyPtr, keySize, hit, buffer);
    if (result <= rem && result > 0) {
      result = Utils.extractValue(buffer);
    }
    return result;
  }
  
  
  /**
   * Get cached value range (if any)
   *
   * @param keyPtr key address
   * @param keySize key size
   * @param rangeStart range start
   * @param rangeSize range size
   * @param hit if true - its a hit
   * @param buffer byte buffer for item
   * @return size of an item (-1 - not found), if is greater than bufSize - retry with a properly
   *     adjusted buffer
   * @throws IOException 
   */
  public long getRange(long keyPtr, int keySize, int rangeStart, int rangeSize, boolean hit, ByteBuffer buffer) 
      throws IOException {
    long result = -1;
    int rem = buffer.remaining();
    try {
      result = engine.getRange(keyPtr, keySize, rangeStart, rangeSize, hit, buffer);
    } catch (IOException e) {
      // IOException is possible
      //TODO: better mitigation 
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

    if(result < 0 && this.victimCache != null) {
      result = this.victimCache.getRange(keyPtr, keySize, rangeStart, rangeSize, hit, buffer);
      // For range queries we do not promote item to the parent cache
    }
    return result;
  }
  
  /* Delete API*/

  /**
   * Delete cached item
   *
   * @param keyPtr key address
   * @param keySize key size
   * @return true - success, false - does not exist
   * @throws IOException 
   */
  public boolean delete(long keyPtr, int keySize) throws IOException {
    boolean result = engine.delete(keyPtr, keySize);
    if (!result && this.victimCache != null) {
      return this.victimCache.delete(keyPtr, keySize);
    }
    return result;
  }

  /**
   * Delete cached item
   *
   * @param key key
   * @param keyOffset key offset
   * @param keySize key size
   * @return true - success, false - does not exist
   * @throws IOException 
   */
  public boolean delete(byte[] key, int keyOffset, int keySize) throws IOException {
    boolean result = engine.delete(key, keyOffset, keySize);
    if (!result && this.victimCache != null) {
      return this.victimCache.delete(key, keyOffset, keySize);
    }
    return result;
  }

  /**
   * Delete cached item
   *
   * @param key key
   * @param keyOffset key offset
   * @param keyLength key size
   * @return true - success, false - does not exist
   * @throws IOException 
   */
  public boolean delete(byte[] key) throws IOException {
    return delete(key, 0, key.length);
  }
  
  /**
   * Expire cached item
   *
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
   *
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
   *
   * @param key key
   * @param keyOffset key offset
   * @param keyLength key size
   * @return true - success, false - does not exist
   * @throws IOException 
   */
  public boolean expire(byte[] key) throws IOException {
    return delete(key, 0, key.length);
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
   * Does key exist
   * @param key key buffer
   * @param off key offset
   * @param size key size
   * @return true or false
   */
  public boolean exists(byte[] key, int off, int size) {
    return this.engine.getMemoryIndex().exists(key, off, size);
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
      //TODO: 
      LOG.error(e);
    }
    return true;
  }

  private void processEviction(long ptr, long $ptr) {
    if (this.victimCache == null) {
     return;
    }
    if (this.admissionController != null && 
        !this.admissionController.shouldEvictToVictimCache(ptr, $ptr)) {
      return;
    }
    IndexFormat indexFormat = this.engine.getMemoryIndex().getIndexFormat();
    int size = indexFormat.fullEntrySize($ptr);
    try {
      // Check embedded mode
      //FIXME: this calls are expensive 
      //FIXME: this code is wrong
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
      LOG.error(e);
    }
  }
  /**
   * Transfer cached item to a victim cache
   * @param ibPtr index block pointer
   * @param indexPtr item pointer
   * @throws IOException 
   */
  public void transferToCache (Cache c, long ibPtr, long indexPtr) throws IOException {
    if (getCacheType() == Type.DISK) {
      LOG.error("Attempt to transfer cached item from cache type = DISK");
      throw new IllegalArgumentException("Victim cache is not supported for DISK type cache");
    }

    // Cache is off-heap 
    IndexFormat format = this.engine.getMemoryIndex().getIndexFormat(); 
    long expire = format.getExpire(ibPtr, indexPtr);
    int rank = this.victimCache.getDefaultRankToInsert();
    int sid = (int) format.getSegmentId(indexPtr);
    long offset = format.getOffset(indexPtr); 
    
    Segment s = this.engine.getSegmentById(sid);
    //TODO : check segment
    try {
      s.readLock();
      if (s.isOffheap()) {
        long ptr = s.getAddress();
        ptr += offset;
        int keySize = Utils.readUVInt(ptr);
        int kSizeSize = Utils.sizeUVInt(keySize);
        ptr += kSizeSize;
        int valueSize = Utils.readUVInt(ptr);
        int vSizeSize = Utils.sizeUVInt(valueSize);
        ptr += vSizeSize;
        // Do not force PUT, let victim's cache admission controller work
        this.victimCache.put(ptr, keySize, ptr + keySize, valueSize, expire, rank, false);
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
   *
   * @param ibPtr index block pointer
   * @param indexPtr item pointer
   * @throws IOException
   */
  public void transferEmbeddedToCache(Cache c, long ibPtr, long indexPtr) throws IOException {
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
    c.put(indexPtr, kSize, indexPtr + kSize, vSize, expire, rank, false);
  }

  // IOEngine.Listener
  @Override
  public void onEvent(IOEngine e, IOEngineEvent evt) {
    if (evt == IOEngineEvent.DATA_SIZE_CHANGED) {
      double used = this.engine.getStorageAllocatedRatio();
      //TODO: performance
      double max = this.conf.getScavengerStartMemoryRatio(this.cacheName);
      double min = this.conf.getScavengerStopMemoryRatio(this.cacheName);
      if (this.evictionDisabledMode) {
        return;
      }
      if (used >= max) {
        this.engine.setEvictionEnabled(true);
        this.tcEnabled = true;
        startScavenger();
      } else if (used < min){
        this.engine.setEvictionEnabled(false);
      }
    }
  }
  
  // Persistence section
  
  /**
   * Loads cache meta data
   * @throws IOException
   */
  private void loadCache() throws IOException {
    CarrotConfig conf = CarrotConfig.getInstance();
    String snapshotDir = conf.getSnapshotDir(this.cacheName);
    String file = CarrotConfig.CACHE_SNAPSHOT_NAME;
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
      CarrotConfig.merge(props);
      this.conf = CarrotConfig.getInstance();
      dis.close();
    } else {
      throw new IOException(String.format("Can not load cache. Path %s does not exists",
        p.toString()));
    }
  }
  
  /**
   * Saves cache meta data
   * @throws IOException
   */
  private void saveCache() throws IOException {
    String snapshotDir = this.conf.getSnapshotDir(this.cacheName);
    String file = CarrotConfig.CACHE_SNAPSHOT_NAME;
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
    String file = CarrotConfig.ADMISSION_CONTROLLER_SNAPSHOT_NAME;
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
    String file = CarrotConfig.ADMISSION_CONTROLLER_SNAPSHOT_NAME;
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
    String file = CarrotConfig.THROUGHPUT_CONTROLLER_SNAPSHOT_NAME;
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
    String file = CarrotConfig.THROUGHPUT_CONTROLLER_SNAPSHOT_NAME;
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
    String file = CarrotConfig.SCAVENGER_STATS_SNAPSHOT_NAME;
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
    String file = CarrotConfig.SCAVENGER_STATS_SNAPSHOT_NAME;
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
    String file = CarrotConfig.CACHE_ENGINE_SNAPSHOT_NAME;
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
    String file = CarrotConfig.CACHE_ENGINE_SNAPSHOT_NAME;
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
    // Cache cache = new Cache();
    // cache.setName(name);
    // cache.load();
    // ready to rumble
    LOG.info("Started loading cache ...");
    long startTime = System.currentTimeMillis();
    loadCache();
    initAllDuringLoad();
    loadAdmissionControlller();
    loadThroughputControlller();
    loadEngine();
    loadScavengerStats();
    startThroughputController();
    initScavenger();
    long endTime = System.currentTimeMillis();
    LOG.info("Cache loaded in {}ms", endTime - startTime);
  }

  /**
   * Load cache from a given directory
   * @param baseDirPath
   * @throws IOException
   */
  public void load(String baseDirPath) throws IOException {
    CarrotConfig conf = CarrotConfig.getInstance();
    conf.setGlobalCacheRootDir(baseDirPath);
    load();
  }
  
  // EvictionListener
  @Override
  public void onEviction(long ibPtr, long ptr) {
    processEviction(ibPtr, ptr);
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
    //return processPromotion(ibPtr, ptr);
    return false;
  }

  /**
   * Dispose cache
   */
  public void dispose() {
    // 1 cancel the timer
    this.timer.cancel();
    stopScavenger();
    this.engine.dispose();
    if (this.victimCache != null) {
      this.victimCache.dispose();
    }
  }
  
  public void printStats() {
    System.out.printf("Cache[%s]: storage size=%d data size=%d items=%d hit rate=%f, puts=%d, bytes written=%d\n",
      this.cacheName, getStorageAllocated(), getStorageUsed(), size(), 
      getHitRate(), getTotalWrites(), getTotalWritesSize());
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
      name = new ObjectName(String.format("%s:type=cache,name=%s",domainName, getName()));
      CacheJMXSink mbean = new CacheJMXSink(this);
      mbs.registerMBean(mbean, name); 
    } catch (Exception e) {
      LOG.error(e);
    }
    if (this.victimCache != null) {
      victimCache.registerJMXMetricsSink(domainName);
    }
  }
  
  public void shutdown() throws IOException {
    // Disable writes/reads
    this.shutdownInProgress = true;
    this.timer.cancel();
    stopScavenger();
    // stop IOEngine
    this.engine.shutdown();
    save();
    if (this.victimCache != null) {
      this.victimCache.shutdown();
    }
  }
  
  public static Cache loadCache(String cacheName) throws IOException {
    CarrotConfig conf = CarrotConfig.getInstance();
    String snapshotDir = conf.getSnapshotDir(cacheName);
    Path p = Paths.get(snapshotDir);
    if (Files.notExists(p)) {
      return null;
    }
    if(Files.list(p).count() == 0) {
      return null;
    }
    // Check that all needed files are present in the snapshot directory
    // at least cache, engine and scavenger statistics
    String file = CarrotConfig.CACHE_SNAPSHOT_NAME;
    Path cachePath = Paths.get(snapshotDir, file);
    if (Files.notExists(cachePath)) {
      throw new IOException(String.format("Cache snapshot file is missing in %s", p.toString()));
    }
    
    file = CarrotConfig.CACHE_ENGINE_SNAPSHOT_NAME;
    Path enginePath = Paths.get(snapshotDir, file);
    if (Files.notExists(enginePath)) {
      throw new IOException(String.format("IOEngine snapshot file is missing in %s", p.toString()));
    }
    
    file = CarrotConfig.SCAVENGER_STATS_SNAPSHOT_NAME;
    Path statsPath = Paths.get(snapshotDir, file);
    if (Files.notExists(statsPath)) {
      throw new IOException(String.format("Scavenger statistics snapshot file is missing in %s", p.toString()));
    }
        
    // Ideally we need to check number of files at least
    //TODO: later more stricter verification of a saved cache data
    Cache cache = new Cache();
    cache.setName(cacheName);
    cache.load();
    
    conf = cache.getCacheConfig();
    //TODO: check if it will work
    String victimCacheName = conf.getVictimCacheName(cacheName);
    if (victimCacheName != null) {
      Cache victimCache = new Cache();
      victimCache.setName(victimCacheName);
      victimCache.load();
      cache.setVictimCache(victimCache);
    }
    return cache;
  }
  
  public static Cache loadCache(String rootDir, String cacheName) throws IOException{
    CarrotConfig conf = CarrotConfig.getInstance();
    conf.setGlobalCacheRootDir(rootDir);
    return loadCache(cacheName);
  }
  
  
}
