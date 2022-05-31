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
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.carrot.cache.controllers.AdmissionController;
import com.carrot.cache.controllers.BaseThroughputController;
import com.carrot.cache.controllers.ThroughputController;
import com.carrot.cache.index.IndexFormat;
import com.carrot.cache.index.MemoryIndex;
import com.carrot.cache.io.FileIOEngine;
import com.carrot.cache.io.IOEngine;
import com.carrot.cache.io.OffheapIOEngine;
import com.carrot.cache.io.Segment;
import com.carrot.cache.io.IOEngine.IOEngineEvent;
import com.carrot.cache.util.CacheConfig;
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
public class Cache implements IOEngine.Listener, Scavenger.Listener {

  /** Logger */
  private static final Logger LOG = LogManager.getLogger(Cache.class);
  
  public static enum Type {
    MEMORY, DISK
  }
  /**
   * Basic cache admission controller. For newly admitted items it always
   * directs them to the Admission queue. For evicted and re-admitted items 
   * it checks  access counter and if it greater than 0 item gets re-admitted 
   * to the Admission queue, otherwise - it is dumped. 
   * It does nothing on item access.
   *
   */
  public static class BaseAdmissionController implements AdmissionController {
    
    private Cache cache;
    private boolean fileCache = false;
    
    /* Section for keeping data to adjust rank according item TTL */
    
    private long[] ttlCounts;
    private long[] cumTtl;
    
    private AtomicLongArray avgTTL;
    
    public BaseAdmissionController() {
      
    }
    
    /**
     * Constructor
     * @param cache
     */
    public BaseAdmissionController(Cache cache) {
      this.cache = cache;
      int ranks = cache.getCacheConfig().getNumberOfRanks(cache.getName());
      this.ttlCounts= new long[ranks];
      this.cumTtl = new long[ranks];
      this.avgTTL = new AtomicLongArray(ranks);
      this.fileCache = cache.getEngine() instanceof FileIOEngine;
    }
    
    @Override
    public void setCache(Cache cache) {
      this.cache = cache;
      int ranks = cache.getCacheConfig().getNumberOfRanks(cache.getName());
      this.ttlCounts= new long[ranks];
      this.cumTtl = new long[ranks];
      this.avgTTL = new AtomicLongArray(ranks);
      this.fileCache = cache.getEngine() instanceof FileIOEngine;
    }
    /**
     * New items are always submitted to the Admission Queue
     */
    @Override
    public Type admit(long keyPtr, int keySize) {
      return this.fileCache? Type.AQ: Type.MQ;
    }
    
    /**
     * New items are always submitted to the Admission Queue
     */
    @Override
    public Type admit(byte[] key, int off, int size) {
      return this.fileCache? Type.AQ: Type.MQ;
    }

    @Override
    public Type readmit(long keyPtr, int keySize) {
      if (!this.fileCache) {
        return Type.DUMP;
      }
      //TODO: this code relies on a particular index format
      MemoryIndex index = this.cache.getEngine().getMemoryIndex();
      int hitCount = index.getHitCount(keyPtr, keySize);
      if (hitCount == -1) {
        //TODO: in theory a legitimate reference can be -1?
        return Type.DUMP;
      }
      // Top 2 bytes stores hit counter
      return hitCount > 0? Type.AQ: Type.DUMP;
    }

    @Override
    public Type readmit(byte[] key, int off, int size) {
      if (!this.fileCache) {
        return Type.DUMP;
      }
      MemoryIndex index = this.cache.getEngine().getMemoryIndex();
      int hitCount = index.getHitCount(key, off, size);
      if (hitCount == -1) {
        // means - not found
        //TODO: make sure that ref is still in the index
        return Type.DUMP;
      }
      return hitCount > 0? Type.AQ: Type.DUMP; 
    }

    @Override
    public void access(byte[] key, int off, int size) {
      // nothing yet
    }
    
    @Override
    public void access(long keyPtr, int keySize) {
      // nothing yet
    }

    @Override
    public void save(OutputStream os) throws IOException {
      // save TTL stuff
      DataOutputStream dos= Utils.toDataOutputStream(os);
      int ranks = this.ttlCounts.length;
      dos.writeInt(ranks);
      for (int i = 0; i < ranks; i++) {
        dos.writeLong(this.ttlCounts[i]);
      }
      for (int i = 0; i < ranks; i++) {
        dos.writeLong(this.cumTtl[i]);
      }
    }

    @Override
    public void load(InputStream is) throws IOException {
      DataInputStream dis = Utils.toDataInputStream(is);
      int ranks = dis.readInt();
      this.ttlCounts = new long[ranks];
      this.cumTtl = new long[ranks];
      for(int i = 0; i < ranks; i++) {
        this.ttlCounts[i] = dis.readLong();
      }
      for(int i = 0; i < ranks; i++) {
        this.cumTtl[i] = dis.readLong();
      }
      this.avgTTL = new AtomicLongArray(ranks);
      
    }

    @Override
    public int adjustRank(int rank, long expire /* relative - not absolute in ms*/) {
      int ranks = this.ttlCounts.length;
      int minRank = rank;
      long minPositive = this.avgTTL.get(rank -1) - expire;
      if (minPositive <= 0) return minRank;

      for (int i = rank; i < ranks; i++) {
        long expDiff = this.avgTTL.get(i) - expire;
        if (expDiff <= 0) {
          return minRank;
        } else if (expDiff < minPositive) {
          minPositive = expDiff;
          minRank = i + 1;
        }
      }
      return minRank;
    }

    @Override
    public synchronized void registerSegmentTTL(int rank, long ttl) {
      // rank is 1 - based
      this.ttlCounts[rank - 1] ++;
      this.cumTtl[rank - 1] += ttl;
      this.avgTTL.set(rank - 1, this.cumTtl[rank - 1] / this.cumTtl[rank - 1]);
    }
  }
  /*
   * Total allocated memory (bytes)
   */
  private AtomicLong allocatedMemory = new AtomicLong(0);

  /** Total used memory (bytes) */
  private AtomicLong usedMemory = new AtomicLong(0);

  /* Total number of accesses (GET)*/
  private AtomicLong totalGets = new AtomicLong(0);
  
  /* Total hits */
  private AtomicLong totalHits = new AtomicLong(0);
  
  /* Total writes */
  private AtomicLong totalWrites = new AtomicLong(0);
  
  /* Total rejected writes */
  private AtomicLong totalRejectedWrites = new AtomicLong(0);
  
  /* Cache name */
  String cacheName;
  
  /** Cache configuration */
  CacheConfig conf;

  /** Maximum memory limit in bytes */
  long maximumCacheSize;

  /** Cache scavenger */
  Scavenger scavenger;

  /* IOEngine */
  IOEngine engine;
  
  /* Admission Queue */
  AdmissionQueue admissionQueue;
  
  /* Admission Controller */
  AdmissionController admissionController;
  
  /* Throughput controller */
  ThroughputController throughputController;
  
  /* Periodic throughput adjustment task */
  Timer timer;
  
  /* Victim cache */
  Cache victimCache;
  
  /* Threshold to reject writes */
  double writeRejectionThreshold;
  /**
   * Constructor with configuration
   *
   * @param conf configuration
   * @throws IOException 
   */
  Cache(String name, CacheConfig conf) throws IOException {
    this.cacheName = name;
    this.conf = conf;
    this.engine = IOEngine.getEngineForCache(this);
    // set engine listener
    this.engine.setListener(this);
    this.writeRejectionThreshold = this.conf.getCacheWriteRejectionThreshold(this.cacheName);
    updateMaxCacheSize();
    initAll();
  }
  
  private void initAll() throws IOException {

    initAdmissionController();
    if (this.engine instanceof FileIOEngine) {
      initAdmissionQueue();
      initThroughputController();
      TimerTask task = new TimerTask() {
        public void run() {
          adjustThroughput();
        }
      };
      this.timer = new Timer();
      long interval = this.conf.getThroughputCheckInterval(this.cacheName);
      this.timer.scheduleAtFixedRate(task, interval, interval);
    } 
  }
  
  private void adjustThroughput() {
    boolean result = this.throughputController.adjustParameters();
    LOG.info("Adjusted throughput controller =" + result);
    this.throughputController.printStats();
  }
  
  /**
   * Initialize admission queue
   * @throws IOException 
   */
  private void initAdmissionQueue() throws IOException {
    this.admissionQueue = new AdmissionQueue(this, conf);
    String snapshotDir = this.conf.getSnapshotDir(this.cacheName);
    String file = CacheConfig.ADMISSION_QUEUE_SNAPSHOT_NAME;
    Path p = Paths.get(snapshotDir, file);
    if (Files.exists(p)) {
      FileInputStream fis = new FileInputStream(p.toFile());
      this.admissionQueue.load(fis);
      fis.close();
    }
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
    this.admissionController.setCache(this);
    
    String snapshotDir = this.conf.getSnapshotDir(this.cacheName);
    String file = CacheConfig.ADMISSION_CONTROLLER_SNAPSHOT_NAME;
    Path p = Paths.get(snapshotDir, file);
    if (Files.exists(p)) {
      FileInputStream fis = new FileInputStream(p.toFile());
      this.admissionController.load(fis);
      fis.close();
    }
  }
  
  /**
   * Initialize throughput controller
   * @throws IOException
   */
  private void initThroughputController() throws IOException {
    //TODO: custom controller class
    this.throughputController = new BaseThroughputController(this);
    String snapshotDir = this.conf.getSnapshotDir(this.cacheName);
    String file = CacheConfig.THROUGHPUT_CONTROLLER_SNAPSHOT_NAME;
    Path p = Paths.get(snapshotDir, file);
    if (Files.exists(p)) {
      FileInputStream fis = new FileInputStream(p.toFile());
      this.throughputController.load(fis);
      fis.close();
    }
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
  public CacheConfig getCacheConfig() {
    return this.conf;
  }
  
  /**
   * Get admission queue
   * @return admission queue
   */
  public AdmissionQueue getAdmissionQueue() {
    return this.admissionQueue;
  }
  
  /**
   * Get IOEngine
   * @return engine
   */
  public IOEngine getEngine() {
    return this.engine;
  }
  
  /**
   * Report used memory (can be negative if free())
   *
   * @param size allocated memory size
   */
  public long reportUsed(long size) {
    return this.usedMemory.addAndGet(size);
  }

  /**
   * Report used memory (can be negative if free())
   * @param keySize key size
   * @param valSize value size
   * @return used memory size
   */
  public long reportUsed(int keySize, int valSize) {
    return reportUsed(Utils.kvSize(keySize, valSize));
  }
  
  /**
   * Get total used memory
   *
   * @return used memory
   */
  public long getUsedMemory() {
    return usedMemory.get();
  }

  /**
   * Report allocated memory (can be negative if free())
   *
   * @param size allocated memory size
   */
  public long reportAllocation(long size) {
    return this.allocatedMemory.addAndGet(size);
  }

  /**
   * Get total allocated memory
   *
   * @return
   */
  public long getAllocatedMemory() {
    return allocatedMemory.get();
  }

  /**
   * Gets memory limit
   * @return memory limit in bytes
   */
  public long getMaximumCacheSize() {
    return this.maximumCacheSize;
  }
  

  /**
   * Get memory used as a fraction of memoryLimit
   *
   * @return memory used fraction
   */
  public double getMemoryUsedPct() {
    return ((double) getUsedMemory()) / maximumCacheSize;
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
  
  private void access() {
    this.totalGets.incrementAndGet();
  }
  
  private void hit() {
    this.totalHits.incrementAndGet();
  }
  
  /***********************************************************
   * Cache API
   */

  /* Put API */

  private boolean rejectWrite() {
    double d = getMemoryUsedPct();
    return d >= this.writeRejectionThreshold;
  }
  
  /**
   * Put item into the cache
   *
   * @param keyPtr key address
   * @param keySize key size
   * @param valPtr value address
   * @param valSize value size
   * @param expire - expiration (0 - no expire)
   */
  public void put(long keyPtr, int keySize, long valPtr, int valSize, long expire)
    throws IOException {    
    int rank = conf.getSLRUInsertionPoint(this.cacheName);
    put(keyPtr, keySize, valPtr, valSize, expire, rank, false);
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
   */
  public void put(
      long keyPtr, int keySize, long valPtr, int valSize, long expire, int rank, boolean force)
      throws IOException {

    if (rejectWrite()) {
      this.totalRejectedWrites.incrementAndGet();
      return;
    }
    this.totalWrites.incrementAndGet();
    // Check rank
    checkRank(rank);

    boolean result = false;

    if (!force) {
      AdmissionController.Type type = AdmissionController.Type.AQ;
      if (admissionController != null) {
        // Adjust rank taking into account item's expiration time
        rank = this.admissionController.adjustRank(rank, expire);
        type = admissionController.admit(keyPtr, keySize);
      }
      if (type == AdmissionController.Type.AQ) {
        // Add to the AQ
        result = admissionQueue.addIfAbsentRemoveIfPresent(keyPtr, keySize);
        if (result) {
          return;
        }
      }
    }
    if (!result) {
      // Add to the cache
      engine.put(keyPtr, keySize, valPtr, valSize, expire, rank);
    }
    // TODO: update stats
    reportUsed(keySize, valSize);
  }

  private void checkRank(int rank) {
    int maxRank = this.engine.getNumberOfRanks();
    if (rank < 0 || rank >= maxRank) {
      throw new IllegalArgumentException(String.format("Items rank %d is illegal"));
    }
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
   */
  public void put(
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
    if (rejectWrite()) {
      this.totalRejectedWrites.incrementAndGet();
      return;
    }
    this.totalWrites.incrementAndGet();
    // Check rank
    checkRank(rank);
    boolean result = false;

    if (!force) {
      AdmissionController.Type type = AdmissionController.Type.AQ;
      if (admissionController != null) {
        // Adjust rank
        rank = this.admissionController.adjustRank(rank, expire);
        type = admissionController.admit(key, keyOffset, keySize);
      }
      if (type == AdmissionController.Type.AQ) {
        // Add to the AQ
        result = admissionQueue.addIfAbsentRemoveIfPresent(key, keyOffset, keySize);
        if (result) {
          return;
        }
      }
    }
    if (!result) {
      // Add to the cache
      engine.put(key, keyOffset, keySize, value, valOffset, valSize, expire, rank);
    }
    // TODO: update stats
    reportUsed(keySize, valSize);
  }

  /**
   * Put item into the cache
   *
   * @param key key
   * @param value value
   * @param expire - expiration (0 - no expire)
   */
  public void put(byte[] key, byte[] value, long expire) throws IOException {
    int rank = conf.getSLRUInsertionPoint(this.cacheName);
    put(key, 0, key.length, value, 0, value.length, expire, rank, false);
  }

  /* Get API*/
  /**
   * Get cached item (if any)
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
    long result = engine.get(keyPtr, keySize, buffer, bufOffset);
    if (result <= buffer.length - bufOffset) {
      access();
      if (result >= 0) {
        hit();
      }
    }
    if (result >= 0 && result <= buffer.length - bufOffset) {
      if (this.admissionController != null) {
        this.admissionController.access(keyPtr, keySize);
      }
    }
    return result;
  }

  /**
   * Get cached item (if any)
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
  public long get(byte[] key, int keyOffset, int keySize, boolean hit, byte[] buffer, int bufOffset) throws IOException {
    long result = engine.get(key, keyOffset, keySize, buffer, bufOffset);
    if (result <= buffer.length - bufOffset) {
      access();
      if (result >= 0) {
        hit();
      }
    }
    if (result >=0 && result <= buffer.length - bufOffset) {
      if (this.admissionController != null) {
        this.admissionController.access(key, keyOffset, keySize);
      }
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
  public long get(byte[] key, int keyOff, int keySize, boolean hit, ByteBuffer buffer) throws IOException {
    int rem = buffer.remaining();
    long result = this.engine.get(key, keyOff, keySize,  buffer);
    if (result <= rem) {
      access();
      if (result >= 0) {
        hit();
      }
    }
    if (result >=0 && result <= rem) {
      if (this.admissionController != null) {
        this.admissionController.access(key, keyOff, keySize);
      }
    }
    return result;
  }

  /**
   * Get cached item (if any)
   *
   * @param key key buffer
   * @param hit if true - its a hit
   * @param buffer byte buffer for item
   * @return size of an item (-1 - not found), if is greater than bufSize - retry with a properly
   *     adjusted buffer
   * @throws IOException 
   */
  public long get(long keyPtr, int keySize, boolean hit, ByteBuffer buffer) throws IOException {
    int rem = buffer.remaining();
    long result = this.engine.get(keyPtr, keySize,  buffer);
    if (result <= rem) {
      access();
      if (result >= 0) {
        hit();
      }
    }
    if (result >=0 && result <= rem) {
      if (this.admissionController != null) {
        this.admissionController.access(keyPtr, keySize);
      }
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
   */
  public boolean delete(long keyPtr, int keySize) {
    return engine.getMemoryIndex().delete(keyPtr, keySize);
  }

  /**
   * Delete cached item
   *
   * @param key key
   * @param keyOffset key offset
   * @param keySize key size
   * @return true - success, false - does not exist
   */
  public boolean delete(byte[] key, int keyOffset, int keySize) {
    return engine.getMemoryIndex().delete(key, keyOffset, keySize);
  }

  /**
   * Delete cached item
   *
   * @param key key
   * @param keyOffset key offset
   * @param keyLength key size
   * @return true - success, false - does not exist
   */
  public boolean delete(byte[] key) {
    return delete(key, 0, key.length);
  }
  
  
  /**
   * Expire cached item
   *
   * @param keyPtr key address
   * @param keySize key size
   * @return true - success, false - does not exist
   */
  public boolean expire(long keyPtr, int keySize) {
    return engine.getMemoryIndex().delete(keyPtr, keySize);
  }

  /**
   * Expire cached item
   *
   * @param key key
   * @param keyOffset key offset
   * @param keySize key size
   * @return true - success, false - does not exist
   */
  public boolean expire(byte[] key, int keyOffset, int keySize) {
    return engine.getMemoryIndex().delete(key, keyOffset, keySize);
  }

  /**
   * Expire cached item
   *
   * @param key key
   * @param keyOffset key offset
   * @param keyLength key size
   * @return true - success, false - does not exist
   */
  public boolean expire(byte[] key) {
    return delete(key, 0, key.length);
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
  }
  
  /**
   * Transfer cached item to a victim cache
   * @param ibPtr index block pointer
   * @param indexPtr item pointer
   * @throws IOException 
   */
  public void transfer(long ibPtr, long indexPtr) throws IOException {
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
    int rank = conf.getSLRUInsertionPoint(this.victimCache.getName());
    int sid = (int) format.getSegmentId(indexPtr);
    long offset = format.getOffset(indexPtr); 
    
    Segment s = this.engine.getSegmentById(sid);
    //TODO : check segment
    try {
      s.readLock();
      long ptr = s.getAddress();
      ptr += offset;
      int keySize = Utils.readUVInt(ptr);
      int kSizeSize = Utils.sizeUVInt(keySize);
      ptr += kSizeSize;
      int valueSize = Utils.readUVInt(ptr);
      int vSizeSize = Utils.sizeUVInt(valueSize);
      ptr += vSizeSize;
      this.victimCache.put(ptr, keySize, ptr + keySize, valueSize, expire, rank, true);
    } finally {
      s.readUnlock();
    }
  }
  
  // Scavenger.Listener
  @Override
  public void evicted(byte[] key, int keyOffset, int keySize, 
      byte[] value, int valueOffset, int valueSize, long expire) {
    if (this.admissionController != null) {
      AdmissionController.Type type = this.admissionController.readmit(key, keyOffset, keySize);
      if (type == AdmissionController.Type.AQ) {
        admissionQueue.addIfAbsentRemoveIfPresent(key, keyOffset, keySize);
      }
    }
  }

  @Override
  public void expired(byte[] key, int off, int keySize) {
    // Do nothing yet
  }
  
  // IOEngine.Listener
  @Override
  public void onEvent(IOEngine e, IOEngineEvent evt) {
    if (evt == IOEngineEvent.DATA_SIZE_CHANGED) {
      double used = getMemoryUsedPct();
      //TODO: performance
      double max = this.conf.getScavengerStartMemoryRatio(this.cacheName);
      double min = this.conf.getScavengerStopMemoryRatio(this.cacheName);
      if (used >= max && (this.scavenger == null || !this.scavenger.isAlive())) {
        this.scavenger = new Scavenger(this);
        this.scavenger.start();
        this.engine.setEvictionEnabled(true);
      } else if (used < min) {
        // Disable eviction
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
    String snapshotDir = this.conf.getSnapshotDir(this.cacheName);
    String file = CacheConfig.CACHE_SNAPSHOT_NAME;
    Path p = Paths.get(snapshotDir, file);
    if (Files.exists(p)) {
      FileInputStream fis = new FileInputStream(p.toFile());
      DataInputStream dis = new DataInputStream(fis);
      this.allocatedMemory.set(dis.readLong());
      this.usedMemory.set(dis.readLong());
      this.totalGets.set(dis.readLong());
      this.totalHits.set(dis.readLong());
      this.totalWrites.set(dis.readLong());
      this.totalRejectedWrites.set(dis.readLong());
      dis.close();
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
    dos.writeLong(allocatedMemory.get());
    dos.writeLong(usedMemory.get());
    dos.writeLong(totalGets.get());
    dos.writeLong(totalHits.get());
    dos.writeLong(totalWrites.get());
    dos.writeLong(totalRejectedWrites.get());
    dos.close();
  }
  
  /**
   * Loads admission queue data
   * @throws IOException
   */
  private void loadAdmissionQueue() throws IOException {
    String snapshotDir = this.conf.getSnapshotDir(this.cacheName);
    String file = CacheConfig.ADMISSION_QUEUE_SNAPSHOT_NAME;
    Path p = Paths.get(snapshotDir, file);
    if (Files.exists(p) && Files.size(p) > 0) {
      FileInputStream fis = new FileInputStream(p.toFile());
      DataInputStream dis = new DataInputStream(fis);
      this.admissionQueue.load(dis);
      dis.close();
    }
  }
  
  /**
   * Saves admission queue data
   * @throws IOException
   */
  private void saveAdmissionQueue() throws IOException {
    String snapshotDir = this.conf.getSnapshotDir(this.cacheName);
    String file = CacheConfig.ADMISSION_QUEUE_SNAPSHOT_NAME;
    Path p = Paths.get(snapshotDir, file);
    FileOutputStream fos = new FileOutputStream(p.toFile());
    DataOutputStream dos = new DataOutputStream(fos);
    this.admissionQueue.save(dos);
    dos.close();
  }
  
  /**
   * Loads admission controller data
   * @throws IOException
   */
  private void loadAdmissionControlller() throws IOException {
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
    String snapshotDir = this.conf.getSnapshotDir(this.cacheName);
    String file = CacheConfig.THROUGHPUT_CONTROLLER_SNAPSHOT_NAME;
    Path p = Paths.get(snapshotDir, file);
    FileOutputStream fos = new FileOutputStream(p.toFile());
    DataOutputStream dos = new DataOutputStream(fos);
    this.throughputController.save(dos);
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
    String file = CacheConfig.THROUGHPUT_CONTROLLER_SNAPSHOT_NAME;
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
    saveAdmissionQueue();
    saveAdmissionController();
    saveThroughputController();
    saveEngine();
    long endTime = System.currentTimeMillis();
    LOG.info("Cache saved in {}ms", endTime - startTime);
  }
  /**
   * Load cache data and meta-data from a file system
   * @throws IOException
   */
  public void load() throws IOException {
    LOG.info("Started loading cache ...");
    long startTime = System.currentTimeMillis();
    loadCache();
    loadAdmissionQueue();
    loadAdmissionControlller();
    loadThroughputControlller();
    loadEngine();
    long endTime = System.currentTimeMillis();
    LOG.info("Cache loaded in {}ms", endTime - startTime);
  }
}
