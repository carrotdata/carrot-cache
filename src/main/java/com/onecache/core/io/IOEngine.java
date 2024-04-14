/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.onecache.core.io;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.onecache.core.Cache;
import com.onecache.core.Scavenger;
import com.onecache.core.compression.CodecFactory;
import com.onecache.core.controllers.RecyclingSelector;
import com.onecache.core.index.IndexFormat;
import com.onecache.core.index.MemoryIndex;
import com.onecache.core.index.MemoryIndex.MutationResult;
import com.onecache.core.util.CacheConfig;
import com.onecache.core.util.MemoryBufferPool;
import com.onecache.core.util.Persistent;
import com.onecache.core.util.UnsafeAccess;
import com.onecache.core.util.Utils;

/**
 * IOEngine base class
 *
 * <p>This class is responsible for storing and retrieving cached data, and keeping statistics
 * information for all data segments. All data is stored in segments Each segment has a size which
 * is configurable Total number of segments is defined by a maximum cache size and segment size.
 * Data can be stored in RAM (off-heap) and on disk (SSD)
 */
public abstract class IOEngine implements Persistent {

  /** Logger */
  private static final Logger LOG = LogManager.getLogger(IOEngine.class);

  protected static final String FILE_NAME = "data_";

  public static int NOT_FOUND = -1;

  public static int READ_ERROR = -2;
  
  public static enum IOEngineEvent {
    DATA_SIZE_CHANGED; // size of a data changed
  }

  public static interface Listener {
    public void onEvent(IOEngine e, IOEngineEvent evt);
  }

  /* Parent cache name */
  protected final String cacheName;

  /* Segment size (bytes)*/
  protected final long segmentSize;

  /* Number of segments */
  protected final int numSegments;

  /* Cache configuration */
  protected final CacheConfig config;

  /* Default item rank */
  protected final int defaultRank;

  /* IOEngine listener */
  protected Listener aListener;

  /* Maximum allowed storage size (in bytes) */
  protected long maxStorageSize;

  /* Total allocated storage size (in bytes) */
  protected AtomicLong storageAllocated = new AtomicLong();

  /* Total storage used size in bytes (uncompressed)*/
  protected AtomicLong rawDataSize = new AtomicLong();

  /* Total storage used size in bytes (compressed)*/
  protected AtomicLong storageUsed = new AtomicLong();
  
  /* Upsert operation - update existing one*/
  protected AtomicLong totalUpdates = new AtomicLong();
  
  /* New inserts */
  protected AtomicLong totalInserts = new AtomicLong();
  
  /* Total number of delete operations */
  protected AtomicLong totalDeletes = new AtomicLong();
  
  /* Total read operations (gets) */
  protected AtomicLong totalReads  = new AtomicLong();
 
  /* Total failed reads */
  protected AtomicLong totalFailedReads = new AtomicLong();
  
  /* Total duration in ns of all read operations*/
  protected AtomicLong totalIOReadDuration = new AtomicLong();
  
  /*
   * RAM buffers accumulates incoming PUT's before submitting them to an IOEngine
   */

  protected Segment[] ramBuffers;

  /* Keeps tracks of all segments*/
  protected Segment[] dataSegments;

  /* Memory index */
  protected MemoryIndex index;

  /* Memory buffer pool*/
  protected MemoryBufferPool memoryBufferPool;
  
  /* Cached data directory name */
  protected String dataDir;

  /* Segment data appender */
  protected DataWriter dataWriter;

  /* IOEngine data reader - reads only memory based segments */
  protected DataReader memoryDataReader;

  /* Recycling selector */
  protected RecyclingSelector recyclingSelector;

  /* Data embedding supported */
  boolean dataEmbedded;

  /* Maximum size for data embedding */
  int maxEmbeddedSize;

  List<Scavenger.Listener> scavengerListeners = new LinkedList<>();
  
  /**
   * Write batches for writers which supports batching of write operations
   * (optimized compressed batch writer)
   */
  WriteBatches writeBatches;
  
  /**
   * Initialize engine for a given cache
   *
   * @param cache cache 
   * @return new engine
   */
  public static IOEngine getEngineForCache(Cache cache) {
    // TODO: Check NULL on return
    String cacheName = cache.getName();
    CacheConfig config = cache.getCacheConfig();
    String[] caches = config.getCacheNames();
    String[] types = config.getCacheTypes();
    if (caches == null || types == null || caches.length != types.length) {
      throw new RuntimeException("Cache misconfiguration");
    }
    for (int i = 0; i < caches.length; i++) {
      if (caches[i].equals(cacheName)) {
        return engineFor(types[i], cache);
      }
    }
    return null;
  }

  /**
   * Get engine for cache
   *
   * @param type cache type
   * @param cache cache itself
   * @return engine
   */
  protected static IOEngine engineFor(String type, Cache cache) {
    if (type.equals("offheap")) {
      return new OffheapIOEngine(cache.getName());
    } else if (type.equals("file")) {
      return new FileIOEngine(cache.getName());
    }
    return null;
  }

  /**
   * Constructor
   *
   * @param cacheName cache name
   */
  public IOEngine(String cacheName) {
    this.cacheName = cacheName;
    this.config = CacheConfig.getInstance();
    this.segmentSize = this.config.getCacheSegmentSize(this.cacheName);
    this.maxStorageSize = this.config.getCacheMaximumSize(this.cacheName);
    // Currently, maximum number of data segments is 64K
    this.numSegments = Math.min((int) (this.maxStorageSize / this.segmentSize) + 1, 1 << 16);
    int num = this.config.getNumberOfPopularityRanks(this.cacheName);
    this.ramBuffers = new Segment[num];
    this.dataSegments = new Segment[this.numSegments];
    this.index = new MemoryIndex(this, MemoryIndex.Type.MQ);
    this.dataDir = this.config.getDataDir(this.cacheName);
    this.defaultRank = this.index.getEvictionPolicy().getDefaultRankForInsert();
    this.dataEmbedded = this.config.isIndexDataEmbeddedSupported(this.cacheName);
    this.maxEmbeddedSize = this.config.getIndexDataEmbeddedSize(this.cacheName);
    int maxPoolSize = this.config.getCacheMemoryBufferPoolMaximumSize(cacheName);
    this.memoryBufferPool = new MemoryBufferPool((int) this.segmentSize, maxPoolSize);
    try {
      this.dataWriter = this.config.getDataWriter(this.cacheName);
      this.memoryDataReader = this.config.getMemoryDataReader(this.cacheName);
      this.recyclingSelector = this.config.getRecyclingSelector(cacheName);
      if (this.dataWriter.isWriteBatchSupported()) {
        this.writeBatches = new WriteBatches(this.dataWriter);
      }

    } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
      LOG.fatal(e);
      throw new RuntimeException(e);
    }
  }

  /**
   * Constructor
   *
   * @param conf cache configuration
   */
  public IOEngine(CacheConfig conf) {
    this("default");
  }

  public void addScavengerListener(Scavenger.Listener l) {
    this.scavengerListeners.add(l);
  }

  /**
   * Get allocated storage size
   *
   * @return size
   */
  public long getStorageAllocated() {
    return this.storageAllocated.get();
  }

  /**
   * Get storage used (uncompressed size)
   *
   * @return size
   */
  public long getRawDataSize() {
    return this.rawDataSize.get();
  }

  /**
   * This method should be called when compression is enabled
   * @return actual storage usage
   */
  public final long getStorageUsed() {
//    long used = 0;
//    for(int i = 0; i < dataSegments.length; i++) {
//      Segment s = dataSegments[i];
//      if (s == null || !s.isValid()) {
//        continue;
//      }
//      used += s.getSegmentDataSize();
//    }
//    /*DEBUG*/ if (ThreadLocalRandom.current().nextDouble() < 0.001) {
//      System.out.println("used=" + this.storageUsed.get() + " calc=" + used);
//    }
//    return used;
    return this.storageUsed.get();
  }
  
  /**
   * Get storage allocation as a ratio of a maximum storage size
   *
   * @return ratio (0. - 1.)
   */
  public double getStorageAllocatedRatio() {
    return (double) this.storageAllocated.get() / this.maxStorageSize;
  }

  /**
   * Report allocation
   *
   * @param value allocation value
   * @return new storage allocation value
   */
  public long reportAllocation(long value) {
    long v = this.storageAllocated.addAndGet(value);
    if (this.aListener != null) {
      // This must the Cache
      aListener.onEvent(this, IOEngineEvent.DATA_SIZE_CHANGED);
    }
    return v;
  }

  /**
   * Report usage
   *
   * @param value usage value
   * @return new storage usage value
   */
  public long reportRawDataSize(long value) {
    return this.rawDataSize.addAndGet(value);
  }

  /**
   * Report actual usage (compressed or raw if compression not enabled)
   *
   * @param value usage value
   * @return new storage usage value
   */
  public long reportStorageUsed(long value) {
    return this.storageUsed.addAndGet(value);
  }
  
  /**
   * Get cache name
   *
   * @return cache name
   */
  public String getCacheName() {
    return this.cacheName;
  }

  /**
   * Enables - disables eviction
   *
   * @param b
   */
  public void setEvictionEnabled(boolean b) {
    this.index.setEvictionEnabled(b);
  }

  /**
   * Is eviction enabled
   *
   * @return true or false
   */
  public boolean isEvictionEnabled() {
    return this.index.isEvictionEnabled();
  }
  /**
   * Sets engine listener
   *
   * @param al listener
   */
  public void setListener(Listener al) {
    this.aListener = al;
  }

  /**
   * Get engine listener
   *
   * @return engine listener
   */
  public Listener getListener() {
    return this.aListener;
  }

  /**
   * Get memory index
   *
   * @return memory index
   */
  public MemoryIndex getMemoryIndex() {
    return this.index;
  }

  /**
   * Get segment default size
   *
   * @return segment default size
   */
  public long getSegmentSize() {
    return this.segmentSize;
  }

  /**
   * Get number of segments
   *
   * @return number of segments
   */
  public int getNumberOfSegments() {
    return this.numSegments;
  }

  /**
   * Get number of ranks
   *
   * @return number of ranks
   */
  public int getNumberOfRanks() {
    return ramBuffers.length;
  }

  /**
   * Get total updates
   * @return total number of update operations
   */
  public long getTotalUpdates() {
    return this.totalUpdates.get();
  }
  
  /**
   * Get total inserts
   * @return total inserts
   */
  public long getTotalInserts() {
    return this.totalInserts.get();
  }
  
  /**
   * Get total deletes
   * @return total deletes
   */
  public long getTotalDeletes() {
    return this.totalDeletes.get();
  }
  
  /**
   * Get total IO read duration
   * @return duration of all read operations so far in ns
   */
  public long getTotalIOReadDuration() {
    return this.totalIOReadDuration.get();
  }
  
  /**
   * Get total number of read operations
   * @return total number of reads
   */
  public long getTotalReads() {
    return this.totalReads.get();
  }
  
  /**
   * Get total number of failed read operations
   * @return total number of failed reads
   */
  public long getTotalFailedReads() {
    return this.totalFailedReads.get();
  }
  
  /**
   * Get segment by segment id
   *
   * @param sid segment id
   * @return segment
   */
  public Segment getSegmentById(int sid) {
    // TODO: checkId(sid);
    return this.dataSegments[sid];
  }

  /**
   * Get key-value into a given byte buffer
   *
   * @param keyPtr key address
   * @param keySize size of a key
   * @param buffer buffer
   * @param bufOffset buffer offset
   * @return length of an item or -1
   * @throws IOException
   */
  public long get(long keyPtr, int keySize, boolean hit, byte[] buffer, int bufOffset)
      throws IOException {
    IndexFormat format = this.index.getIndexFormat();
    // TODO: embedded entry case
    int entrySize = format.indexEntrySize();
    long buf = UnsafeAccess.mallocZeroed(entrySize);
    try {

      long offset = 0;
      while (offset < -1) {
        // We can stuck in the endless loop if memory index contains
        // 'orphan' index for some write buffer
        // FIXME ?
        long result = index.find(keyPtr, keySize, hit, buf, entrySize);
        if (result < 0) {
          return NOT_FOUND;
        } else if (result > entrySize) {
          UnsafeAccess.free(buf);
          entrySize = (int) result;
          buf = UnsafeAccess.mallocZeroed(entrySize);
          result = index.find(keyPtr, keySize, hit, buf, entrySize);
          if (result < 0) {
            return NOT_FOUND;
          }
        }
        // Cached item offset in a data segment
        offset = format.getOffset(buf);
        // Check if it is i a write buffer
        if (offset < -1) {
          // Check write buffers
          if (this.writeBatches == null) {
            throw new RuntimeException("Corrupted index, returns negative offset, but write batches are disbled: off=" + offset);
          }
          WriteBatch wb = this.writeBatches.getWriteBatch((int) offset);
          if (wb == null) {
            throw new RuntimeException("Corrupted index, returns negative offset, but write batch was not found: off=" + offset);
          }
          int size = wb.get(keyPtr, keySize, buffer, bufOffset);
          if (size >= 0) {
            return size;
          }
        }
      }
      // This call returns TOTAL size: key + value + kSize + vSize
      int keyValueSize = format.getKeyValueSize(buf);
      // TODO: actually, not correct IT CAN RETURN -1
      if (keyValueSize > buffer.length - bufOffset) {
        return keyValueSize;
      }
      boolean dataEmbedded = this.dataEmbedded && (keyValueSize < this.maxEmbeddedSize);
      if (dataEmbedded) {
        // Return embedded data
        int off = format.getEmbeddedOffset();
        int kSize = Utils.readUVInt(buf + off);
        if (kSize != keySize) {
          return NOT_FOUND;
        }
        int kSizeSize = Utils.sizeUVInt(kSize);
        off += kSizeSize;
        int vSize = Utils.readUVInt(buf + off);
        int vSizeSize = Utils.sizeUVInt(vSize);
        off += vSizeSize;
        if (Utils.compareTo(keyPtr, keySize, buf + off, kSize) != 0) {
          return NOT_FOUND;
        }
        off -= kSizeSize + vSizeSize;
        // Copy data to buffer
        UnsafeAccess.copy(buf + off, buffer, bufOffset, keyValueSize);
        return keyValueSize;
      } else {
        // Segment id
        int sid = (int) format.getSegmentId(buf);
        // Read the data
        Segment s = this.dataSegments[sid];
        if (s == null || !s.isValid()) {
          return NOT_FOUND;
        }
        // Make up to 3 attempts
        int maxAttempts = 3;
        int attempt = 0;
        int res = NOT_FOUND;
        while (attempt++ < maxAttempts) {
          try {
            s.readLock();
            int id = this.index.getSegmentId(keyPtr, keySize);
            if (id < 0) {
              return NOT_FOUND;
            }
            if (id != sid) {
              s.readUnlock();
              return get(keyPtr, keySize, hit, buffer, bufOffset);
            }
            // Read the data
            res = get(sid, offset, keyValueSize, keyPtr, keySize, buffer, bufOffset);
            if (res >= 0) {
              access(s, res, hit);
            } else if (res == READ_ERROR) {
              continue;
            }
            return res;
          } finally {
            s.readUnlock();
          }
        }
        if (res == READ_ERROR) {
          this.totalFailedReads.incrementAndGet();
        }
      }
      return NOT_FOUND;
    } finally {
      UnsafeAccess.free(buf);
    }
  }
  
  /**
   * Get value range into a given byte buffer
   *
   * @param keyPtr key address
   * @param keySize size of a key
   * @param rangeStart range start
   * @param rangeSize range size
   * @param buffer buffer
   * @param bufOffset buffer offset
   * @return length of an item or -1
   * @throws IOException
   */
  public long getRange(long keyPtr, int keySize, int rangeStart, int rangeSize, boolean hit, 
       byte[] buffer, int bufOffset)
      throws IOException {
    IndexFormat format = this.index.getIndexFormat();
    // TODO: embedded entry case
    int entrySize = format.indexEntrySize();
    long buf = UnsafeAccess.mallocZeroed(entrySize);
    try {

      long result = index.find(keyPtr, keySize, hit, buf, entrySize);
      
      if (result < 0) {
        return NOT_FOUND;
      } else if (result > entrySize) {
        UnsafeAccess.free(buf);
        entrySize = (int) result;
        buf = UnsafeAccess.mallocZeroed(entrySize);
        result = index.find(keyPtr, keySize, hit, buf, entrySize);
        if (result < 0) {
          return NOT_FOUND;
        }
      }
      // This call returns TOTAL size: key + value + kSize + vSize
      int keyValueSize = format.getKeyValueSize(buf);

      boolean dataEmbedded = this.dataEmbedded && (keyValueSize < this.maxEmbeddedSize);
      if (dataEmbedded) {
        // Return embedded data
        int off = format.getEmbeddedOffset();
        int kSize = Utils.readUVInt(buf + off);
        if (kSize != keySize) {
          return NOT_FOUND;
        }
        int kSizeSize = Utils.sizeUVInt(kSize);
        off += kSizeSize;
        int vSize = Utils.readUVInt(buf + off);
        int vSizeSize = Utils.sizeUVInt(vSize);
        off += vSizeSize;
        if (Utils.compareTo(keyPtr, keySize, buf + off, kSize) != 0) {
          return NOT_FOUND;
        }
        off -= kSizeSize + vSizeSize;
        // Copy data to buffer
        UnsafeAccess.copy(buf + off, buffer, bufOffset, keyValueSize);
        return keyValueSize;
      } else {
        // Cached item offset in a data segment
        long offset = format.getOffset(buf);
        // Segment id
        int sid = (int) format.getSegmentId(buf);
        // Read the data
        Segment s = this.dataSegments[sid];
        if (s == null || !s.isValid()) {
          return NOT_FOUND;
        }
        
        try {
          s.readLock();
          int id = this.index.getSegmentId(keyPtr, keySize);
          if (id < 0) {
            return NOT_FOUND;
          }
          if (id != sid) {
            s.readUnlock();
            return getRange(keyPtr, keySize, rangeStart, rangeSize, hit, buffer, bufOffset);
          }
          // Read the data
          int res = getRange(sid, offset, keyValueSize, keyPtr, keySize, 
            rangeStart, rangeSize, buffer, bufOffset);
          access(s, res, hit);
          return res;
        } finally {
          s.readUnlock();
        }
      }
    } finally {
      UnsafeAccess.free(buf);
    }
  }
  /**
   * Get item into a given byte buffer
   *
   * @param key key buffer
   * @param keyOffset offset
   * @param keySize size of a key
   * @param buffer buffer
   * @param bufOffset buffer offset
   * @return length of an item or -1
   * @throws IOException
   */
  public long get(byte[] key, int keyOffset, int keySize, boolean hit, byte[] buffer, int bufOffset)
      throws IOException {

    IndexFormat format = this.index.getIndexFormat();
    // TODO: embedded entry case
    int entrySize = format.indexEntrySize();
    long buf = UnsafeAccess.mallocZeroed(entrySize);
    int bufferAvail = buffer.length - bufOffset;
    try {
      long offset = 0;
      while (offset < -1) {
        // We can stuck in the endless loop if memory index contains
        // 'orphan' index for some write buffer
        // FIXME ?
        long result = index.find(key, keyOffset, keySize, hit, buf, entrySize);
        if (result < 0) {
          return NOT_FOUND;
        } else if (result > entrySize) {
          UnsafeAccess.free(buf);
          entrySize = (int) result;
          buf = UnsafeAccess.mallocZeroed(entrySize);
          result = index.find(key, keyOffset, keySize, hit, buf, entrySize);
          if (result < 0) {
            return NOT_FOUND;
          }
        }
        // Cached item offset in a data segment
        offset = format.getOffset(buf);
        // Check if it is i a write buffer
        if (offset < -1) {
          // Check write buffers
          if (this.writeBatches == null) {
            throw new RuntimeException("Corrupted index, returns negative offset, but write batches are disbled: off=" + offset);
          }
          WriteBatch wb = this.writeBatches.getWriteBatch((int) offset);
          if (wb == null) {
            throw new RuntimeException("Corrupted index, returns negative offset, but write batch was not found: off=" + offset);
          }
          int size = wb.get(key, keyOffset, keySize, buffer, bufOffset);
          if (size >= 0) {
            return size;
          }
        }
      }
      // This call returns TOTAL size: key + value + kSize + vSize
      int keyValueSize = format.getKeyValueSize(buf);
      // can be negative
      if (keyValueSize > bufferAvail) {
        return keyValueSize;
      }
      boolean dataEmbedded = this.dataEmbedded && (keyValueSize < this.maxEmbeddedSize);
      if (dataEmbedded) {
        // For index formats which supports embedding
        // Return embedded data
        int off = format.getEmbeddedOffset();
        int kSize = Utils.readUVInt(buf + off);
        if (kSize != keySize) {
          return NOT_FOUND;
        }
        int kSizeSize = Utils.sizeUVInt(kSize);
        off += kSizeSize;
        int vSize = Utils.readUVInt(buf + off);
        int vSizeSize = Utils.sizeUVInt(vSize);
        off += vSizeSize;
        if (Utils.compareTo(key, keyOffset, keySize, buf + off, kSize) != 0) {
          return NOT_FOUND;
        }
        off -= kSizeSize + vSizeSize;
        // Copy data to buffer
        UnsafeAccess.copy(buf + off, buffer, bufOffset, keyValueSize);
        return keyValueSize;
      } else {
        // segment id
        int sid = (int) format.getSegmentId(buf);
        Segment s = this.dataSegments[sid];
        if (s == null || !s.isValid()) {
          return NOT_FOUND;
        }
        
        // Make up to 3 attempts
        int maxAttempts = 3;
        int attempt = 0;
        int res = NOT_FOUND;
        while (attempt++ < maxAttempts) {
          try {
            s.readLock();
            // Check if scavenger removed this object or moved it to another segment
            int id = this.index.getSegmentId(key, keyOffset, keySize);
            if (id < 0) {
              return NOT_FOUND;
            }
            if (id != sid) {
              s.readUnlock();
              return get(key, keyOffset, keySize, hit, buffer, bufOffset);
            }
            // Read the data
            res = get(sid, offset, keyValueSize, key, keyOffset, keySize, buffer, bufOffset);
            if (res >= 0) {
              access(s, res, hit);
            } else  if (res == IOEngine.READ_ERROR) {
              // next attempt
              continue;
            }
            return res;
          } finally {
            s.readUnlock();
          }
        }
        if (res == READ_ERROR) {
          this.totalFailedReads.incrementAndGet();
        }
      }
      return NOT_FOUND;
    } finally {
      UnsafeAccess.free(buf);
    }
  }

  /**
   * Get value range into a given byte buffer
   *
   * @param key key buffer
   * @param keyOffset offset
   * @param keySize size of a key
   * @param rangeStart range start
   * @param rangeSize range size
   * @param buffer buffer
   * @param bufOffset buffer offset
   * @return length of a range or -1
   * @throws IOException
   */
  public long getRange(byte[] key, int keyOffset, int keySize, int rangeStart, int rangeSize, 
      boolean hit, byte[] buffer, int bufOffset)
      throws IOException {

    IndexFormat format = this.index.getIndexFormat();
    // TODO: embedded entry case
    int entrySize = format.indexEntrySize();
    long buf = UnsafeAccess.mallocZeroed(entrySize);
    try {
      long result = index.find(key, keyOffset, keySize, hit, buf, entrySize);
      if (result < 0) {
        return NOT_FOUND;
      } else if (result > entrySize) {
        UnsafeAccess.free(buf);
        entrySize = (int) result;
        buf = UnsafeAccess.mallocZeroed(entrySize);
        result = index.find(key, keyOffset, keySize, hit, buf, entrySize);
        if (result < 0) {
          return NOT_FOUND;
        }
      }
      // This call returns TOTAL size: key + value + kSize + vSize
      int keyValueSize = format.getKeyValueSize(buf);

      //TODO: getRange does not make sense for embedded data
      boolean dataEmbedded = this.dataEmbedded && (keyValueSize < this.maxEmbeddedSize);
      if (dataEmbedded) {
        // For index formats which supports embedding
        // Return embedded data
        int off = format.getEmbeddedOffset();
        int kSize = Utils.readUVInt(buf + off);
        if (kSize != keySize) {
          return NOT_FOUND;
        }
        int kSizeSize = Utils.sizeUVInt(kSize);
        off += kSizeSize;
        int vSize = Utils.readUVInt(buf + off);
        int vSizeSize = Utils.sizeUVInt(vSize);
        off += vSizeSize;
        if (Utils.compareTo(key, keyOffset, keySize, buf + off, kSize) != 0) {
          return NOT_FOUND;
        }
        off -= kSizeSize + vSizeSize;
        // Copy data to buffer
        UnsafeAccess.copy(buf + off, buffer, bufOffset, keyValueSize);
        return keyValueSize;
      } else {
        // Cached item offset in a data segment
        long offset = format.getOffset(buf);
        // segment id
        int sid = (int) format.getSegmentId(buf);
        Segment s = this.dataSegments[sid];
        if (s == null || !s.isValid()) {
          return NOT_FOUND;
        }
        
        try {
          s.readLock();
          // Check if scavenger removed this object or moved it to another segment
          int id = this.index.getSegmentId(key, keyOffset, keySize);
          if (id < 0) {
            return NOT_FOUND;
          }
          if (id != sid) {
            s.readUnlock();
            return getRange(key, keyOffset, keySize, rangeStart, rangeSize, hit, buffer, bufOffset);
          }
          // Read the data
          int res = getRange(sid, offset, keyValueSize, key, keyOffset, keySize, rangeStart, rangeSize,  buffer, bufOffset);
          access(s, res, hit);
          return res;
        } finally {
          s.readUnlock();
        }
      }
    } finally {
      UnsafeAccess.free(buf);
    }
  }

  private void access(Segment s, int result, boolean hit) {
    if (result > 0 && hit) {
      if (s != null) {
        s.access();
      }
    }
  }
  
  /**
   * Get item into a given byte buffer
   *
   * @param keyPtr key address
   * @param keySize size of a key
   * @param hit
   * @param buffer byte buffer
   * @return length of an item or -1
   * @throws IOException
   */
  public long get(long keyPtr, int keySize, boolean hit, ByteBuffer buffer) throws IOException {
    IndexFormat format = this.index.getIndexFormat();
    int entrySize = format.indexEntrySize();
    long buf = UnsafeAccess.mallocZeroed(entrySize);
    //int slot = 0;
    try {
      long offset = 0;
      while (offset < -1) {
        // We can stuck in the endless loop if memory index contains
        // 'orphan' index for some write buffer
        // FIXME ?
        long result = index.find(keyPtr, keySize, hit, buf, entrySize);
        if (result < 0) {
          return NOT_FOUND;
        } else if (result > entrySize) {
          UnsafeAccess.free(buf);
          entrySize = (int) result;
          buf = UnsafeAccess.mallocZeroed(entrySize);
          result = index.find(keyPtr, keySize, hit, buf, entrySize);
          if (result < 0) {
            return NOT_FOUND;
          }
        }
        // Cached item offset in a data segment
        offset = format.getOffset(buf);
        // Check if it is i a write buffer
        if (offset < -1) {
          // Check write buffers
          if (this.writeBatches == null) {
            throw new RuntimeException("Corrupted index, returns negative offset, but write batches are disbled: off=" + offset);
          }
          WriteBatch wb = this.writeBatches.getWriteBatch((int) offset);
          if (wb == null) {
            throw new RuntimeException("Corrupted index, returns negative offset, but write batch was not found: off=" + offset);
          }
          int size = wb.get(keyPtr, keySize, buffer);
          if (size >= 0) {
            return size;
          }
        }
      }
      // This call returns TOTAL size: key + value + kSize + vSize
      int keyValueSize = format.getKeyValueSize(buf);
      // TODO: actually, not correct
      if (keyValueSize > buffer.remaining()) {
        return keyValueSize;
      }

      boolean dataEmbedded = this.dataEmbedded && (keyValueSize < this.maxEmbeddedSize);
      if (dataEmbedded) {
        // Return embedded data
        int off = format.getEmbeddedOffset();
        int kSize = Utils.readUVInt(buf + off);
        if (kSize != keySize) {
          return NOT_FOUND;
        }
        int kSizeSize = Utils.sizeUVInt(kSize);
        off += kSizeSize;
        int vSize = Utils.readUVInt(buf + off);
        int vSizeSize = Utils.sizeUVInt(vSize);
        off += vSizeSize;
        if (Utils.compareTo(keyPtr, keySize, buf + off, kSize) != 0) {
          return NOT_FOUND;
        }
        off -= kSizeSize + vSizeSize;
        // Copy data to buffer
        UnsafeAccess.copy(buf + off, buffer, keyValueSize);
        return keyValueSize;
      } else {
        // Segment id
        int sid = (int) format.getSegmentId(buf);
        // Finally, read the cached item
        Segment s = this.dataSegments[sid];
        if (s == null || !s.isValid()) {
          return NOT_FOUND;
        }
        // Make up to 3 attempts
        int maxAttempts = 3;
        int attempt = 0;
        int res = NOT_FOUND;
        while (attempt++ < maxAttempts) {
          try {
            s.readLock();
            // Check if scavenger removed this object or moved it to another segment
            int id = this.index.getSegmentId(keyPtr, keySize);
            if (id < 0) {
              return NOT_FOUND;
            }
            if (id != sid) {
              s.readUnlock();
              return get(keyPtr, keySize, hit, buffer);
            }
            // Read the data
            res = get(sid, offset, keyValueSize, keyPtr, keySize, buffer);
            if (res >= 0) {
              access(s, res, hit);
            } else if (res == READ_ERROR) {
              this.totalFailedReads.incrementAndGet();
            }
            return res;
          } finally {
            s.readUnlock();
          }
        }
        if (res == READ_ERROR) {
          this.totalFailedReads.incrementAndGet();
        }
      }
      return NOT_FOUND;
    } finally {
      UnsafeAccess.free(buf);
     // this.index.unlock(slot);
    }
  }

  /**
   * Get value range into a given byte buffer
   *
   * @param keyPtr key address
   * @param keySize size of a key
   * @param rangeStart range start
   * @param rangeSize range size
   * @param hit
   * @param buffer byte buffer
   * @return length of an item or -1
   * @throws IOException
   */
  public long getRange(long keyPtr, int keySize, int rangeStart, int rangeSize, boolean hit, 
       ByteBuffer buffer) throws IOException {
    IndexFormat format = this.index.getIndexFormat();
    int entrySize = format.indexEntrySize();
    long buf = UnsafeAccess.mallocZeroed(entrySize);
    try {
      // TODO: double locking?
      // Index locking  that segment will not be recycled
      //
      long result = index.find(keyPtr, keySize, true, buf, entrySize);
 
      if (result < 0) {
        return NOT_FOUND;
      } else if (result > entrySize) {
        UnsafeAccess.free(buf);
        entrySize = (int) result;
        buf = UnsafeAccess.mallocZeroed(entrySize);
        result = index.find(keyPtr, keySize, true, buf, entrySize);
        if (result < 0) {
          return NOT_FOUND;
        }
      }
      // This call returns TOTAL size: key + value + kSize + vSize
      int keyValueSize = format.getKeyValueSize(buf);

      boolean dataEmbedded = this.dataEmbedded && (keyValueSize < this.maxEmbeddedSize);
      if (dataEmbedded) {
        // Return embedded data
        int off = format.getEmbeddedOffset();
        int kSize = Utils.readUVInt(buf + off);
        if (kSize != keySize) {
          return NOT_FOUND;
        }
        int kSizeSize = Utils.sizeUVInt(kSize);
        off += kSizeSize;
        int vSize = Utils.readUVInt(buf + off);
        int vSizeSize = Utils.sizeUVInt(vSize);
        off += vSizeSize;
        if (Utils.compareTo(keyPtr, keySize, buf + off, kSize) != 0) {
          return NOT_FOUND;
        }
        off -= kSizeSize + vSizeSize;
        // Copy data to buffer
        UnsafeAccess.copy(buf + off, buffer, keyValueSize);
        return keyValueSize;
      } else {
        // Cached item offset in a data segment
        long offset = format.getOffset(buf);
        // Segment id
        int sid = (int) format.getSegmentId(buf);
        // Finally, read the cached item
        Segment s = this.dataSegments[sid];
        if (s == null || !s.isValid()) {
          return NOT_FOUND;
        }
        
        try {
          s.readLock();
          // Check if scavenger removed this object or moved it to another segment
          int id = this.index.getSegmentId(keyPtr, keySize);
          if (id < 0) {
            return NOT_FOUND;
          }
          if (id != sid) {
            s.readUnlock();
            return getRange(keyPtr, keySize, rangeStart, rangeSize, hit, buffer);
          }
          // Read the data
          int res = getRange(sid, offset, keyValueSize, keyPtr, keySize, rangeStart, rangeSize, buffer);
          access(s, res, hit);
          return res;
        } finally {
          s.readUnlock();
        }
      }
    } finally {
      UnsafeAccess.free(buf);
    }
  }

  
  /**
   * Get item into a given byte buffer
   *
   * @param key key buffer
   * @param keyOffset offset
   * @param keySize size of a key
   * @param hit record hit if true
   * @param buffer buffer
   * @return length of an item or -1 (not found)
   * @throws IOException
   */
  public long get(byte[] key, int keyOffset, int keySize, boolean hit, ByteBuffer buffer)
      throws IOException {

    IndexFormat format = this.index.getIndexFormat();
    int entrySize = format.indexEntrySize();
    long buf = UnsafeAccess.mallocZeroed(entrySize);
    try {
      long offset = 0;
      while (offset < -1) {
        // We can stuck in the endless loop if memory index contains
        // 'orphan' index for some write buffer
        // FIXME ?
        long result = index.find(key, keyOffset, keySize, hit, buf, entrySize);
        if (result < 0) {
          return NOT_FOUND;
        } else if (result > entrySize) {
          UnsafeAccess.free(buf);
          entrySize = (int) result;
          buf = UnsafeAccess.mallocZeroed(entrySize);
          result = index.find(key, keyOffset, keySize, hit, buf, entrySize);
          if (result < 0) {
            return NOT_FOUND;
          }
        }
        // Cached item offset in a data segment
        offset = format.getOffset(buf);
        // Check if it is i a write buffer
        if (offset < -1) {
          // Check write buffers
          if (this.writeBatches == null) {
            throw new RuntimeException("Corrupted index, returns negative offset, but write batches are disbled: off=" + offset);
          }
          WriteBatch wb = this.writeBatches.getWriteBatch((int) offset);
          if (wb == null) {
            throw new RuntimeException("Corrupted index, returns negative offset, but write batch was not found: off=" + offset);
          }
          int size = wb.get(key, keyOffset, keySize, buffer);
          if (size >= 0) {
            return size;
          }
        }
      }
      // This call returns TOTAL size: key + value + kSize + vSize
      int keyValueSize = format.getKeyValueSize(buf);
      // TODO: actually, not correct
      if (keyValueSize > buffer.remaining()) {
        return keyValueSize;
      }
      boolean dataEmbedded = this.dataEmbedded && (keyValueSize < this.maxEmbeddedSize);
      if (dataEmbedded) {
        // Return embedded data
        int off = format.getEmbeddedOffset();
        int kSize = Utils.readUVInt(buf + off);
        if (kSize != keySize) {
          return NOT_FOUND;
        }
        int kSizeSize = Utils.sizeUVInt(kSize);
        off += kSizeSize;
        int vSize = Utils.readUVInt(buf + off);
        int vSizeSize = Utils.sizeUVInt(vSize);
        off += vSizeSize;
        if (Utils.compareTo(key, keyOffset, keySize, buf + off, kSize) != 0) {
          return NOT_FOUND;
        }
        off -= kSizeSize + vSizeSize;
        // Copy data to buffer
        UnsafeAccess.copy(buf + off, buffer, keyValueSize);
        return keyValueSize;
      } else {
        // Cached item offset in a data segment
        //long offset = format.getOffset(buf);
        // segment id
        int sid = (int) format.getSegmentId(buf);
        // Read the data
        Segment s = this.dataSegments[sid];
        if (s == null || !s.isValid()) {
          return NOT_FOUND;
        }
        // Make up to 3 attempts
        int maxAttempts = 3;
        int attempt = 0;
        int res = NOT_FOUND;
        while (attempt++ < maxAttempts) {
          try {
            s.readLock();
            // Check if scavenger removed this object or moved it to another segment
            int id = this.index.getSegmentId(key, keyOffset, keySize);
            if (id < 0) {
              return NOT_FOUND;
            }
            if (id != sid) {
              s.readUnlock();
              return get(key, keyOffset, keySize, hit, buffer);
            }
            // Read the data
            res = get(sid, offset, keyValueSize, key, keyOffset, keySize, buffer);
            if (res >=0) {
              access(s, res, hit);
            } else if (res == READ_ERROR) {
              continue;
            }
            return res;
          } finally {
            s.readUnlock();
          }
        }
        if (res == READ_ERROR) {
          this.totalFailedReads.incrementAndGet();
        }
      }
      return NOT_FOUND;
    } finally {
      UnsafeAccess.free(buf);
    }
  }

  /**
   * Get item into a given byte buffer
   *
   * @param key key buffer
   * @param keyOffset offset
   * @param keySize size of a key
   * @param rangeStart range start
   * @param rangeSize range size
   * @param hit record hit if true
   * @param buffer buffer
   * @return length of an item or -1
   * @throws IOException
   */
  public long getRange(byte[] key, int keyOffset, int keySize, 
      int rangeStart, int rangeSize, boolean hit, ByteBuffer buffer)
      throws IOException {

    IndexFormat format = this.index.getIndexFormat();
    int entrySize = format.indexEntrySize();
    long buf = UnsafeAccess.mallocZeroed(entrySize);
    try {
      long result = index.find(key, keyOffset, keySize, hit, buf, entrySize);
      if (result < 0) {
        return NOT_FOUND;
      } else if (result > entrySize) {
        UnsafeAccess.free(buf);
        entrySize = (int) result;
        buf = UnsafeAccess.mallocZeroed(entrySize);
        result = index.find(key, keyOffset, keySize, hit, buf, entrySize);
        if (result < 0) {
          return NOT_FOUND;
        }
      }
      // This call returns TOTAL size: key + value + kSize + vSize
      int keyValueSize = format.getKeyValueSize(buf);

      boolean dataEmbedded = this.dataEmbedded && (keyValueSize < this.maxEmbeddedSize);
      if (dataEmbedded) {
        // Return embedded data
        int off = format.getEmbeddedOffset();
        int kSize = Utils.readUVInt(buf + off);
        if (kSize != keySize) {
          return NOT_FOUND;
        }
        int kSizeSize = Utils.sizeUVInt(kSize);
        off += kSizeSize;
        int vSize = Utils.readUVInt(buf + off);
        int vSizeSize = Utils.sizeUVInt(vSize);
        off += vSizeSize;
        if (Utils.compareTo(key, keyOffset, keySize, buf + off, kSize) != 0) {
          return NOT_FOUND;
        }
        off -= kSizeSize + vSizeSize;
        // Copy data to buffer
        UnsafeAccess.copy(buf + off, buffer, keyValueSize);
        return keyValueSize;
      } else {
        // Cached item offset in a data segment
        long offset = format.getOffset(buf);
        // segment id
        int sid = (int) format.getSegmentId(buf);
        // Read the data
        Segment s = this.dataSegments[sid];
        if (s == null || !s.isValid()) {
          return NOT_FOUND;
        }

        try {
          s.readLock();
          // Check if scavenger removed this object or moved it to another segment
          int id = this.index.getSegmentId(key, keyOffset, keySize);
          if (id < 0) {
            return NOT_FOUND;
          }
          if (id != sid) {
            s.readUnlock();
            return getRange(key, keyOffset, keySize, rangeStart, rangeSize, hit,  buffer);
          }
          // Read the data
          int res = getRange(sid, offset, keyValueSize, key, keyOffset, keySize, 
            rangeStart, rangeSize, buffer);
          access(s, res, hit);
          return res;
        } finally {
          s.readUnlock();
        }
      }
    } finally {
      UnsafeAccess.free(buf);
    }
  }
  
  /**
   * Get cached item
   *
   * @param id data segment id to read from
   * @param offset data segment offset
   * @param size size of an item in bytes
   * @param key key buffer
   * @param keyOffset offset in a key buffer
   * @param keySize key size
   * @param buffer memory buffer to load data to
   * @param bufOffset offset
   * @return size of a K-V pair or -1 (if not found)
   */
  private int get(
      int id,
      long offset,
      int size,
      byte[] key,
      int keyOffset,
      int keySize,
      byte[] buffer,
      int bufOffset)
      throws IOException {
    if (buffer == null || size > 0 && (buffer.length - bufOffset) < size) {
      throw new IllegalArgumentException();
    }
    this.totalReads.incrementAndGet();
    int result = getFromRAMBuffers(id, offset, size, key, keyOffset, keySize, buffer, bufOffset);
    if (result >= 0) {
      return result;
    }
    if (result == READ_ERROR) {
      //this.totalFailedReads.incrementAndGet();
      return result;
    }
    if (!isOffheap()) {
      return getInternal(id, offset, size, key, keyOffset, keySize, buffer, bufOffset);
    } else {
      return NOT_FOUND;
    }
  }

  /**
   * Get cached item range
   *
   * @param id data segment id to read from
   * @param offset data segment offset
   * @param size size of an item in bytes
   * @param key key buffer
   * @param keyOffset offset in a key buffer
   * @param keySize key size
   * @param rangeStart range start
   * @param rangeSize range size
   * @param buffer memory buffer to load data to
   * @param bufOffset offset
   * @return size of a K-V pair or -1 (if not found)
   */
  private int getRange(int sid, long offset, int keyValueSize, byte[] key, int keyOffset,
      int keySize, int rangeStart, int rangeSize, byte[] buffer, int bufOffset) throws IOException {
    if (buffer == null) {
      throw new IllegalArgumentException("buffer is null");
    }
    this.totalReads.incrementAndGet();

    int result = getRangeFromRAMBuffers(sid, offset, keyValueSize, key, keyOffset, keySize, 
      rangeStart, rangeSize, buffer, bufOffset);
    if (result >= 0) {
      return result;
    }
    if(result == READ_ERROR) {
      this.totalFailedReads.incrementAndGet();
    }
    if (!isOffheap()) {
      return getRangeInternal(sid, offset, keyValueSize, key, keyOffset, keySize, rangeStart, rangeSize, buffer, bufOffset);
    }
    return NOT_FOUND;
  }
  
  /**
   * Get cached item 
   *
   * @param id data segment id to read from
   * @param offset data segment offset
   * @param size size of an item in bytes
   * @param keyPtr key address
   * @param keySize key size
   * @param buffer memory buffer to load data to
   * @param bufOffset offset
   * @return size of a K-V pair or -1 (if not found)
   */
  private int get(
      int id, long offset, int size, long keyPtr, int keySize, byte[] buffer, int bufOffset)
      throws IOException {
    if (buffer == null || size > 0 && (buffer.length - bufOffset) < size) {
      throw new IllegalArgumentException();
    }
    this.totalReads.incrementAndGet();

    int result = getFromRAMBuffers(id, offset, size, keyPtr, keySize, buffer, bufOffset);
    if (result >= 0) {
      return result;
    }
    if(result == READ_ERROR) {
      //this.totalFailedReads.incrementAndGet();
      return result;
    }
    if (!isOffheap()) {
      return getInternal(id, offset, size, keyPtr, keySize, buffer, bufOffset);
    }
    return NOT_FOUND;
  }

  /**
   * Get cached item range
   *
   * @param id data segment id to read from
   * @param offset data segment offset
   * @param size size of an item in bytes
   * @param keyPtr key address
   * @param keySize key size
   * @param rangeStart range start
   * @param rangeSize range size
   * @param buffer memory buffer to load data to
   * @param bufOffset offset
   * @return size of a K-V pair or -1 (if not found)
   */
  private int getRange(
      int id, long offset, int size, long keyPtr, int keySize, int rangeStart, 
      int rangeSize, byte[] buffer, int bufOffset)
      throws IOException {
    if (buffer == null) {
      throw new IllegalArgumentException("buffer is null");
    }
    this.totalReads.incrementAndGet();

    int result = getRangeFromRAMBuffers(id, offset, size, keyPtr, keySize, rangeStart, 
      rangeSize, buffer, bufOffset);
    if (result >= 0) {
      return result;
    }
    if(result == READ_ERROR) {
      this.totalFailedReads.incrementAndGet();
    }
    if (!isOffheap()) {
      return getRangeInternal(id, offset, size, keyPtr, keySize, rangeStart, rangeSize, buffer, bufOffset);
    }
    return NOT_FOUND;
  }
  
  /**
   * Try to get data from RAM buffers
   *
   * @param id segment's id
   * @param offset offset in a segment (0 - based)
   * @param size size of an cached item (can be negative - unknown)
   * @param key key buffer
   * @param keyOffset key offset
   * @param keySize key size
   * @param buffer buffer to load data to
   * @return size of a K-V pair or -1 (if not found)
   */
  private int getFromRAMBuffers(
      int sid,
      long offset,
      int size,
      byte[] key,
      int keyOffset,
      int keySize,
      byte[] buffer,
      int bufOffset) {
    Segment s = getSegmentById(sid);
    try {
      if (s != null) {
        s.readLock();
        // now check s again
        if (!s.isOffheap()) {
          return NOT_FOUND;
        }
        // OK it is in memory
        try {
          return this.memoryDataReader.read(
              this, key, keyOffset, keySize, sid, offset, size, buffer, bufOffset);
        } catch (IOException e) {
          // never happens
          return READ_ERROR;
        }
      }
    } finally {
      if (s != null) {
        s.readUnlock();
      }
    }
    return NOT_FOUND;
  }

 
  /**
   * Try to get data from RAM buffers
   *
   * @param id segment's id
   * @param offset offset in a segment (0 - based)
   * @param size size of an cached item (can be negative - unknown)
   * @param key key buffer
   * @param keyOffset key offset
   * @param keySize key size
   * @param buffer buffer to load data to
   * @return size of a K-V pair or -1 (if not found)
   */
  private int getRangeFromRAMBuffers(
      int sid,
      long offset,
      int size,
      byte[] key,
      int keyOffset,
      int keySize,
      int rangeStart,
      int rangeSize,
      byte[] buffer,
      int bufOffset) {
    Segment s = getSegmentById(sid);
    try {
      if (s != null) {
        s.readLock();
        // now check s again
        if (!s.isOffheap()) {
          return NOT_FOUND;
        }
        // OK it is in memory
        try {
           return this.memoryDataReader.readValueRange(
              this, key, keyOffset, keySize, sid, offset, size, buffer, bufOffset, rangeStart, rangeSize);
        } catch (IOException e) {
          // never happens
          return READ_ERROR;
        }
      }
    } finally {
      if (s != null) {
        s.readUnlock();
      }
    }
    return NOT_FOUND;
  }
  
  /**
   * Try to get data from RAM buffers
   *
   * @param id segment's id
   * @param offset offset in a segment (0 - based)
   * @param size size of an cached item (can be negative - unknown)
   * @param keyPtr key address
   * @param keySize key size
   * @param buffer buffer to load data to
   * @return size of a K-V pair or -1 (if not found)
   */
  private int getFromRAMBuffers(
      int sid, long offset, int size, long keyPtr, int keySize, byte[] buffer, int bufOffset) {
    Segment s = getSegmentById(sid);
    try {
      if (s != null) {
        s.readLock();
        // now check s again
        if (!s.isOffheap()) {
          return NOT_FOUND;
        }
        // OK it is in memory
        try {
          return this.memoryDataReader.read(
              this, keyPtr, keySize, sid, offset, size, buffer, bufOffset);
        } catch (IOException e) {
          // never happens
          return READ_ERROR;
        }
      }
    } finally {
      if (s != null) {
        s.readUnlock();
      }
    }
    return NOT_FOUND;
  }
  
  /**
   * Try to get data from RAM buffers
   *
   * @param id segment's id
   * @param offset offset in a segment (0 - based)
   * @param size size of an cached item (can be negative - unknown)
   * @param keyPtr key address
   * @param keySize key size
   * @param rangeStart range start
   * @param rangeSize,
   * @param buffer buffer to load data to
   * @return size of a K-V pair or -1 (if not found)
   */
  private int getRangeFromRAMBuffers(
      int sid, long offset, int size, long keyPtr, int keySize, int rangeStart, int rangeSize, 
      byte[] buffer, int bufOffset) {
    Segment s = getSegmentById(sid);
    try {
      if (s != null) {
        s.readLock();
        // now check s again
        if (!s.isOffheap()) {
          return NOT_FOUND;
        }
        // OK it is in memory
        try {
          return this.memoryDataReader.readValueRange(
              this, keyPtr, keySize, sid, offset, size, buffer, bufOffset, rangeStart, rangeSize);
        } catch (IOException e) {
          // never happens
          return READ_ERROR;
        }
      }
    } finally {
      if (s != null) {
        s.readUnlock();
      }
    }
    return NOT_FOUND;
  }
  /**
   * Try to get data from RAM buffers
   *
   * @param id segment's id
   * @param offset offset in a segment (0 - based)
   * @param size size of an cached item
   * @param key key buffer
   * @param keyOffset key offset
   * @param keySize key size
   * @param buffer buffer to load data to
   * @return full size required or -1
   */
  private int getFromRAMBuffers(
      int sid, long offset, int size, byte[] key, int keyOffset, int keySize, ByteBuffer buffer) {
    Segment s = getSegmentById(sid);
    try {
      if (s != null) {
        s.readLock();
        // now check s again
        if (!s.isOffheap()) {
          return NOT_FOUND;
        }
        // OK it is in memory
        try {
          return this.memoryDataReader.read(
              this, key, keyOffset, keySize, sid, offset, size, buffer);
        } catch (IOException e) {
          // never happens
          return READ_ERROR;
        }
      }
    } finally {
      if (s != null) {
        s.readUnlock();
      }
    }
    return NOT_FOUND;
  }

  /**
   * Try to get data from RAM buffers
   *
   * @param id segment's id
   * @param offset offset in a segment (0 - based)
   * @param size size of an cached item
   * @param key key buffer
   * @param keyOffset key offset
   * @param keySize key size
   * @param rangeStart range start
   * @param rangeSize
   * @param buffer buffer to load data to
   * @return full size required or -1
   */
  private int getRangeFromRAMBuffers(
      int sid, long offset, int size, byte[] key, int keyOffset, int keySize, 
      int rangeStart, int rangeSize, ByteBuffer buffer) {
    Segment s = getSegmentById(sid);
    try {
      if (s != null) {
        s.readLock();
        // now check s again
        if (!s.isOffheap()) {
          return NOT_FOUND;
        }
        // OK it is in memory
        try {
          return this.memoryDataReader.readValueRange(
              this, key, keyOffset, keySize, sid, offset, size, buffer, rangeStart, rangeSize);
        } catch (IOException e) {
          // never happens
          return READ_ERROR;
        }
      }
    } finally {
      if (s != null) {
        s.readUnlock();
      }
    }
    return NOT_FOUND;
  }

  
  /**
   * Try to get data from RAM buffers
   *
   * @param id segment's id
   * @param offset offset in a segment (0 - based)
   * @param size size of an cached item
   * @param keyPtr key address
   * @param keySize key size
   * @param buffer buffer to load data to
   * @return full size required or -1
   */
  private int getFromRAMBuffers(
      int sid, long offset, int size, long keyPtr, int keySize, ByteBuffer buffer) {
    Segment s = getSegmentById(sid);
    try {
      if (s != null) {
        s.readLock();
        // now check s again
        if (!s.isOffheap()) {
          return NOT_FOUND;
        }
        // OK it is in memory
        try {
          return this.memoryDataReader.read(this, keyPtr, keySize, sid, offset, size, buffer);
        } catch (IOException e) {
          // never happens
          return READ_ERROR;
        }
      }
    } finally {
      if (s != null) {
        s.readUnlock();
      }
    }
    return NOT_FOUND;
  }

  /**
   * Try to get data from RAM buffers
   *
   * @param id segment's id
   * @param offset offset in a segment (0 - based)
   * @param size size of an cached item
   * @param keyPtr key address
   * @param keySize key size
   * @param rangeStart range start
   * @param rangeSize range size
   * @param buffer buffer to load data to
   * @return full size required or -1
   */
  private int getRangeFromRAMBuffers(
      int sid, long offset, int size, long keyPtr, int keySize, 
      int rangeStart, int rangeSize, ByteBuffer buffer) {
    Segment s = getSegmentById(sid);
    try {
      if (s != null) {
        s.readLock();
        // now check s again
        if (!s.isOffheap()) {
          return NOT_FOUND;
        }
        // OK it is in memory
        try {
          return this.memoryDataReader.readValueRange(this, keyPtr, keySize, sid, 
            offset, size, buffer, rangeStart, rangeSize);
        } catch (IOException e) {
          // never happens
          return READ_ERROR;
        }
      }
    } finally {
      if (s != null) {
        s.readUnlock();
      }
    }
    return NOT_FOUND;
  }
  
  /**
   * Get cached item from underlying IOEngine implementation
   *
   * @param id segment id
   * @param offset offset in a segment
   * @param size size of an item
   * @param key key buffer
   * @param keyOffset key offset
   * @param keySize key Size
   * @param buffer buffer to load data to
   * @param bufOffset offset
   * @return full size required or -1
   * @throws IOException
   */
  protected abstract int getInternal(
      int id,
      long offset,
      int size,
      byte[] key,
      int keyOffset,
      int keySize,
      byte[] buffer,
      int bufOffset)
      throws IOException;

  /**
   * Get cached item from underlying IOEngine implementation
   *
   * @param id segment id
   * @param offset offset in a segment
   * @param size size of an item
   * @param keyPtr key address
   * @param keySize key Size
   * @param buffer buffer to load data to
   * @param bufOffset offset
   * @return full size required or -1
   * @throws IOException
   */
  protected abstract int getInternal(
      int id, long offset, int size, long keyPtr, int keySize, byte[] buffer, int bufOffset)
      throws IOException;

  /**
   * Get cached item from underlying IOEngine implementation
   *
   * @param id segment id
   * @param offset offset in a segment
   * @param size size of an item
   * @param key key buffer
   * @param keyOffset offset
   * @param keySize key size
   * @param buffer buffer to load data to
   * @return full size required or -1
   * @throws IOException
   */
  protected abstract int getInternal(
      int id, long offset, int size, byte[] key, int keyOffset, int keySize, ByteBuffer buffer)
      throws IOException;

  /**
   * Get cached item from underlying IOEngine implementation
   *
   * @param id segment id
   * @param offset offset in a segment
   * @param size size of an item
   * @param keyPtr key address
   * @param keySize key size
   * @param buffer buffer to load data to
   * @return full size required or -1
   * @throws IOException
   */
  protected abstract int getInternal(
      int id, long offset, int size, long keyPtr, int keySize, ByteBuffer buffer)
      throws IOException;
  
 
  /**
   * Get cached item from underlying IOEngine implementation
   *
   * @param id segment id
   * @param offset offset in a segment
   * @param size size of an item
   * @param key key buffer
   * @param keyOffset key offset
   * @param keySize key Size
   * @param rangeStart range start
   * @param rangeSize range size
   * @param buffer buffer to load data to
   * @param bufOffset offset
   * @return full size required or -1
   * @throws IOException
   */
  protected abstract int getRangeInternal(
      int id,
      long offset,
      int size,
      byte[] key,
      int keyOffset,
      int keySize,
      int rangeStart,
      int rangeSize,
      byte[] buffer,
      int bufOffset)
      throws IOException;

  /**
   * Get cached item range from underlying IOEngine implementation
   *
   * @param id segment id
   * @param offset offset in a segment
   * @param size size of an item
   * @param keyPtr key address
   * @param keySize key Size
   * @param rangeStart range start
   * @param rangeSize range size
   * @param buffer buffer to load data to
   * @param bufOffset offset
   * @return full size required or -1
   * @throws IOException
   */
  protected abstract int getRangeInternal(
      int id, long offset, int size, long keyPtr, int keySize, int rangeStart, int rangeSize, 
      byte[] buffer, int bufOffset)
      throws IOException;

  /**
   * Get cached item range from underlying IOEngine implementation
   *
   * @param id segment id
   * @param offset offset in a segment
   * @param size size of an item
   * @param key key buffer
   * @param keyOffset offset
   * @param keySize key size
   * @param rangeStart range start
   * @param rangeSize range size
   * @param buffer buffer to load data to
   * @return full size required or -1
   * @throws IOException
   */
  protected abstract int getRangeInternal(
      int id, long offset, int size, byte[] key, int keyOffset, int keySize, int rangeStart, 
      int rangeSize, ByteBuffer buffer)
      throws IOException;

  /**
   * Get cached item range from underlying IOEngine implementation
   *
   * @param id segment id
   * @param offset offset in a segment
   * @param size size of an item
   * @param keyPtr key address
   * @param keySize key size
   * @param rangeStart range start
   * @param rangeSize range size
   * @param buffer buffer to load data to
   * @return full size required or -1
   * @throws IOException
   */
  protected abstract int getRangeInternal(
      int id, long offset, int size, long keyPtr, int keySize, int rangeStart, int rangeSize,  ByteBuffer buffer)
      throws IOException;
  
  
  /**
   * Get cached item
   *
   * @param id data segment id to read from
   * @param offset data segment offset
   * @param size size of an item in bytes
   * @param key key buffer
   * @param keyOffset offset in a key buffer
   * @param keySize key size
   * @param buffer byte buffer to load data to
   * @return full size required or -1
   */
  private int get(
      int id, long offset, int size, byte[] key, int keyOffset, int keySize, ByteBuffer buffer)
      throws IOException {
    if (buffer == null || buffer.remaining() < size) {
      throw new IllegalArgumentException();
    }
    this.totalReads.incrementAndGet();

    int result = getFromRAMBuffers(id, offset, size, key, keyOffset, keySize, buffer);
    if (result >= 0) {
      return result;
    }
    if (result == READ_ERROR) {
      //this.totalFailedReads.incrementAndGet();
      return result;
    }
    if (!isOffheap()) {
      return getInternal(id, offset, size, key, keyOffset, keySize, buffer);
    }
    return NOT_FOUND;
  }

  /**
   * Get cached item range
   *
   * @param id data segment id to read from
   * @param offset data segment offset
   * @param size size of an item in bytes
   * @param key key buffer
   * @param keyOffset offset in a key buffer
   * @param keySize key size
   * @param rangeStart range start
   * @param rangeSize range size
   * @param buffer byte buffer to load data to
   * @return full size required or -1
   */
  private int getRange(
      int id, long offset, int size, byte[] key, int keyOffset, int keySize, 
      int rangeStart, int rangeSize, ByteBuffer buffer)
      throws IOException {
    if (buffer == null) {
      throw new IllegalArgumentException("buffer is null");
    }
    this.totalReads.incrementAndGet();

    int result = getRangeFromRAMBuffers(id, offset, size, key, keyOffset, keySize, rangeStart, rangeSize, buffer);
    if (result >= 0) {
      return result;
    }
    if (result == READ_ERROR) {
      this.totalFailedReads.incrementAndGet();
    }
    if (!isOffheap()) {
      return getRangeInternal(id, offset, size, key, keyOffset, keySize, rangeStart, rangeSize, buffer);
    }
    return NOT_FOUND;
  }
  
  /**
   * Get cached item
   *
   * @param id data segment id to read from
   * @param offset data segment offset
   * @param size size of an item in bytes
   * @param buffer byte buffer to load data to
   * @return true - on success, false - otherwise
   */
  private int get(int id, long offset, int size, long keyPtr, int keySize, ByteBuffer buffer)
      throws IOException {
    if (buffer == null || buffer.remaining() < size) {
      throw new IllegalArgumentException();
    }
    this.totalReads.incrementAndGet();

    int result = getFromRAMBuffers(id, offset, size, keyPtr, keySize, buffer);

    if (result >= 0) {
      return result;
    }
    if (result == READ_ERROR) {
      //this.totalFailedReads.incrementAndGet();
      return result;
    }
    if (!isOffheap()) {
      return getInternal(id, offset, size, keyPtr, keySize, buffer);
    }
    return NOT_FOUND;
  }

  /**
   * Get cached item range
   *
   * @param id data segment id to read from
   * @param offset data segment offset
   * @param size size of an item in bytes
   * @param buffer byte buffer to load data to
   * @return true - on success, false - otherwise
   */
  private int getRange(int id, long offset, int size, long keyPtr, int keySize, 
      int rangeStart, int rangeSize, ByteBuffer buffer)
      throws IOException {
    if (buffer == null) {
      throw new IllegalArgumentException("buffer is null");
    }
    this.totalReads.incrementAndGet();

    int result = getRangeFromRAMBuffers(id, offset, size, keyPtr, keySize, rangeStart, rangeSize, buffer);

    if (result >= 0) {
      return result;
    }
    if (result == READ_ERROR) {
      this.totalFailedReads.incrementAndGet();
    }
    if (!isOffheap()) {
      return getRangeInternal(id, offset, size, keyPtr, keySize, rangeStart, rangeSize, buffer);
    }
    return NOT_FOUND;
  }
  /**
   * Save data segment
   *
   * @param data data segment
   */
  public void save(Segment data) throws IOException {

    try {
      data.writeLock();
      if (data.isSealed()) {
        return;
      }
      // TODO: remove this. Move data to a main storage
      this.dataSegments[data.getId()] = data;
      this.ramBuffers[data.getInfo().getGroupRank()] = null;
      // }
      // Call IOEngine - specific (FileIOEngine overrides it)
      // Can be costly - executed in a separate thread
      saveInternal(data);
      // seal only after we materialize segment in a file system
      // Notify listener
      if (this.aListener != null) {
        aListener.onEvent(this, IOEngineEvent.DATA_SIZE_CHANGED);
      }
    } finally {
      data.writeUnlock();
    }
  }

  /**
   * IOEngine subclass can override this method
   *
   * @param data data segment
   * @throws IOException
   */
  protected void saveInternal(Segment data) throws IOException {}

  /**
   * Get maximum storage size (depends on IOEngine)
   *
   * @return maximum storage size
   */
  public long getMaximumStorageSize() {
    return this.maxStorageSize;
  }

  /**
   * Creates segment scanner
   *
   * @param s segment
   * @return segment scanner
   * @throws IOException
   */
  public abstract SegmentScanner getScanner(Segment s) throws IOException;

  /**
   * Release id after segment recycling
   *
   * @param seg data segment 
   */
  public void disposeDataSegment(Segment seg) {
    //FIXME: does not work for compressed data
    //long dataSize = seg.getInfo().getSegmentDataSize();
    try {
      seg.writeLock();
      if (seg.isOffheap()) {
        boolean res = this.memoryBufferPool.offer(seg.getAddress());
        if(res) {
          seg.setAddress(0);
        }
      }
      seg.dispose();
      dataSegments[seg.getId()] = null;
      reportAllocation(-this.segmentSize);
      //FIXME: must update actual usage (compressed)
      // usage uncompressed was updated in Scavenger
      //reportUsage(-dataSize);
    } finally {
      seg.setRecycling(false);
      seg.writeUnlock();
    }
  }
  /**
   * Update statistics for a segment with a given id for eviction, deletion
   *
   * @param id segment id
   * @param expire expiration time (can be -1)
   */
  public void updateStats(int id, long expire) {
    checkId(id);
    Segment s = this.dataSegments[id];
    if (s == null) {
      return; // possible when segment was recycled recently
    }
    if (expire < 0) {
      s.updateEvictedDeleted();
    } else {
      s.updateExpired(expire);
    }
  }
  
  /**
   * Get recycling selector
   * @return
   */
  public RecyclingSelector getRecyclingSelector() {
    return this.recyclingSelector;
  }
  /**
   * Get best segment for recycling MUST be sealed TODO: need synchronized?
   *
   * @return segment
   */
  public synchronized Segment getSegmentForRecycling() {
    Segment s = this.recyclingSelector.selectForRecycling(dataSegments);
    if (s != null && !s.isSealed()) {
      throw new RuntimeException("Segment for recycling must be sealed");
    }
    return s;
  }

  /**
   * Scans and finds available id for a new data segment
   *
   * @return id (or -1)
   */
  protected final int getAvailableId() {
    for (int i = 0; i < dataSegments.length; i++) {
      if (dataSegments[i] == null) {
        return i;
      }
    }
    return NOT_FOUND; // not found
  }

  private void checkId(int id) {
    if (id < 0 || id >= dataSegments.length) {
      throw new IllegalArgumentException(String.format("illegal id %d ", id));
    }
  }

  /**
   * Delete key from a cache
   *
   * @param key key buffer
   * @param off key offset
   * @param size key size
   * @return true on success, false otherwise
   * @throws IOException
   */
  public boolean delete(byte[] key, int off, int size) throws IOException {
    // Delete from index
    boolean result = this.index.delete(key, off, size);
    if (result) {
      this.totalDeletes.incrementAndGet();
    }
    return result;
  }

  /**
   * Delete key from a cache
   *
   * @param keyPtr key address
   * @param size key size
   * @return true on success, false otherwise
   * @throws IOException
   */
  public boolean delete(long keyPtr, int size) throws IOException {
    // Delete from index
    boolean result = this.index.delete(keyPtr, size);
    if (result) {
      this.totalDeletes.incrementAndGet();
    }
    return result;
  }

  /**
   * Put key-value into a cache
   *
   * @param key key buffer
   * @param value value buffer
   * @param expire expiration time
   * @throws IOException
   * @return true on success, false - otherwise
   */
  public boolean put(byte[] key, byte[] value, long expire) throws IOException {
    return put(key, 0, key.length, value, 0, value.length, expire, this.defaultRank);
  }

  /**
   * Put key-value into a cache with a rank
   *
   * @param key key buffer
   * @param value value buffer
   * @param expire expiration time
   * @param rank cache item rank
   * @return true on success, false - otherwise
   * @throws IOException
   */
  public boolean put(byte[] key, byte[] value, long expire, int rank) throws IOException {
    return put(key, 0, key.length, value, 0, value.length, expire, rank);
  }

  /**
   * Put key-value into a cache
   *
   * @param key key buffer
   * @param keyOff key offset
   * @param keyLength key length
   * @param value value buffer
   * @param valueOff value offset
   * @param valueLength value length
   * @param expire absolute expiration time in ms, 0 - no expire
   * @return true on success , false otherwise
   * @throws IOException
   */
  public boolean put(
      byte[] key,
      int keyOff,
      int keyLength,
      byte[] value,
      int valueOff,
      int valueLength,
      long expire)
      throws IOException {
    return put(key, keyOff, keyLength, value, valueOff, valueLength, expire, this.defaultRank);
  }
  
  /**
   * Put key-value into a cache
   *
   * @param key key buffer
   * @param keyOff key offset
   * @param keyLength key length
   * @param value value buffer
   * @param valueOff value offset
   * @param valueLength value length
   * @param expire absolute expiration time in ms, 0 - no expire
   * @param rank rank of a cache item
   * @return true on success, false - otherwise
   * @throws IOException
   */
  public boolean put(
      byte[] key,
      int keyOff,
      int keyLength,
      byte[] value,
      int valueOff,
      int valueLength,
      long expire,
      int rank)
      throws IOException {
    int groupRank = rank;
    return put(key, keyOff, keyLength, value, valueOff, valueLength, expire, rank, groupRank, false);
  }
  /**
   * Put key-value into a cache 
   *
   * @param key key buffer
   * @param keyOff key offset
   * @param keyLength key length
   * @param value value buffer
   * @param valueOff value offset
   * @param valueLength value length
   * @param expire absolute expiration time in ms, 0 - no expire
   * @param rank rank of a cache item
   * @param groupRank group rank
   * @return true on success, false - otherwise
   * @throws IOException
   */
  
  public boolean put(
      byte[] key,
      int keyOff,
      int keyLength,
      byte[] value,
      int valueOff,
      int valueLength,
      long expire,
      int rank,
      int groupRank,
      boolean scavenger)
      throws IOException {
    checkRank(rank);
    checkRank(groupRank);

    Segment s = getRAMSegmentByRank(groupRank);

    if (s == null) {
      // We silently ignore PUT operation due to lack of resources
      // TODO: update stats
      return false;
    }
    // Offset must less 32bit
    long offset = s.append(key, keyOff, keyLength, value, valueOff, valueLength, expire);

    if (offset < 0) {
      if(!s.isSealed()) {
        //FIXME: is it sync call
        save(s); // removes segment from RAM buffers
      }
      s = getRAMSegmentByRank(groupRank);
      if (s == null) {
        // We silently ignore PUT operation due to lack of resources
        // TODO: update stats
        return false;
      }
      offset = s.append(key, keyOff, keyLength, value, valueOff, valueLength, expire);
    }
    int kvSize = Utils.kvSize(keyLength, valueLength);
    reportRawDataSize(kvSize);
    //reportStorageUsed(kvSize);
    MutationResult result = this.index.insertWithRank(
        key,
        keyOff,
        keyLength,
        value,
        valueOff,
        valueLength,
        (short) s.getId(),
        (int) offset,
        rank,
        expire);
    if (result == MutationResult.INSERTED) {
      this.totalInserts.incrementAndGet();
    } else if (result == MutationResult.UPDATED && !scavenger) {
      this.totalUpdates.incrementAndGet(); 
    }
    return true;
  }

  /**
   * Put key-value into a cache 
   *
   * @param keyPtr key address
   * @param keyLength key length
   * @param valuePtr value address
   * @param valueLength value length
   * @param expire absolute expiration time in ms, 0 - no expire
   * @return true on success, false - otherwise
   * @throws IOException
   */
  public boolean put(long keyPtr, int keyLength, long valuePtr, int valueLength, long expire)
      throws IOException {

    return put(keyPtr, keyLength, valuePtr, valueLength, expire, this.defaultRank);
  }
  
  /**
   * Put key-value into a cache with a rank
   *
   * @param keyPtr key address
   * @param keyLength key length
   * @param valuePtr value address
   * @param valueLength value length
   * @param expire absolute expiration time in ms, 0 - no expire
   * @param rank rank of a cache item
   * @throws IOException
   */
  public boolean put(
      long keyPtr, int keyLength, long valuePtr, int valueLength, 
      long expire, int rank) throws IOException {
    int groupRank = rank;
    return put(keyPtr, keyLength, valuePtr, valueLength, expire, rank, groupRank, false);
  }
  /**
   * Put key-value into a cache with a rank and group rank
   *
   * @param keyPtr key address
   * @param keyLength key length
   * @param valuePtr value address
   * @param valueLength value length
   * @param expire absolute expiration time in ms, 0 - no expire
   * @param rank rank of a cache item
   * @param groupRank group rank
   * @throws IOException
   */
  public boolean put(
      long keyPtr, int keyLength, long valuePtr, int valueLength, 
      long expire, int rank, int groupRank, boolean scavenger)
      throws IOException {
    checkRank(rank);
    checkRank(groupRank);
    Segment s = getRAMSegmentByRank(groupRank);
    if (s == null) {
      // We silently ignore PUT operation due to lack of resources
      // TODO: update stats
      return false;
    }
    long offset = s.append(keyPtr, keyLength, valuePtr, valueLength, expire);
    if (offset < 0) {
      if(!s.isSealed()) {
        save(s); // removes segment from RAM buffers
      }
      s = getRAMSegmentByRank(groupRank);
      if (s == null) {
        // We silently ignore PUT operation due to lack of resources
        // TODO: update stats
        return false;
      }
      offset = s.append(keyPtr, keyLength, valuePtr, valueLength, expire);
    }

    int kvSize = Utils.kvSize(keyLength, valueLength);
    reportRawDataSize(kvSize);
    //reportStorageUsed(kvSize);
    
    MutationResult result = this.index.insertWithRank(
        keyPtr, keyLength, valuePtr, valueLength, (short) s.getId(), (int) offset, rank, expire);
    if (result == MutationResult.INSERTED) {
      this.totalInserts.incrementAndGet();
    } else if (result == MutationResult.UPDATED && !scavenger) {
      this.totalUpdates.incrementAndGet(); 
    }
    return true;
  }

  protected ReentrantLock ramBufferLock = new ReentrantLock();
  
  /**
   * FIXME: the source of thread contention
   * @param rank
   * @return
   */
  protected Segment getRAMSegmentByRank(int rank) {
    Segment s = this.ramBuffers[rank];
    if (s == null) {
      try {
        ramBufferLock.lock();
        s = this.ramBuffers[rank];
        if (s != null) {
          return s;
        }
        int id = getAvailableId();
        if (id < 0) {
          return null;
        }
        if (this.dataSegments[id] == null) {
          long ptr = this.memoryBufferPool.poll();
          s = Segment.newSegment(ptr, (int) this.segmentSize, id, rank);
          s.init(this.cacheName);
          reportAllocation(this.segmentSize);
          // Set data appender
          s.setDataWriterAndEngine(this.dataWriter, this);
          this.dataSegments[id] = s;

        } else {
          s = this.dataSegments[id];
          s.reuse(id, rank, System.currentTimeMillis());
        }
        this.ramBuffers[rank] = s;
      } finally {
        ramBufferLock.unlock();
      }
    }
    return s;
  }

  private void checkRank(int rank) {
    if (rank < 0 || rank >= ramBuffers.length) {
      throw new IllegalArgumentException(String.format("Illegal rank value: %d", rank));
    }
  }

  @Override
  public void save(OutputStream os) throws IOException {
    DataOutputStream dos = Utils.toDataOutputStream(os);
    int num = getNumberOfActiveSegments();
    dos.writeInt(num);
    // Save all segment meta info
    for (Segment s : this.dataSegments) {
      if (s == null) {
        continue;
      }
      num++;
      s.save(dos);
    }
    // Save index
    this.index.save(dos);
    this.recyclingSelector.save(dos);
    dos.writeLong(this.storageAllocated.get());
    dos.writeLong(this.rawDataSize.get());
    dos.writeLong(this.storageUsed.get());
    dos.writeLong(this.totalInserts.get());
    dos.writeLong(this.totalUpdates.get());
    dos.writeLong(this.totalDeletes.get());
    dos.writeLong(this.totalIOReadDuration.get());
    dos.writeLong(this.totalReads.get());
    dos.writeLong(this.totalFailedReads.get());
    // Codec 
    CodecFactory.getInstance().saveCodecForCache(cacheName, dos);
    if (this.writeBatches != null) {
      this.writeBatches.save(dos);
    }
    dos.close();
  }

  protected int getNumberOfActiveSegments() {
    int num = 0;
    synchronized (this.dataSegments) {
      for (Segment s : this.dataSegments) {
        if (s != null) num++;
      }
    }
    return num;
  }

  @Override
  public void load(InputStream is) throws IOException {
    DataInputStream dis = Utils.toDataInputStream(is);
    int num = dis.readInt();
    for (int i = 0; i < num; i++) {
      Segment s = new Segment();
      s.load(dis);
      s.setDataWriterAndEngine(this.dataWriter, this);
      this.dataSegments[s.getId()] = s;
    }
    this.index.load(dis);
    this.recyclingSelector.load(dis);
    this.storageAllocated.set(dis.readLong());
    this.rawDataSize.set(dis.readLong());
    this.storageUsed.set(dis.readLong());
    this.totalInserts.set(dis.readLong());
    this.totalUpdates.set(dis.readLong());
    this.totalDeletes.set(dis.readLong());
    this.totalIOReadDuration.set(dis.readLong());
    this.totalReads.set(dis.readLong());
    this.totalFailedReads.set(dis.readLong());
    // Codec
    CodecFactory.getInstance().initCompressionCodecForCache(cacheName, dis);
    if (this.writeBatches != null) {
      this.writeBatches.load(dis);
    }
    dis.close();
  }

  /**
   * Get data segment file name
   *
   * @param id data segment id
   * @return file name
   */
  protected String getSegmentFileName(int id) {
    return FILE_NAME + Utils.format(Integer.toString(id), 6);
  }

  /** Dispose I/O engine - used for testing */
  public void dispose() {
    // 1. dispose memory segments
    for (Segment s : this.dataSegments) {
      if (s == null) {
        continue;
      }
      s.dispose();
    }
    // 2. dispose memory index
    this.index.dispose();
    // 3. Dispose memory buffer pool
    this.memoryBufferPool.dispose();
    // 4. Dispose write batches
    if (this.writeBatches != null) {
      this.writeBatches.dispose();
    }
  }

  /**
   * Number of cached items currently in the cache
   *
   * @return number
   */
  public long size() {
    long total = 0;
    for (Segment s : this.dataSegments) {
      if (s == null) {
        continue;
      }
      total += s.getTotalItems();
    }
    return total;
  }

  /**
   * Get total data size
   *
   * @return data size
   */
  public long dataSize() {
    long total = 0;
    for (Segment s : this.dataSegments) {
      if (s == null) {
        continue;
      }
      total += s.getSegmentDataSize();
    }
    return total;
  }

  /**
   * Get number of active cached items (still accessible)
   * FIXME: optimize, check hot path calls
   * @return number
   */
  public long activeSize() {
    long total = 0;
    for (Segment s : this.dataSegments) {
      if (s == null) {
        continue;
      }
      total += s.getTotalItems() - s.getNumberEvictedDeletedItems() - s.getNumberExpiredItems();
    }
    return total;
  }

  /**
   * Active size ratio
   *
   * @return active size ratio
   */
  public double activeSizeRatio() {
    long size = size();
    if (size == 0) return 1d;
    long activeSize = activeSize();
    return (double) activeSize / size;
  }

  /**
   * Active data size (estimate)
   *
   * @return active data size
   */
  public long activeDataSize() {
    long s = size();
    if (s == 0) {
      return 0;
    }
    long as = activeSize();
    double ratio = (double) as / s;
    return (long) (ratio * dataSize());
  }

  /**
   * Called by Scavenger
   *
   * @param s segment to recycle
   */
  public void startRecycling(Segment s) {
    for (Scavenger.Listener l : this.scavengerListeners) {
      l.startSegment(s);
    }
  }

  /**
   * Called by Scavenger
   *
   * @param s segment to recycle
   */
  public void finishRecycling(Segment s) {
    for (Scavenger.Listener l : this.scavengerListeners) {
      l.finishSegment(s);
    }
  }
  
  public void shutdown() {
    // do nothing, delegate to subclass
    this.memoryBufferPool.shutdown();
  }
  
  public Segment[] getDataSegmentsSorted() {
    Arrays.sort(dataSegments, new Comparator<Segment>() {
      @Override
      public int compare(Segment o1, Segment o2) {
        if (o1 == null && o2 == null) return 0;
        if (o1 == null) return -1;
        if (o2 == null) return 1;
        
        return (int) (o1.getInfo().getCreationTime() - o2.getInfo().getCreationTime());
      }      
    });
    return this.dataSegments;
  }
  
  /**
   * Get write batches
   * @return write batches
   */
  WriteBatches getWriteBatches() {
    return this.writeBatches;
  }
  
  protected abstract boolean isOffheap() ;

}
