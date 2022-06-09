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
package com.carrot.cache.index;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.carrot.cache.Cache;
import com.carrot.cache.eviction.EvictionListener;
import com.carrot.cache.eviction.EvictionPolicy;
import com.carrot.cache.eviction.FIFOEvictionPolicy;
import com.carrot.cache.util.CacheConfig;
import com.carrot.cache.util.Persistent;
import com.carrot.cache.util.UnsafeAccess;
import com.carrot.cache.util.Utils;

/**
 * Memory Index is a dynamic hash table, which implements smart incremental rehashing technique to
 * avoid large pauses during operation. It supports blocked rehashing as well
 *
 * <p>[SHORT] - block size [SHORT] - number of entries [SHORT] - data size [entry]+
 * 
 * This is fixed header size. Implementation of IndexFormat can increase header size, but first 
 * 6 bytes are always fixed. 
 */
public class MemoryIndex implements Persistent {
  /** Logger */
  private static final Logger LOG = LogManager.getLogger(MemoryIndex.class);
  
  public static enum MutationResult{
    //REHASH_REQUESTED,
    INSERTED, /* Operation succeeded */ 
    DELETED,  /* Operation succeeded (for AQ and MQ), but existing one was deleted in case of AQ*/
    FAILED    /* Failed due to memory index full rehashing  */
  }
  
  public static enum Type {
    AQ, /* Admission Queue*/
    /*
     * AQ Index item is 8 bytes hashed key value
     */
    MQ  /* Main Queue */
    /*
     * MQ Index item is 20 bytes:
     * 8 bytes - hashed key value
     * 4 bytes - total item size (key + value)
     * 8 bytes - location in the storage - information 
     */
  }
  
  
  /* Failure code */
  private static final int FAILED = -1;
  
  /* Not found code */
  private static final int NOT_FOUND = -1;
  
  /* Offsets in meta section of an index segment*/
  private static final int BLOCK_SIZE_OFFSET = 0;
  
  /* Entries offset */
  private static final int ENTRIES_OFFSET = Utils.SIZEOF_SHORT;
  
  /*Data size offset */
  private static final int DATA_SIZE_OFFSET = 2 * Utils.SIZEOF_SHORT;
  
  
  /*
   * TODO: make this configurable
   * TODO: Optimal block ratios (check jemalloc sizes)
   * 512-4096 with step 256 - this is jemalloc specific
   * sizes of allocation
   * 256 * 2, 3, 4, ... 16
   */
  public static int BASE_SIZE = 128;
  // TODO: align block multipliers with jemalloc
  // 4K block
  static int[] BASE_MULTIPLIERS =
      new int[] {
        2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 18, 20, 22, 24, 26, 28, 30, 32
      };

  /**
   * Get maximum index block size
   * @return maximum index block size
   */
  public static int getMaximumBlockSize() {
    return BASE_SIZE * BASE_MULTIPLIERS[BASE_MULTIPLIERS.length - 1];
  }

  /**
   * Get minimum index block size
   * @return minimum index block size
   */
  public static int getMinimumBlockSize() {
    return BASE_SIZE * BASE_MULTIPLIERS[0];
  }
  
  
  /**
   * Get min size greater than current
   *
   * @param max - max size
   * @param current current size
   * @return min size or -1;
   */
  static int getMinSizeGreaterOrEqualsThan(int current) {
    for (int i = 0; i < BASE_MULTIPLIERS.length; i++) {
      int size = BASE_SIZE * BASE_MULTIPLIERS[i];
      // CHANGE
      if (size >= current) return size;
    }
    return FAILED;
  }

  /* Global locks */
  private ReentrantReadWriteLock[] locks = new ReentrantReadWriteLock[1117];
  
  /** Index base array 
   * TODO: use native memory */
  private AtomicReference<long[]> ref_index_base = new AtomicReference<long[]>();
  
  /** When rehash is in progress this is the rehash destination */
  private AtomicReference<long[]> ref_index_base_rehash = new AtomicReference<long[]>();
  
  /* Cache configuration */
  private CacheConfig cacheConfig;
  
  /* Eviction policy */
  private EvictionPolicy evictionPolicy;
  
  /* Eviction listener */
  private EvictionListener evictionListener;
  
  /* Index type */
  private Type indexType;
  
  /* Index entry size */
  private int indexSize;
  
  // index block size (2) followed by number of entries (2) followed by data size (2) 
  private volatile int indexBlockHeaderSize = 3 * Utils.SIZEOF_SHORT;

  /* Is eviction enabled yet?*/
  private volatile boolean evictionEnabled = false;
  
  /* Total number of index entries */
  private AtomicLong numEntries = new AtomicLong(0);
  
  /* Maximum number of entries - for AQ*/
  private volatile long maxEntries = 0; // 0 - means no max
  
  /* Parent cache  */
  private Cache cache;
  
  /* Parent cache name */
  private String cacheName;
  
  /* Index format */
  private IndexFormat indexFormat;
  
  /* Is rehashing in progress */
  private volatile boolean rehashInProgress;
  
  /* Number of rehashed slots so far */
  private AtomicLong rehashedSlots = new AtomicLong();
  
  public MemoryIndex() {
    this.cacheConfig = CacheConfig.getInstance();
    initLocks();
  }
  
  /**
   * Constructor
   *
   * @param type index type
   */
  public MemoryIndex(Cache cache, Type type) {
    this.cache = cache;
    this.cacheConfig = CacheConfig.getInstance();
    this.cacheName = this.cache.getName();
    init();
    setType(type);
  }

  /**
   * For testing
   * @param cacheName
   * @param type
   */
  public MemoryIndex(String cacheName, Type type) {
    this.cacheConfig = CacheConfig.getInstance();
    this.cacheName = cacheName;
    init();
    setType(type);
  }
  

  /**
   * Disposes array of pointers
   */
  public void dispose() {
    //FIXME: not a thread safe, can't be called twice
    Arrays.stream(ref_index_base.get()).forEach( x -> {if (x != 0) UnsafeAccess.free(x);});
    if (ref_index_base_rehash.get() != null) {
      Arrays.stream(ref_index_base_rehash.get()).forEach( x -> {if (x != 0) UnsafeAccess.free(x);});
    }
  }
  
  /**
   * Set eviction listener
   * @param aListener
   */
  public void setEvictionListener(EvictionListener aListener) {
    this.evictionListener = aListener;
  }
  
  /**
   * Get eviction listener
   * @return eviction listener
   */
  public EvictionListener getEvictionListener() {
    return this.evictionListener;
  }
  
  /**
   * Set index format
   * @param format index format
   */
  public void setIndexFormat (IndexFormat format) {
    this.indexFormat = format;
  }
  
  /**
   * Get index format
   * @return index format
   */
  public IndexFormat getIndexFormat() {
    return this.indexFormat;
  }
  
  /**
   * Size of the index in number of entries
   * @return number of entries
   */
  public long size() {
    return this.numEntries.get();
  }
  
  /**
   * Increment number of index entries
   * TODO: eviction ON/OFF
   * @param n increment value
   * @return number of entries after increment
   */
  private long incrIndexSize(int n) {
    long c = this.numEntries.addAndGet(n);
    checkEviction();
    return c;
  }
  
  /**
   * Trigger eviction on/off
   */
  private void checkEviction() {
    if (this.indexType != Type.AQ) return;
    if (this.maxEntries == 0) return; // no limit - good for testing
    if (this.numEntries.get() >= this.maxEntries && !evictionEnabled) {
      setEvictionEnabled(true);
    } else if (evictionEnabled && this.numEntries.get() < 0.95 * this.maxEntries) {
      setEvictionEnabled(false);
    }
  }

  /**
   * Maximum number of entries (relevant for AQ)
   * @return maximum number of entries
   */
  public long getMaximumSize() {
    return this.maxEntries;
  }
  
  /**
   * Set maximum size of an index (for AQ). 
   * By varying maximum size of an index - we can control overall disk write rate
   * @param max maximum number of entries to keep
   */
  public void setMaximumSize(long max) {
    this.maxEntries = max;
    if (max < size()) {
      setEvictionEnabled(true);
      // blocked operation
      shrinkIndex();
    } else {
      setEvictionEnabled(false);
    }
  }

  /**
   * For AQ memory index - asynchronous operation
   */
  private void shrinkIndex() {
    Runnable r =
        () -> {
          /*1*/long[] index = ref_index_base.get();
          for (int i = 0; i < index.length; i++) {
            boolean result = shrinkIndexSlot(index, i);
            if (!result) {
              // Rehashing is in progress
           /*2*/   index = ref_index_base_rehash.get();
              if (index == null) {
                // Rare race condition possible when index rehashing finishes 
                // between 1 and 2
                // Try main again
                index = ref_index_base.get();
                result = shrinkIndexSlot(index, i);
                // Please enable assertions
                assert result;
              } else {
                shrinkIndexSlot(index, i * 2);
                shrinkIndexSlot(index, i * 2 + 1);
              }
            }
          }
        };
    new Thread(r).start();
  }

  private boolean shrinkIndexSlot(long[] index, int slot) {
    double ratio = ((double) this.maxEntries) / size();
    
    try {
      writeLock(slot);
      long ptr = index[slot];
      if (ptr == 0) {
        return false;
      }
      int num = numEntries(ptr);
      int newNum = (int) Math.floor(num * ratio);
      setNumEntries(ptr, newNum);
      incrDataSize(ptr, (newNum - num) * this.indexSize);
      incrIndexSize(newNum - num);
      index[slot] = shrink(ptr); 
    } finally {
      writeUnlock(slot);
    }
    return true;
  }
  
  /**
   * Set eviction policy for this index
   * @param policy eviction policy
   */
  public void setEvictionPolicy (EvictionPolicy policy) {
    this.evictionPolicy = policy;
  }
  
  /**
   * Get current eviction policy
   * @return eviction policy
   */
  public EvictionPolicy getEvictionPolicy() {
    return this.evictionPolicy;
  }
  
  /**
   * Set eviction enabled. Eviction is enabled when cache reaches max
   * @param b true/false
   */
  public void setEvictionEnabled(boolean b) {
    this.evictionEnabled = b;
  }
  
  /**
   * Is eviction enabled
   * @return true - false
   */
  public boolean isEvictionEnabled() {
    return this.evictionEnabled;
  }

  /**
   * Set type of an index - either AdmissionQueue index or Main Queue
   *
   * @param type
   * @throws IllegalAccessException
   * @throws InstantiationException
   * @throws ClassNotFoundException
   */
  public void setType(Type type) {
    this.indexType = type;
    IndexFormat format;
    try {
      if (type == Type.AQ) { // Admission queue
        setEvictionPolicy(new FIFOEvictionPolicy());
        format = cacheConfig.getAdmissionQueueIndexFormat(cacheName);
      } else { // Main queue
        EvictionPolicy policy = cacheConfig.getCacheEvictionPolicy(this.cacheName);
        setEvictionPolicy(policy);
        format = cacheConfig.getMainQueueIndexFormat(cacheName);
      }
      setIndexFormat(format);      
      this.indexSize = this.indexFormat.indexEntrySize();
      this.indexBlockHeaderSize = this.indexFormat.getIndexBlockHeaderSize();
    } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
      LOG.error(e);
      throw new RuntimeException(e);
    }
  }

  /**
   * Get index type
   * @return index type
   */
  public Type getType() {
    return this.indexType;
  }
  
  
  /** Index initializer */
  private void init() {
    int startNumberOfSlots = 1 << cacheConfig.getStartIndexNumberOfSlotsPower(this.cacheName);
    //TODO: must be positive 
    long[] index_base = new long[startNumberOfSlots];
    int size = BASE_SIZE * BASE_MULTIPLIERS[0];
    for (int i = 0; i < index_base.length; i++) {
      index_base[i] = UnsafeAccess.mallocZeroed(size); // 256 bytes
      // Set block size
      UnsafeAccess.putShort(index_base[i], (short) (size));
      // Number of entries and data size are 0
    }
    ref_index_base.set(index_base);
    initLocks();
  }
  
  private void initLocks() {
    // Initialize locks
    for (int i = 0; i < locks.length; i++) {
      locks[i] = new ReentrantReadWriteLock();
    }
  }
  /**
   * Expand index block
   *
   * @param indexBlockPtr current pointer
   * @return new pointer - can be -1 (check return value)
   */
  long expand(long indexBlockPtr, int requiredSize) {
    int blockSize = blockSize(indexBlockPtr);
    if (blockSize >= requiredSize) return indexBlockPtr;
    int newSize = getMinSizeGreaterOrEqualsThan(requiredSize);
    if (newSize == FAILED) {
      return FAILED;
    }
    long ptr = UnsafeAccess.mallocZeroed(newSize);
    int dataSize = dataSize(indexBlockPtr);
    UnsafeAccess.copy(indexBlockPtr, ptr, dataSize + indexBlockHeaderSize);
    // Update block size
    setBlockSize(ptr, newSize);
    UnsafeAccess.free(indexBlockPtr);
    return ptr;
  }

  /**
   * Shrink index block (after deletion, rarely needed)
   *
   * @param indexBlockPtr current pointer
   * @return new pointer
   */
  long shrink(long indexBlockPtr) {
    int dataSize = dataSize(indexBlockPtr);
    int blockSize = blockSize(indexBlockPtr);
    int newSize = getMinSizeGreaterOrEqualsThan(dataSize + indexBlockHeaderSize);
    if (newSize == blockSize) {
      return indexBlockPtr;
    }
    long ptr = UnsafeAccess.mallocZeroed(newSize);
    UnsafeAccess.copy(indexBlockPtr, ptr, dataSize + indexBlockHeaderSize);
    // Update block size
    setBlockSize(ptr, newSize);
    UnsafeAccess.free(indexBlockPtr);
    return ptr;
  }

  /**
   * Get index data size in bytes
   *
   * @param indexBlockPtr index block pointer
   * @return data size
   */
  final int dataSize(long indexBlockPtr) {
    return UnsafeAccess.toShort(indexBlockPtr + DATA_SIZE_OFFSET);
  }

  /**
   * Sets new index data size
   *
   * @param indexBlockPtr index block pointer
   * @param newSize new size
   */
  final void setDataSize(long indexBlockPtr, int newSize) {
    UnsafeAccess.putShort(indexBlockPtr + DATA_SIZE_OFFSET, (short) newSize);
  }

  /**
   * Increment data size
   *
   * @param indexBlockPtr index block pointer
   * @param incr increment value
   * @return size after increment
   */
  final int incrDataSize(long indexBlockPtr, int incr) {
    int size = dataSize(indexBlockPtr);
    setDataSize(indexBlockPtr, size + incr);
    return size + incr;
  }

  /**
   * Get index block size in bytes
   *
   * @param indexBlockPtr index block pointer
   * @return size
   */
  final int blockSize(long indexBlockPtr) {
    return UnsafeAccess.toShort(indexBlockPtr + BLOCK_SIZE_OFFSET);
  }

  /**
   * Sets new index block size
   *
   * @param indexBlockPtr index block pointer
   * @param newSize new size
   */
  final void setBlockSize(long indexBlockPtr, int newSize) {
    UnsafeAccess.putShort(indexBlockPtr + BLOCK_SIZE_OFFSET, (short) newSize);
  }

  /**
   * Get number of entries in a given index block
   *
   * @param indexBlockPtr index block pointer
   * @return number of entries
   */
  final int numEntries(long indexBlockPtr) {
    return UnsafeAccess.toShort(indexBlockPtr + ENTRIES_OFFSET);
  }

  /**
   * Set number of entries in index block
   *
   * @param indexBlockPtr index block pointer
   * @param num new number of entries
   */
  final void setNumEntries(long indexBlockPtr, int num) {
    UnsafeAccess.putShort(indexBlockPtr + ENTRIES_OFFSET, (short) num);
  }

  /**
   * Get number of entries
   *
   * @param indexBlockPtr
   * @param incr
   * @return
   */
  final int incrNumEntries(long indexBlockPtr, int incr) {
    int num = numEntries(indexBlockPtr);
    setNumEntries(indexBlockPtr, num + incr);
    //TODO: avoid eviction flip/flop
    incrIndexSize(incr);
    return num + incr;
  }
  
  /**
   * Is rehashing in progress
   * @return true or false
   */
  public final boolean isRehashingInProgress() {
    return this.rehashInProgress;
  }
  
  /**
   * Read lock on a key (slot)
   * @param keyPtr key address
   * @param keySize key address
   */
  public int readLock(long keyPtr, int keySize) {
    long[] index = ref_index_base.get();
    long hash = Utils.hash64(keyPtr, keySize);
    int slot = getSlotNumber(hash, index.length);
    ReentrantReadWriteLock lock = locks[slot % locks.length];
    lock.readLock().lock();
    if(index[slot] == 0) {
      // rehash is in progress
      lock.readLock().unlock();
      index = ref_index_base_rehash.get();
      if (index == null) {
        // Get main back - rare race condition
        // when rehashing was completed during this method invocation
        // and now main REF contains new index (2 x times bigger)
        index = ref_index_base.get();
      }
      slot = getSlotNumber(hash, index.length);
      lock = locks[slot % locks.length];
      lock.readLock().lock();
      // NOTES: either we lock correct slot in a main index or a
      // correct slot in rehash index (during rehashing)
      // In both cases we are safe
    }
    return slot;
  }
  
  
  /**
   * Write lock on a key (slot)
   * @param keyPtr key address
   * @param keySize key address
   */
  public int writeLock(long keyPtr, int keySize) {
    long hash = Utils.hash64(keyPtr, keySize);
    long[] index = ref_index_base.get();
    // Always != null - safe
    int slot = getSlotNumber(hash, index.length);
    ReentrantReadWriteLock lock = locks[slot % locks.length];
    lock.writeLock().lock();
    if(index[slot] == 0) {
      // rehash is in progress
      lock.writeLock().unlock();
      index = ref_index_base_rehash.get();
      if (index == null) {
        // Get main back - rare race condition
        // when rehashing was completed during this method invocation
        index = ref_index_base.get();
      }
      slot = getSlotNumber(hash, index.length);
      lock = locks[slot % locks.length];
      lock.writeLock().lock();
      // NOTES: either we lock correct slot in a main index or a
      // correct slot in rehash index (during rehashing)
      // In both cases we are safe
    }
    return slot;
  }
  
  /**
   * Read lock on a key (slot)
   * @param key key buffer
   * @param off offset
   * @param keySize key size
   */
  public int readLock(byte[] key, int off, int keySize) {
    long hash = Utils.hash64(key, off, keySize);
    long[] index = ref_index_base.get();
    int slot = getSlotNumber(hash, index.length);
    ReentrantReadWriteLock lock = locks[slot % locks.length];
    lock.readLock().lock();
    if(index[slot] == 0) {
      // rehash is in progress
      lock.readLock().unlock();
      index = ref_index_base_rehash.get();
      if (index == null) {
        // Get main back - rare race condition
        // when rehashing was completed during this method invocation
        index = ref_index_base.get();
      }
      slot = getSlotNumber(hash, index.length);
      lock = locks[slot % locks.length];
      lock.readLock().lock();
      // NOTES: either we lock correct slot in a main index or a
      // correct slot in rehash index (during rehashing)
      // In both cases we are safe
    }
    return slot;
  }


  /**
   * Write lock on a key
   * @param key key buffer
   * @param off offset
   * @param keySize key size
   */
  public int writeLock(byte[] key, int off, int keySize) {
    long hash = Utils.hash64(key, off, keySize);
    long[] index = ref_index_base.get();
    int slot = getSlotNumber(hash, index.length);
    ReentrantReadWriteLock lock = locks[slot % locks.length];
    lock.writeLock().lock();
    if(index[slot] == 0) {
      // rehash is in progress
      lock.writeLock().unlock();
      index = ref_index_base_rehash.get();
      if (index == null) {
        // Get main back - rare race condition
        // when rehashing was completed during this method invocation
        index = ref_index_base.get();
      }
      slot = getSlotNumber(hash, index.length);
      lock = locks[slot % locks.length];
      lock.writeLock().lock();
      // NOTES: either we lock correct slot in a main index or a
      // correct slot in rehash index (during rehashing)
      // In both cases we are safe
    }
    return slot;
  }
    
  /**
   * Read lock on a key (slot)
   * @param slot slot number
   */
  public void readLock(int slot) {
    ReentrantReadWriteLock lock = locks[slot % locks.length];
    lock.readLock().lock();
  }
  
  /**
   * Read unlock on a key (slot)
   * @param slot slot number
   */
  public void readUnlock(int slot) {
    ReentrantReadWriteLock lock = locks[slot % locks.length];
    lock.readLock().unlock();
  }
  
  /**
   * Write lock on a slot
   * @param slot slot number
   */
  public void writeLock(int slot) {
    ReentrantReadWriteLock lock = locks[slot % locks.length];
    lock.writeLock().lock();
  }
  
  /**
   * Write unlock on a slot
   * @param slot slot number
   */
  public void writeUnlock(int slot) {
    ReentrantReadWriteLock lock = locks[slot % locks.length];
    if (lock.isWriteLockedByCurrentThread()) {
      lock.writeLock().unlock();
    }
  }
  
  /**
   * Find index for a key
   *
   * @param key key array
   * @param off key offset
   * @param size key size
   * @return index size (8 or 12); -1 - not found
   */
  public long find(byte[] key, int off, int size, boolean hit, long buf, int bufSize) {
    int slot = 0;
    try {
      slot = readLock(key, off, size);
      long hash = Utils.hash64(key, off, size);
      return find(hash, hit, buf, bufSize);
    } finally {
      readUnlock(slot);
    }
  }

  /**
   * Get item size (only for MQ)
   * @param key key buffer
   * @param off offset
   * @param size key size
   * @return size of an item, both key and value (-1 - not found)
   */
  public int getItemSize(byte[] key, int off, int size) {
    //TODO: does it work for variable sizes?
    int bufSize = this.indexFormat.indexEntrySize();
    long buf = UnsafeAccess.mallocZeroed(bufSize);
    try {
      long result = find(key, off, size, false, buf, bufSize);
      if (result != bufSize) {
        return -1;
      }
      int itemSize = this.indexFormat.getKeyValueSize(buf);
      return itemSize;
    } finally {
      UnsafeAccess.free(buf);
    }
  }
  
  /**
   * Get item size (only for MQ)
   * @param key key buffer
   * @param off offset
   * @param size key size
   * @return size of an item, both key and value (-1 - not found)
   */
  public int getItemSize(long keyPtr, int keySize) {
    //TODO: does it work for variable sizes?
    int bufSize = this.indexFormat.indexEntrySize();
    long buf = UnsafeAccess.mallocZeroed(bufSize);
    try {
      long result = find(keyPtr, keySize, false, buf, bufSize);
      if (result != bufSize) {
        return -1;
      }
      int itemSize = this.indexFormat.getKeyValueSize(buf);
      return itemSize;
    } finally {
      UnsafeAccess.free(buf);
    }
  }

  /**
   * Get item's hit count (only for MQ)
   *
   * @param key key buffer
   * @param off offset
   * @param size key size
   * @return hit count (or -1)
   */
  public int getHitCount(byte[] key, int off, int size) {
    //TODO: double locking
    int bufSize = this.indexFormat.indexEntrySize();
    long buf = UnsafeAccess.mallocZeroed(bufSize);
    try {
      long result = find(key, off, size, false, buf, bufSize);
      if (result != bufSize) {
        return -1;
      }
      // Check expiration
      int count = this.indexFormat.getHitCount(buf);
      return count;
    } finally {
      UnsafeAccess.free(buf);
    }
  }

  /**
   * Get item's hit count (only for MQ)
   *
   * @param key key buffer
   * @param off offset
   * @param size key size
   * @return hit count (or -1)
   */
  public int getHitCount(long keyPtr, int keySize) {
    int bufSize = this.indexFormat.indexEntrySize();
    long buf = UnsafeAccess.mallocZeroed(bufSize);
    try {
      
      long result = find(keyPtr, keySize, false, buf, bufSize);
      if (result != bufSize) {
        return -1;
      }
      int count = this.indexFormat.getHitCount(buf);
      return count;
    } finally {
      UnsafeAccess.free(buf);
    }
  }

  /**
   * Delete entry from a given index block at a given address
   * @param ptr index block address
   * @param $ptr entry address
   * @param count - index of an entry at this index block 0-based
   */
  private void deleteAt(long ptr, long $ptr, int count) {
    // TODO: Move this to a separate method
    int dataSize = dataSize(ptr);
    int numEntries = numEntries(ptr);
    // delete entry
    int toDelete = this.indexFormat.fullEntrySize($ptr);
    int toMove = (int) ((ptr + indexBlockHeaderSize + dataSize) - $ptr - toDelete);
    UnsafeAccess.copy($ptr + toDelete, $ptr, toMove);
    incrDataSize(ptr, -toDelete);
    incrNumEntries(ptr, -1);
    // Update stats
    //TODO: separate method
    int numSegments = this.cacheConfig.getNumberOfRanks(this.cacheName);  
    int rank = this.evictionPolicy.getRankForIndex(numSegments, count, numEntries);
    int sid1 = getSegmentIdForEntry(ptr, count);
    // Update stats
    if (this.cache != null) {
      cache.getEngine().updateStats(sid1, -1, -(numSegments - rank));
    }
  }
  /**
   * Finds entry in index and promote if hit == true
   *
   * @param ptr address of index block
   * @param hash has of a key
   * @param hit promote if true
   * @param buf address to copy index part to
   * @param bufSize buffer size
   * @return found index size or -1
   */
  //TODO: check return value
  private int findAndPromote(long ptr, long hash, boolean hit, long buf, int bufSize) {
    int numEntries = numEntries(ptr);
    //TODO: this works ONLY when index size = item size (no embedded data)
    
    long $ptr = ptr + this.indexBlockHeaderSize;
    int count = 0;
    int indexSize = NOT_FOUND; // not found
    while (count < numEntries) {
      if (this.indexFormat.equals($ptr, hash)) {
        indexSize = this.indexFormat.fullEntrySize($ptr);
        if (indexSize > bufSize) {
          return indexSize;
        }
        // Check if expired
        long expire = this.indexFormat.getExpire(ptr, $ptr);
        if (expire > 0) {
          long current = System.currentTimeMillis();
          if (current > expire) {
            deleteAt(ptr, $ptr, count);
            //TODO Update segment stats for expired item
            //TOD:move this code into deleteAt method
            int sid0 = this.indexFormat.getSegmentId($ptr);
            int numSegments = this.cacheConfig.getNumberOfRanks(this.cacheName); 
            int rank = this.evictionPolicy.getRankForIndex(numSegments, count, numEntries);
            if (this.cache != null) {
              cache.getEngine().updateStats(sid0, -1, -rank);
            }
            return NOT_FOUND;
          }
        }
        // Update hits
        if (hit) {
          this.indexFormat.hit($ptr);
          int sid0 = this.indexFormat.getSegmentId($ptr);
          //TODO: this works only if we promote item by +1 rank
          // Update stats
          if (this.cache != null) {
            this.cache.getEngine().updateStats(sid0, 0, 1);
          }
          int numSegments = this.cacheConfig.getNumberOfRanks(this.cacheName); 
          int rank = this.evictionPolicy.getRankForIndex(numSegments, count, numEntries);
          int idx = this.evictionPolicy.getStartIndexForRank(numSegments, rank + 1, numEntries);
          int sid1 = getSegmentIdForEntry(ptr, idx);
          // Update stats
          if (this.cache != null) {
            cache.getEngine().updateStats(sid1, 0, -1);
          }
        }
        // Save item size and item location to a buffer
        UnsafeAccess.copy($ptr, buf, indexSize);
        
        if (hit && count > 0) {
          // ask parent where to move
          int idx = this.evictionPolicy.getPromotionIndex(ptr, count, numEntries);
          int off = offsetFor(ptr, idx); 
          int offc = offsetFor($ptr, count);
          int toMove = offc - off;
          
          // Move data between 'idx' (inclusive) and 'count' (exclusive)(count > idx must be)
          UnsafeAccess.copy(ptr + off, ptr + off + indexSize, toMove);
          // insert index into new place
          UnsafeAccess.copy(buf, ptr + off, indexSize);
        }
  
        break;
      }
      count++;
      $ptr += this.indexFormat.fullEntrySize($ptr);
    }
    return indexSize;
  }
  
  final int getSegmentIdForEntry(long ptr, int entryNumber) {
    long $ptr = ptr + this.indexBlockHeaderSize;
    int count = 0;
    while (count < entryNumber) {
      $ptr += this.indexFormat.fullEntrySize($ptr);
      count++;
    }
    return this.indexFormat.getSegmentId($ptr);
  }
  
  final int offsetFor(long ptr, int idx) {
    //TODO: can be optimized
    long $ptr = ptr + this.indexBlockHeaderSize;
    int count = 0;
    while (count++ < idx) {
      $ptr += this.indexFormat.fullEntrySize($ptr);
    }
    return (int) ($ptr - ptr);
  }
  
  /**
   * Finds entry in index and deletes if hit == true
   * This is used by AQ (admission queue)
   * @param ptr address of index block
   * @param hash has of a key
   * @param delete delete if true
   * @return found index size or NOT_FOUND
   */
  private int findAndDelete(long ptr, long hash, boolean delete, long buf, int bufSize) {
    int numEntries = numEntries(ptr);
    long $ptr = ptr + this.indexBlockHeaderSize;
    int count = 0;
    int indexSize; // not found
    while (count < numEntries) {
      indexSize = this.indexFormat.fullEntrySize($ptr);
      if (this.indexFormat.equals($ptr, hash)) {
        if (delete) {
          int dataSize = dataSize(ptr);
          int toMove =(int) (ptr + dataSize + this.indexBlockHeaderSize - $ptr - indexSize);
          // Move
          UnsafeAccess.copy($ptr + indexSize, $ptr, toMove); 
          incrNumEntries(ptr, -1);
          incrDataSize(ptr, -indexSize);
        }
        // For testing mostly
        UnsafeAccess.putLong(buf, hash);
        return indexSize;
      }
      count++;
      $ptr += indexSize;
    }
    return NOT_FOUND;
  }
  
  private int getSlotNumber(long hash, int indexSize) {
    int level = Integer.numberOfTrailingZeros(indexSize);
    int $slot = (int) hash >>> (64 - level);
    return $slot;
  }
  
  /**
   * Find index for a key
   *
   * @param ptr key address
   * @param size key size
   * @param hit - perform promotion if true
   * @return index size; -1 - not found
   */
  public long find(long ptr, int size, boolean hit, long buf, int bufSize) {
    int slot = 0;
    try {
      slot = readLock(ptr, size);
      long hash = Utils.hash64(ptr, size);
      return find(hash, hit, buf, bufSize);
    } finally {
      readUnlock(slot);
    }
  }

  /**
   * Find index for a key's hash and copy its value to a buffer
   *
   * @param hash key's hash
   * @param hit if true - promote item on hit
   * @return found index size or -1 (not found)
   */
  private long find(long hash, boolean hit, long buf, int bufSize) {
    // This  method is called under lock
    // Get slot number
    long[] index = ref_index_base.get();
    int $slot = getSlotNumber(hash, index.length);
    long ptr = 0;

    ptr = index[$slot];
    if (ptr == 0) {
      // Rehashing is in progress
      index = ref_index_base_rehash.get();
      if (index == null) {
        // Rehashing finished - race condition
        index = ref_index_base.get();
      }
      $slot = getSlotNumber(hash, index.length);
      ptr = index[$slot];
    }
    return findInternal(ptr, hash, hit, buf, bufSize);
  }

  private int findInternal(long ptr, long hash, boolean hit, long buf, int bufSize) {
    return this.indexType == Type.AQ? findAndDelete(ptr, hash, hit, buf, bufSize) : 
        findAndPromote(ptr, hash, hit, buf, bufSize);
  }
  /**
   * Delete key from index
   *
   * @param keyPtr key address
   * @param keySize key size
   * @return true on success, false - otherwise
   */
  public boolean delete(long keyPtr, int keySize) {
    int slot = 0;
    try {
      slot = writeLock(keyPtr, keySize);
      long hash = Utils.hash64(keyPtr, keySize);
      return delete(hash);
    } finally {
      writeUnlock(slot);
    }
  }

  /**
   * Delete key from index
   *
   * @param key key buffer
   * @param keyOffset - key offset
   * @param keySize key size
   * @return true on success, false - otherwise
   */
  public boolean delete(byte[] key, int keyOffset, int keySize) {
    int slot = 0;
    try {
      slot = writeLock(key, keyOffset, keySize);
      long hash = Utils.hash64(key, keyOffset, keySize);
      return delete(hash);
    } finally {
      writeUnlock(slot);
    }
  }

  /**
   * Delete index for a key's hash
   *
   * @param hash key's hash
   * @return true on success, false - otherwise
   */
  private boolean delete(long hash) {
    return delete(hash, true);
  }

  /**
   * Delete index for a key's hash
   *
   * @param hash key's hash
   * @return true on success, false - otherwise
   */
  private boolean delete(long hash, boolean shrink) {
    // Get slot number
    long[] index = ref_index_base.get();
    int $slot = getSlotNumber(hash, index.length);
    long ptr = index[$slot];
    if (ptr == 0) {
      // Rehashing is in progress
      index = ref_index_base_rehash.get();
      if (index == null) {
        // Rehashing finished in the middle of this operation
        index = ref_index_base.get(); 
      }
      $slot = getSlotNumber(hash, index.length);
      ptr = index[$slot];
    }
    boolean result = delete(ptr, hash);
    if (shrink && result) {
      long nptr = shrink(ptr);
      if (nptr != ptr) {
        index[$slot] = nptr;
      }
    }
    return result;
  }
  
  
  /**
   * Delete key with a given hash from a index block
   *
   * @param ptr index block address
   * @param hash key's hash
   * @return true or false
   */
  private boolean delete(long ptr, long hash) {
    int numEntries = numEntries(ptr);
    long $ptr = ptr + indexBlockHeaderSize;
    int count = 0;
    while (count < numEntries) {
      if (this.indexFormat.equals($ptr, hash)) {
        deleteAt(ptr, $ptr, count);
        return true;
      }
      count++;
      $ptr += this.indexFormat.fullEntrySize($ptr);
    }
    return false;
  }
  
  /**
   * Internal API: used by Scavenger Get key's popularity for a given key
   *
   * @param key key buffer
   * @param off offset
   * @param keySize key size
   * @return number between 1.0 and 0.0 (0.0 - means key is absent), 1.0 - key is at the top of a
   *     index block.
   */
  public final double popularity(byte[] key, int off, int keySize) {
    int slot = 0;
    try {
      slot = readLock(key, off, keySize);
      long hash = Utils.hash64(key, off, keySize);
      return popularity(hash);
    } finally {
      readUnlock(slot);
    }
  }

  /**
   * Internal API: used by Scavenger Get key's popularity for a given key's hash
   *
   * @param hash key's hash
   * @return number between 1.0 and 0.0 (0.0 - means key is absent), 1.0 - key is at the top of a
   *     index block.
   */
  private double popularity(long hash) {
    // Get slot number
    long[] index = ref_index_base.get();
    int $slot = getSlotNumber(hash, index.length);
    long ptr = index[$slot];
    if (ptr == 0) {
      // Rehashing is in progress
      index = ref_index_base_rehash.get();
      if (index == null) {
        // Rehashing finished in the middle
        index = ref_index_base.get();
      }
      $slot = getSlotNumber(hash, index.length);
      ptr = index[$slot];
    }
    return blockPopularity(ptr, hash);
  }

  /**
   * Get popularity of key by a given hash in a given index block
   *
   * @param ptr address of index block
   * @param hash hash of a key
   * @return number between 1.0 and 0.0 (0.0 - means key is absent), 1.0 - key is at the top of a
   *     index block.
   */
  private double blockPopularity(long ptr, long hash) {
    int numEntries = numEntries(ptr);
    long $ptr = ptr + indexBlockHeaderSize;
    int count = 0;
    double pop = 0.;
    while (count < numEntries) {
      if (this.indexFormat.equals($ptr, hash)) {
        pop = ((double) (numEntries - count)) / numEntries;
        break;
      }
      count++;
      $ptr += this.indexFormat.fullEntrySize($ptr);
    }
    return pop;
  }

  /**
   * Insert new index entry
   *
   * @param key item key
   * @param keyOff item's key offset
   * @param keySize item key size
   * @param indexPtr item index data pointer (relevant only for MQ)
   * @param indexSize index size
   * @return INSERTED or FAILED
   */
  public MutationResult insert(byte[] key, int keyOff, int keySize, long indexPtr, int indexSize) {
    int slot = 0;
    try {
      slot = writeLock(key, keyOff, keySize);
      long hash = Utils.hash64(key, keyOff, keySize);
      MutationResult result = insertInternal(hash, indexPtr, indexSize);
      return result;
    } finally {
      writeUnlock(slot);
    }
  }

  /**
   * Insert new index entry with a rank
   *
   * @param key item key
   * @param keyOff item's key offset
   * @param keySize item key size
   * @param indexPtr item index data pointer (relevant only for MQ)
   * @param indexSize index size
   * @param rank item's rank
   * @return INSERTED or FAILED
   */
  public MutationResult insertWithRank(byte[] key, int keyOff, int keySize, long indexPtr, int indexSize, int rank) {
    int slot = 0;
    try {
      slot = writeLock(key, keyOff, keySize);
      long hash = Utils.hash64(key, keyOff, keySize);
      return insertInternal(hash, indexPtr, indexSize, rank);
    } finally {
      writeUnlock(slot);
    }
  }
  
  /**
   * Insert new index entry
   *
   * @param key item key
   * @param indexPtr item index data pointer
   */
  public MutationResult insert(byte[] key, long indexPtr, int indexSize) {
    return insert(key, 0, key.length, indexPtr, indexSize);
  }

  /**
   * Insert new index entry with a rank
   *
   * @param key item key
   * @param indexPtr item index data pointer
   * @param indexSize index size
   * @param rank rank of an item (1 - max, 8 - min)
   * @return INSERTED or FAILED
   */
  public MutationResult insertWithRank(byte[] key, long indexPtr, int indexSize, int rank) {
    return insertWithRank(key, 0, key.length, indexPtr, indexSize, rank);
  }

  /**
   * Insert new index entry
   *
   * @param ptr key address
   * @param size key size
   * @param index data pointer (not used for AQ, for MQ - its 12 bytes value)
   * @param indexSize index size
   * @return INSERTED or FAILED
   */
  public MutationResult insert(long ptr, int size, long indexPtr, int indexSize) {
    int slot = 0;
    try {
      slot = writeLock(ptr, size);
      long hash = Utils.hash64(ptr, size);
      MutationResult result = insertInternal(hash, indexPtr, indexSize);
      return result;
    } finally {
      writeUnlock(slot);
    }
  }

  /**
   * Insert new index entry with a rank
   *
   * @param ptr key address
   * @param size key size
   * @param index data pointer (not used for AQ, for MQ - its 12 bytes value)
   * @param indexSize index size
   * @param rank item's rank
   * @param mutation result: INSERTED or FAILED
   */
  public MutationResult insertWithRank(long ptr, int size, long indexPtr, int indexSize, int rank) {
    int slot = 0;
    // get hashed key value
    try {
      slot = writeLock(ptr, size);
      long hash = Utils.hash64(ptr, size);
      return insertInternal(hash, indexPtr, indexSize, rank);
    } finally {
      writeUnlock(slot);
    }
  }
  
  /**
   * Insert hash - value into index
   *
   * @param hash hash
   * @param indexPtr value
   * @param indexSize index size
   * @return MutationResult.INSERTED 

   */
  private MutationResult insertInternal(long hash, long indexPtr, int indexSize) {
    int rank = this.cacheConfig.getSLRUInsertionPoint(this.cacheName);
    return insertInternal(hash, indexPtr, indexSize, rank);
  }

  /**
   * Selects correct index table for a hashed key
   * Can be either ref_index_base or ref_index_base_rehash
   * @param hash hashed key 
   * @return index
   */
  private long[] getIndexForHash(long hash) {
    long[] index = ref_index_base.get();
    int $slot = getSlotNumber(hash, index.length);
    long ptr = index[$slot];
    if (ptr == 0) {
      // Rehashing is in progress
      index = ref_index_base_rehash.get();
      // Check one more time (there are possible race conditions)
      if (index == null) {
        // Get back to the main
        index = ref_index_base.get();
      } 
    }
    return index;
  }
  
  /**
   * Insert hash - value into index
   *
   * @param hash hash
   * @param indexPtr value
   * @param rank item's rank
   * @return MutationResult.INSERTED 
   */
  private MutationResult insertInternal(long hash, long indexPtr, int indexSize, int rank) {
    // Get slot number
    // Get slot number
    long[] index = getIndexForHash(hash);
    int $slot = getSlotNumber(hash, index.length);
    long ptr = index[$slot];
    long $ptr = insert0(ptr, hash, indexPtr, indexSize, rank);
    
    if ($ptr != ptr && $ptr > 0) {
      // Possible block expansion or rehash (0)
      // update index segment address
      index[$slot] = $ptr;
    } 
    return MutationResult.INSERTED;
  }
  

  /**
   * Insert hash - value into a given index block
   *
   * @param ptr index block address
   * @param hash hash of a key
   * @param indexPtr index data pointer (not used for AQ, 12 bytes for MQ)
   * @return new index block pointer
   * @throws  
   */
  private long insert0(long ptr, long hash, long indexPtr, int indexSize) {
    int rank = this.cacheConfig.getSLRUInsertionPoint(this.cacheName);
    return insert0(ptr, hash, indexPtr, indexSize, rank);
  }

 
  /**
   * Insert hash - value into a given index block with a rank
   *
   * @param ptr index block address
   * @param hash hash of a key
   * @param indexPtr index data pointer (not used for AQ, 12 bytes for MQ)
   * @param rank item's rank
   * @return new index block pointer
   * @throws  
   */
  private long insert0(long ptr, long hash, long indexPtr, int indexSize, int rank) {
    if (isEvictionEnabled()) {
      doEviction(ptr); // TODO: take into account size of a new item
    }
    // If indexPtr == 0, then insert hash only (for AQ)
    boolean isAQ = indexPtr == 0;
    int blockSize = blockSize(ptr);
    int dataSize = dataSize(ptr);
    int requiredSize = dataSize + this.indexBlockHeaderSize + (isAQ? Utils.SIZEOF_LONG: indexSize); 
    long retPtr = ptr;
    if (requiredSize > blockSize) {
      long $ptr = expand(ptr, requiredSize);
      if ($ptr > 0) {
        ptr = $ptr;
        retPtr = ptr;
      } else {
        // rehash slot
        int $slot = getSlotNumber(hash, ref_index_base.get().length);
        long pptr = ref_index_base.get()[$slot];
        
        // This is done under write lock for the slot
        rehashSlot($slot);
        long rehashed = rehashedSlots.incrementAndGet();

        $slot = getSlotNumber(hash, ref_index_base_rehash.get().length);
        retPtr = 0; // for rehash we return 0;
        ptr = ref_index_base_rehash.get()[$slot];
        
        blockSize = blockSize(ptr);
        requiredSize = dataSize(ptr) + this.indexBlockHeaderSize + (isAQ? Utils.SIZEOF_LONG: indexSize); 
        if (blockSize < requiredSize) {
          // Check on requiredSize again 
          //TODO: optimize in shrink - we do shrink followed by expand 
          $ptr = expand(ptr, requiredSize);
          ptr = $ptr;
          ref_index_base_rehash.get()[$slot] = ptr;
        }
        if (rehashed == ref_index_base.get().length) {
          // Rehash is complete
          ref_index_base.set(ref_index_base_rehash.get());
          //TODO: Do we really need to set this to NULL?
          ref_index_base_rehash.set(null);
          rehashedSlots.set(0);
          this.rehashInProgress = false;
        }
      }
    }
    insertEntry(ptr, hash, indexPtr, indexSize, rank);
    return retPtr;
  }
  
  /**
   * TODO: eviction by size
   * Perform eviction
   * @param slotPtr index-data-block address
   * @throws IOException 
   */
  private void doEviction(long slotPtr) { 
    int toEvict = -1;
    boolean evictToVictim  = true;
    
    if (this.indexFormat.isExpirationSupported()){
      toEvict = findExpired(slotPtr);
    }
    
    evictToVictim = toEvict < 0;

    if (toEvict == -1) {
      int numEntries = numEntries(slotPtr);
      toEvict = evictionPolicy.getEvictionCandidateIndex(slotPtr, numEntries);
    }
    // report eviction
    if (this.evictionListener != null) {
      long ptr = slotPtr + offsetFor(slotPtr, toEvict);
      int size = this.indexFormat.fullEntrySize(ptr);
      this.evictionListener.onEviction(ptr, size);
    }
    deleteEntry(slotPtr, toEvict, evictToVictim);
  }
  
  private int findExpired(long slotPtr) {
    int toEvict = -1;
    int numEntries = numEntries(slotPtr);
    int count = 0;
    long ptr = slotPtr + this.indexBlockHeaderSize;
    while(count < numEntries) {
      long time = this.indexFormat.getExpire(slotPtr, ptr);
      if (time > 0 && System.currentTimeMillis() > time) {
        toEvict = count;
        break;
      }
      ptr += this.indexFormat.fullEntrySize(ptr);
      count ++;
    }
    return toEvict;
  }

  /**
   * Delete entry by index (used only for eviction)
   * @param ptr index segment address 
   * @param num index to delete
   * @throws IOException 
   */
  private void deleteEntry(long ptr, int num, boolean evictToVictim) {
    int numEntries = numEntries(ptr);
    int off = offsetFor(ptr, num);
    long $ptr = ptr + off;
    int toDelete = this.indexFormat.fullEntrySize($ptr);
    
    // TODO: send evicted item to a victim cache
    if (evictToVictim) {
      evictToVictimCache(ptr, $ptr);
    }
    
    if (num != numEntries - 1) {
      int toMove = dataSize(ptr) + this.indexBlockHeaderSize - off;
      UnsafeAccess.copy($ptr + toDelete, $ptr, toMove);
    }
    incrDataSize(ptr, -toDelete);
    incrNumEntries(ptr, -1);
    // decrease total number of index entries
    //TODO: this works only if we promote item by +1 rank
    int numSegments = this.cacheConfig.getNumberOfRanks(this.cacheName);
    int rank = this.evictionPolicy.getRankForIndex(numSegments, num, numEntries);
    int sid1 = getSegmentIdForEntry(ptr, num);
    // Update stats
    if (this.cache != null) {
      cache.getEngine().updateStats(sid1, -1, -(numSegments - rank));
    }
  }

  private void evictToVictimCache(long ptr, long $ptr) {
    Cache victim = this.cache != null?this.cache.getVictimCache(): null;
    if (victim == null) {
      return;
    }

    int size = this.indexFormat.fullEntrySize($ptr);
    long expire = this.indexFormat.getExpire(ptr, $ptr);
    try {
      // Check embedded mode
      if (this.cacheConfig.isIndexDataEmbeddedSupported()) {
        if (size <= this.cacheConfig.getIndexDataEmbeddedSize()) {
          int off = this.indexFormat.getEmbeddedOffset();
          $ptr += off;
          int kSize = Utils.readUVInt($ptr);
          int kSizeSize = Utils.sizeUVInt(kSize);
          $ptr += kSizeSize;
          int vSize = Utils.readUVInt($ptr);
          int vSizeSize = Utils.sizeUVInt(vSize);
          $ptr += vSizeSize;
          int rank = this.cacheConfig.getSLRUInsertionPoint(victim.getName());

          victim.put($ptr, kSize, $ptr + kSize, vSize, expire, rank, true);
          return;
        }
      }
      // else - not embedded
      // transfer item to victim cache
      this.cache.transfer(ptr, $ptr);

    } catch (IOException e) {
      LOG.error(e);
    }
  }

  /**
   * Insert hash - value entry with a rank
   * TODO: insert by rank
   * @param ptr index block address
   * @param hash hash
   * @param indexPtr index data pointer (not relevant for AQ, for MQ - 12 byte data)
   */
  private void insertEntry(long ptr, long hash, long indexPtr, int indexSize, int rank) {
    // Check if it exists already - update
    delete(ptr, hash);
    int numEntries = numEntries(ptr);
    int numRanks = this.cacheConfig.getNumberOfRanks(this.cacheName);
    int insertIndex = indexType == Type.MQ? evictionPolicy.getStartIndexForRank(numRanks, rank, numEntries):
      evictionPolicy.getInsertIndex(ptr, numEntries);
    // TODO: entry size can be variable
    int off = offsetFor(ptr, insertIndex);
    int toMove = dataSize(ptr) + this.indexBlockHeaderSize - off;
    int itemSize = this.indexType == Type.AQ? Utils.SIZEOF_LONG: indexSize;
    UnsafeAccess.copy(ptr + off, ptr + off + itemSize, toMove);
    // Insert new entry
    // Update number of elements
    incrNumEntries(ptr, 1);
    // Update used size
    incrDataSize(ptr, itemSize);
    // increment total index size
    if (this.indexType == Type.AQ) {
      UnsafeAccess.putLong(ptr + off, hash);
      // return for AQ
      return;
    } else {
      UnsafeAccess.copy(indexPtr, ptr + off, indexSize);
    }

    //TODO: make it a separate method
    //Update stats
    //TODO: is rank 1- based?
    
    int sid1 = this.indexFormat.getSegmentId(indexPtr);
    
    updateStats(ptr, sid1, rank, numEntries);
  
  }


  private void updateStats(long ptr, int sid1, int rank, int numEntries) {
    if (this.cache == null) {
      return;
    }
    int numSegments = this.cacheConfig.getNumberOfRanks(this.cacheName);
    // Update stats
    this.cache.getEngine().updateStats(sid1, 1, (numSegments - rank));
    if (numEntries >= numSegments) {
      int prev = -1;
      for (int r = rank + 1; r < numSegments; r++) {
        int idx = this.evictionPolicy.getStartIndexForRank(numSegments, r, numEntries);
        if (idx == prev) continue;
        prev = idx;
        int sid = getSegmentIdForEntry(ptr, idx);
          // decrement by total data segment rank by 1 
          this.cache.getEngine().updateStats(sid, 0, -1);
      }
    }
  }

  private void rehashSlot(int slot) {
    // We keep write lock on parent slot - so we are safe to
    // work with rehash index
    // confirm rehashing
    this.rehashInProgress = true;
    long ptr = ref_index_base.get()[slot];
    int numEntries = numEntries(ptr);
    // TODO: again variable sized indexes
    int blockSize = getMaximumBlockSize();
    int indexSize = ref_index_base.get().length;
    long[] rehash_index = ref_index_base_rehash.get();
    if (rehash_index == null || rehash_index.length == indexSize) {
      long[] $rehash_index = new long[2 * indexSize];
      ref_index_base_rehash.compareAndSet(rehash_index, $rehash_index);
      rehash_index = ref_index_base_rehash.get();
    }

    int level = Integer.numberOfTrailingZeros(indexSize);
    // get two slots in a new index
    int slot0 = slot << 1;
    int slot1 = slot0 + 1;

    long ptr0 = UnsafeAccess.mallocZeroed(blockSize);
    setBlockSize(ptr0, blockSize);

    long ptr1 = UnsafeAccess.mallocZeroed(blockSize);
    setBlockSize(ptr1, blockSize);

    // set slots pointers
    rehash_index[slot0] = ptr0;
    rehash_index[slot1] = ptr1;

    int numSlot0 = 0, numSlot1 = 0;
    int dataSize0 = 0, dataSize1 = 0;
    int count = 0;
    long $ptr = ptr + indexBlockHeaderSize;
    // increment level
    level++;

    while (count < numEntries) {

      // This is the hack
      long h = this.indexFormat.getHash($ptr);
      int $slot = (int) h >>> (64 - level);
      // int off = 0, size = 0;
      int size = this.indexFormat.fullEntrySize($ptr);

      if ($slot == slot0) {
        // Copy to data to slot0 in new index
        // off = offsetFor(ptr, count);
        UnsafeAccess.copy($ptr, ptr0 + indexBlockHeaderSize + dataSize0, size);
        numSlot0++;
        dataSize0 += size;
      } else if ($slot == slot1) {
        // Copy to data to slot0 in new index
        UnsafeAccess.copy($ptr, ptr1 + indexBlockHeaderSize + dataSize1, size);
        numSlot1++;
        dataSize1 += size;
      }
      $ptr += size;
      count++;
    }
    // Update slot0 and slot1 num entries
    setNumEntries(ptr0, numSlot0);
    setNumEntries(ptr1, numSlot1);
    // Update slot0 and slot1 data size
    setDataSize(ptr0, dataSize0);
    setDataSize(ptr1, dataSize1);

    // Now we can shrink
    rehash_index[slot0] = shrink(ptr0);
    rehash_index[slot1] = shrink(ptr1);
    // Free previous index block
    // It is safe, because this index block is under write lock
    
    UnsafeAccess.free(ptr);
    ref_index_base.get()[slot] = 0;
    
  }

  /*
   * This method is not thread safe 
   */
  private void completeRehashing() {
    if (isRehashingInProgress() == false) return;
    long[] index = ref_index_base.get();
    for (int i = 0; i < index.length; i++) {
      if (index[i] == 0) continue;
      rehashSlot(i);
    }
    
    // Finalize
    this.ref_index_base.set(this.ref_index_base_rehash.get());
    this.ref_index_base_rehash.set(null);
    this.rehashedSlots.set(0);
    this.rehashInProgress = false;
  }
  
  /**
   * Add-if Absent-Remove-if Present Atomic operation
   *
   * @param key key
   * @param off offset
   * @param len key length
   * @return FAILED, INSERTED or DELETED
   */
  public MutationResult aarp(byte[] key, int off, int len) {
    
    int slot = 0;
    try {
      slot = writeLock(key, off, len);
      // get hashed key value
      long hash = Utils.hash64(key, off, len);
      MutationResult result = aarp(hash);
      return result;
    } finally {
      writeUnlock(slot);
    }
  }

  /**
   * Add-if Absent-Remove-if Present 
   * Atomic operation
   * @param key key
   * @return FAILED, INSERTED or DELETED 
   */
  public MutationResult aarp(long keyPtr, int keySize) {
    int slot = 0;
    try {
      slot = writeLock(keyPtr, keySize);
      // get hashed key value
      long hash = Utils.hash64(keyPtr, keySize);
      MutationResult result = aarp(hash);
      return result;
    } finally {
      writeUnlock(slot);
    }
  }

  /**
   * Add-if Absent-Remove-if Present Atomic operation - implementation
   *
   * @param hash hashed key (8 bytes)
   * @return true - if was added, false - if was deleted
   */

  public MutationResult aarp(long hash) {
    // Get slot number
    long[] index = ref_index_base.get();
    int $slot = getSlotNumber(hash, index.length);
    long ptr = index[$slot];

    if (ptr == 0) {
      // Rehash is in progress
      index = ref_index_base_rehash.get();
      if (index == null) {
        // Rehashing finished - race condition
        index = ref_index_base.get();
      }
      $slot = getSlotNumber(hash, index.length);
      ptr = index[$slot];
    }
    // try to delete first
    boolean result = delete(hash);
    if (result) {
      return MutationResult.DELETED; // Deleted
    }
    long $ptr = insert0(ptr, hash, 0L /* not used for AQ*/, 0);
    if ($ptr != ptr && $ptr > 0) {
      // Possible block expansion or rehash (0)
      // update index segment address
      index[$slot] = $ptr;
    } 
    return MutationResult.INSERTED;
  }

  @Override
  public void save(OutputStream os) throws IOException {

    DataOutputStream dos = Utils.toDataOutputStream(os);
    completeRehashing();
    // TODO: locking index?
    // Cache name
    dos.writeUTF(cacheName);
    /* Type */
    dos.writeInt(this.indexType.ordinal());
    /* Index format */
    indexFormat.save(dos);
    /* Hash table size */
    dos.writeLong(this.ref_index_base.get().length);
    /* Index entry size */
    dos.writeInt(this.indexSize);
    /* Is eviction enabled yet?*/
    dos.writeBoolean(this.evictionEnabled);
    /* Total number of index entries */
    dos.writeLong(numEntries.get());
    /* Maximum number of entries - for AQ*/
    dos.writeLong(this.maxEntries);

    long[] table = this.ref_index_base.get();
    byte[] buffer = new byte[getMaximumBlockSize()];
    for (int i = 0; i < table.length; i++) {
      long ptr = table[i];
      int size = blockSize(ptr);
      UnsafeAccess.copy(ptr, buffer, 0, size);
      dos.writeInt(size);
      dos.write(buffer, 0, size);
    }
    dos.flush();
  }

  @Override
  public void load(InputStream is) throws IOException {
    DataInputStream dis = Utils.toDataInputStream(is);
 // Read index type
    this.cacheName = dis.readUTF();
    int ord = dis.readInt();
    Type type = Type.values()[ord];
    setType(type);
    // Load index format
    indexFormat.load(dis);
    // Read table size
    int tableSize = (int) dis.readLong();
    long[] table = new long[tableSize];
    // Entry size
    this.indexSize = dis.readInt();
    // Eviction enabled
    this.evictionEnabled = dis.readBoolean();
    // Number of entries
    this.numEntries.set(dis.readLong());
    // Maximum number of entries
    this.maxEntries = dis.readLong();
    byte[] buffer = new byte[getMaximumBlockSize()];
    for (int i = 0; i < tableSize; i++) {
      // index segment size
      int len = dis.readInt();
      dis.readFully(buffer, 0, len);
      long ptr = UnsafeAccess.malloc(len);
      UnsafeAccess.copy(buffer, 0, ptr, len);
      table[i] = ptr;
    }
    this.ref_index_base.set(table);
  }
}
