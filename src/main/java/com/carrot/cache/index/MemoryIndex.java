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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;

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
  @SuppressWarnings("unused")
  private static final Logger LOG = LogManager.getLogger(MemoryIndex.class);
  
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
  
  /* Response code for insert - full rehashing is required */
  private static final int FULL_REHASH_REQUEST = -10;
  
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
  
  /* Main .lock is used for blocked index rehashing */
  private ReentrantReadWriteLock mainLock;
  
  /* Index type */
  private Type indexType;
  
  /* Index entry size */
  private int indexSize;
  
  // index block size (2) followed by number of entries (2) followed by data size (2) 
  private int indexBlockHeaderSize = 3 * Utils.SIZEOF_SHORT;

  /* Is eviction enabled yet?*/
  private volatile boolean evictionEnabled = false;
  
  /* Total number of index entries */
  private AtomicLong numEntries = new AtomicLong(0);
  
  /* Maximum number of entries - for AQ*/
  private volatile long maxEntries = 0;
  
  /* Parent cache  */
  private Cache cache;
  
  /* Index format */
  private IndexFormat indexFormat;
  
  /* Is rehashing in progress */
  private volatile boolean rehashInProgress;
  
  public MemoryIndex() {
  }
  
  /**
   * Constructor
   *
   * @param type index type
   */
  public MemoryIndex(Cache cache, Type type) {
    this.cache = cache;
    this.cacheConfig = CacheConfig.getInstance();
    init();
    setType(type);
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
   * Shrink index size (AQ only)
   * Blocked operation.
   */
  private void shrinkIndex() {
    try {
      //TODO: avoid main locking
      mainWriteLock();
      double ratio = ((double) this.maxEntries) / size(); 
      long[] index = ref_index_base.get();
      for (int i = 0; i < index.length; i++) {
        long ptr = index[i];
        int num = numEntries(ptr);
        int newNum = (int) Math.floor(num * ratio);
        setNumEntries(ptr, newNum);
        incrDataSize(ptr,  (newNum - num) * this.indexSize);
        incrIndexSize(newNum - num);
        //TODO: shrink index segment
        index[i] = shrink(ptr);
      }
    } finally {
      mainWriteUnlock();
    }
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
   * @param type
   */
  public void setType(Type type) {
    this.indexType = type;
    if (type == Type.AQ) { // Admission queue
      setEvictionPolicy(new FIFOEvictionPolicy());
      setIndexFormat(new AQIndexFormat());
    } else { // Main queue
      EvictionPolicy policy = cacheConfig.getCacheEvictionPolicy(this.cache.getName());
      setEvictionPolicy(policy);
      setIndexFormat(new MQIndexFormat());
    }
    this.indexSize = this.indexFormat.indexEntrySize();
    this.indexBlockHeaderSize = this.indexFormat.getIndexBlockHeaderSize();
  }
  
  /**
   * Get index type
   * @return index type
   */
  public Type getType() {
    return this.indexType;
  }
  
  /**
   * Read lock index - main
   */
  private void mainReadLock() {
    this.mainLock.readLock().lock();
  }
  
  /**
   * Read unlock index - main
   */
  private void mainReadUnlock() {
    this.mainLock.readLock().unlock();
  }
  
  /**
   * Write lock index - used during index rehashing
   */
  private void mainWriteLock() {
    this.mainLock.writeLock().lock();
  }
  
  /**
   * Write unlock index - used after rehashing is complete
   */
  private void mainWriteUnlock() {
    WriteLock lock = this.mainLock.writeLock();
    if (lock.isHeldByCurrentThread()) {
      lock.unlock();
    }
  }
  
  /** Index initializer */
  private void init() {
    int startNumberOfSlots = 1 << cacheConfig.getStartIndexNumberOfSlotsPower(this.cache.getName());
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
  public void readLock(long keyPtr, int keySize) {
    mainReadLock();
    long hash = Utils.hash8(keyPtr, keySize);
    int slot = getSlotNumber(hash, ref_index_base.get().length);
    ReentrantReadWriteLock lock = locks[slot % locks.length];
    lock.readLock().lock();
  }
  
  /**
   * Read unlock on a key (slot)
   * @param keyPtr key address
   * @param keySize key size
   */
  public void readUnlock(long keyPtr, int keySize) {
    mainReadUnlock();
    long hash = Utils.hash8(keyPtr, keySize);
    int slot = getSlotNumber(hash, ref_index_base.get().length);
    ReentrantReadWriteLock lock = locks[slot % locks.length];
    lock.readLock().unlock();
  }
  
  /**
   * Write lock on a key (slot)
   * @param keyPtr key address
   * @param keySize key address
   */
  public void writeLock(long keyPtr, int keySize) {
    mainReadLock();
    long hash = Utils.hash8(keyPtr, keySize);
    int slot = getSlotNumber(hash, ref_index_base.get().length);
    ReentrantReadWriteLock lock = locks[slot % locks.length];
    lock.writeLock().lock();
  }
  
  /**
   * Write unlock on a key (slot)
   * @param keyPtr key address
   * @param keySize key size
   */
  public void writeUnlock(long keyPtr, int keySize) {
    mainReadUnlock();
    long hash = Utils.hash8(keyPtr, keySize);
    int slot = getSlotNumber(hash, ref_index_base.get().length);
    ReentrantReadWriteLock lock = locks[slot % locks.length];
    lock.writeLock().unlock();
  }
  
  /**
   * Read lock on a key (slot)
   * @param key key buffer
   * @param off offset
   * @param keySize key size
   */
  public void readLock(byte[] key, int off, int keySize) {
    mainReadLock();
    long hash = Utils.hash8(key, off, keySize);
    int slot = getSlotNumber(hash, ref_index_base.get().length);
    ReentrantReadWriteLock lock = locks[slot % locks.length];
    lock.readLock().lock();
  }
  
  /**
   * Read unlock on a key (slot)
   * @param key key buffer
   * @param off offset
   * @param keySize key size
   */
  public void readUnlock(byte[] key, int off, int keySize) {
    mainReadUnlock();
    long hash = Utils.hash8(key, off, keySize);
    int slot = getSlotNumber(hash, ref_index_base.get().length);
    ReentrantReadWriteLock lock = locks[slot % locks.length];
    lock.readLock().unlock();
  }
  
  /**
   * Write lock on a key
   * @param key key buffer
   * @param off offset
   * @param keySize key size
   */
  public void writeLock(byte[] key, int off, int keySize) {
    mainReadLock();
    long hash = Utils.hash8(key, off, keySize);
    int slot = getSlotNumber(hash, ref_index_base.get().length);
    ReentrantReadWriteLock lock = locks[slot % locks.length];
    lock.writeLock().lock();
  }
  
  /**
   * Write lock on a key
   * @param key key buffer
   * @param off offset
   * @param keySize key size
   */
  public void writeUnlock(byte[] key, int off, int keySize) {
    mainReadUnlock();
    long hash = Utils.hash8(key, off, keySize);
    int slot = getSlotNumber(hash, ref_index_base.get().length);
    ReentrantReadWriteLock lock = locks[slot % locks.length];
    lock.writeLock().unlock();
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
    lock.writeLock().unlock();
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
    long hash = Utils.hash8(key, off, size);
    return find(hash, hit, buf, bufSize);
  }

  /**
   * Get item size (only for MQ)
   * @param key key buffer
   * @param off offset
   * @param size key size
   * @return size of an item, both key and value (-1 - not found)
   */
  public int getItemSize(byte[] key, int off, int size) {
    int bufSize = this.indexFormat.indexEntrySize();
    long buf = UnsafeAccess.mallocZeroed(bufSize);
    try {
      
      // No write lock is required for pure reads
      readLock(key, off, size);
      
      long result = find(key, off, size, false, buf, bufSize);
      if (result != bufSize) {
        return -1;
      }
      int itemSize = this.indexFormat.getKeyValueSize(buf);
      return itemSize;
    } finally {
      UnsafeAccess.free(buf);
      readUnlock(key, off, size);
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
    int bufSize = this.indexFormat.indexEntrySize();
    long buf = UnsafeAccess.mallocZeroed(bufSize);
    try {
      // No write locks is required for pure reads
      readLock(keyPtr, keySize);
      
      long result = find(keyPtr, keySize, false, buf, bufSize);
      if (result != bufSize) {
        return -1;
      }
      int itemSize = this.indexFormat.getKeyValueSize(buf);
      return itemSize;
    } finally {
      UnsafeAccess.free(buf);
      readUnlock(keyPtr, keySize);
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
    int bufSize = this.indexFormat.indexEntrySize();
    long buf = UnsafeAccess.mallocZeroed(bufSize);
    try {
      // No write lock is required for pure reads
      readLock(key, off, size);
      long result = find(key, off, size, false, buf, bufSize);
      if (result != bufSize) {
        return -1;
      }
      int count = this.indexFormat.getHitCount(buf);
      return count;
    } finally {
      UnsafeAccess.free(buf);
      readUnlock(key, off, size);
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
      
      // No write lock is required for pure reads
      readLock(keyPtr, keySize);
      
      long result = find(keyPtr, keySize, false, buf, bufSize);
      if (result != bufSize) {
        return -1;
      }
      int count = this.indexFormat.getHitCount(buf);
      return count;
    } finally {
      UnsafeAccess.free(buf);
      readUnlock(keyPtr, keySize);
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
        // Update hits
        if (hit) {
          this.indexFormat.hit($ptr);
          int sid0 = this.indexFormat.getSegmentId($ptr);
          //TODO: this works only if we promote item by +1 rank
          // Update stats
          cache.getEngine().updateStats(sid0, 0, 1);
          int numSegments = cache.getEngine().getNumberOfSegments();
          int rank = this.evictionPolicy.getSegmentForIndex(numSegments, count, numEntries);
          int idx = this.evictionPolicy.getStartIndexForSegment(numSegments, rank + 1, numEntries);
          int sid1 = getSegmentIdForEntry(ptr, idx);
          // Update stats
          cache.getEngine().updateStats(sid1, 0, -1);
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
  private int findAndDelete(long ptr, long hash, boolean delete) {
    int numEntries = numEntries(ptr);
    long $ptr = ptr + this.indexBlockHeaderSize;
    int count = 0;
    int indexSize = NOT_FOUND; // not found
    while (count < numEntries) {
      if (this.indexFormat.equals($ptr, hash)) {
        indexSize = this.indexFormat.fullEntrySize($ptr);
        if (delete) {
          int toMove =(int) (ptr + dataSize($ptr) + this.indexBlockHeaderSize - $ptr - indexSize);
          // Move
          UnsafeAccess.copy($ptr + indexSize, $ptr, toMove);          
        }
        break;
      }
      count++;
      $ptr += indexSize;
    }
    return indexSize;
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
    long hash = Utils.hash8(ptr, size);
    return find(hash, hit, buf, bufSize);
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
      return NOT_FOUND;
    }
    return findInternal(ptr, hash, hit, buf, bufSize);
  }

  private int findInternal(long ptr, long hash, boolean hit, long buf, int bufSize) {
    return this.indexType == Type.AQ? findAndDelete(ptr, hash, hit) : 
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
    if (isRehashingInProgress()) {
      // No delete operations during rehashing
      return false;
    }
    try {
      writeLock(keyPtr, keySize);
      long hash = Utils.hash8(keyPtr, keySize);
      return delete(hash);
    } finally {
      writeUnlock(keyPtr, keySize);
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
    if (isRehashingInProgress()) {
      // No delete operations during rehashing
      return false;
    }
    try {
      writeLock(key, keyOffset, keySize);
      long hash = Utils.hash8(key, keyOffset, keySize);
      return delete(hash);
    } finally {
      writeUnlock(key, keyOffset, keySize);
    }
  }

  /**
   * Delete index for a key's hash
   *
   * @param hash key's hash
   * @return true on success, false - otherwise
   */
  private boolean delete(long hash) {
    // Get slot number
    long[] index = ref_index_base.get();
    int $slot = getSlotNumber(hash, index.length);
    long ptr = index[$slot];
    if (ptr == 0) {
      return false; // not found
    }
    boolean result = delete(ptr, hash);
    if (result) {
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
      // First 8 bytes is hashed key value
      if (this.indexFormat.equals($ptr, hash)) {
        int toDelete = this.indexFormat.fullEntrySize($ptr);
        int dataSize = dataSize(ptr);
        int toMove = (int) ((ptr + indexBlockHeaderSize + dataSize) - $ptr - toDelete);
        UnsafeAccess.copy($ptr + toDelete, $ptr, toMove);
        incrDataSize(ptr, -toDelete);
        incrNumEntries(ptr, -1);
        // Update stats
        //TODO: separate method
        int numSegments = cache.getEngine().getNumberOfSegments();
        int rank = this.evictionPolicy.getSegmentForIndex(numSegments, count, numEntries);
        int sid1 = getSegmentIdForEntry(ptr, count);
        // Update stats
        cache.getEngine().updateStats(sid1, -1, -(numSegments - rank));
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
    try {
      readLock(key, off, keySize);
      long hash = Utils.hash8(key, off, keySize);
      return popularity(hash);
    } finally {
      readUnlock(key, off, keySize);
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
      return 0.0; // no key
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
   */
  public void insert(byte[] key, int keyOff, int keySize, long indexPtr, int indexSize) {
    if (isRehashingInProgress()) {
      // No insert operations during rehashing
      return;
    }
    try {
      writeLock(key, keyOff, keySize);
      long hash = Utils.hash8(key, keyOff, keySize);
      insertInternal(hash, indexPtr, indexSize);
    } finally {
      writeUnlock(key, keyOff, keySize);
    }
  }

  /**
   * Insert new index entry
   *
   * @param key item key
   * @param indexPtr item index data pointer
   */
  public void insert(byte[] key, long indexPtr, int indexSize) {
    if (isRehashingInProgress()) {
      // No insert operations during rehashing
      return;
    }
    try {
      writeLock(key, 0, key.length);
      long hash = Utils.hash8(key, 0, key.length);
      insertInternal(hash, indexPtr, indexSize);
    } finally {
      writeUnlock(key, 0, key.length);
    }
  }

  /**
   * Insert new index entry
   *
   * @param ptr key address
   * @param size key size
   * @param index data pointer (not used for AQ, for MQ - its 12 bytes value)
   */
  public void insert(long ptr, int size, long indexPtr, int indexSize) {
    if (isRehashingInProgress()) {
      // No insert operations during rehashing
      return;
    }
    // get hashed key value
    try {
      writeLock(ptr, size);
      long hash = Utils.hash8(ptr, size);
      insertInternal(hash, indexPtr, indexSize);
    } finally {
      writeUnlock(ptr, size);
    }
  }

  /**
   * Insert hash - value into index
   *
   * @param hash hash
   * @param indexPtr value
   */
  private void insertInternal(long hash, long indexPtr, int indexSize) {
    // Get slot number
    long[] index = ref_index_base.get();
    int $slot = getSlotNumber(hash, index.length);
    long ptr = 0;
    ptr = index[$slot];
    if (ptr == 0) {
      ptr = UnsafeAccess.mallocZeroed(getMinimumBlockSize());
      index[$slot] = ptr;
    }
    long $ptr = insert0(ptr, hash, indexPtr, indexSize);
    
    if ($ptr != ptr && $ptr > 0) {
      // Possible block expansion or rehash (0)
      // update index segment address
      index[$slot] = $ptr;
    } else if ($ptr == FULL_REHASH_REQUEST) {
      // full rehash is required
      // writeUnlock($slot);
      rehashAll();
      // repeat call
      insertInternal(hash, indexPtr, indexSize);
    } 
  }

  /**
   * Blocked index rehashing
   */
  private void rehashAll() {
    // Check if rehashing is in progress
    if (isRehashingInProgress()) {
      return;
    }
    try {
      long[] index = ref_index_base.get();
      mainWriteLock(); 
      if(isRehashingInProgress()) {
        return;
      }
      if (index != ref_index_base.get()) {
        // some other thread completed rehashing
        return;
      }
      this.rehashInProgress = true;
      mainWriteUnlock();
      // We do rehashing without main locking
      // to allow reads
      for (int i = 0; i < index.length; i++) {
        rehashSlot(i);
      }
      // Lock again b/c we need guarantee that what? 
      mainWriteLock();
      ref_index_base.set(ref_index_base_rehash.get());
      ref_index_base_rehash.set(null);
    } finally {
      mainWriteUnlock(); // write unlock index
      this.rehashInProgress = false;
    }
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
    if (isEvictionEnabled()) {
      doEviction(ptr); // TODO: take into account size of a new item
    }
    // If indexPtr == 0, then insert hash only (for AQ)
    boolean isAQ = indexPtr == 0;
    int blockSize = blockSize(ptr);
    int dataSize = dataSize(ptr);
    int requiredSize = dataSize + this.indexBlockHeaderSize + (isAQ? Utils.SIZEOF_LONG: indexSize); 
    long $ptr = 0;
    if (requiredSize > blockSize) {
      // Try to expand
      $ptr = expand(ptr, requiredSize);
      if ($ptr < 0) {
        return FULL_REHASH_REQUEST;// request full rehash
      }
      ptr = $ptr;
    }
    insertEntry(ptr, hash, indexPtr, indexSize);
    return ptr;
  }

  /**
   * TODO: eviction by size
   * Perform eviction
   * @param slotPtr index-data-block address
   * @throws IOException 
   */
  private void doEviction(long slotPtr) { 
    int numEntries = numEntries(slotPtr);
    int toEvict = evictionPolicy.getEvictionCandidateIndex(slotPtr, numEntries);
    // report eviction
    if (this.evictionListener != null) {
      long ptr = slotPtr + offsetFor(slotPtr, toEvict);
      int size = this.indexFormat.fullEntrySize(ptr);
      this.evictionListener.onEviction(ptr, size);
    }
    deleteEntry(slotPtr, toEvict);
  }
  
  /**
   * Delete entry by index (used only for eviction)
   * @param ptr index segment address 
   * @param num index to delete
   * @throws IOException 
   */
  private void deleteEntry(long ptr, int num) {
    int numEntries = numEntries(ptr);
    int off = offsetFor(ptr, num);
    long $ptr = ptr + off;
    int toDelete = this.indexFormat.fullEntrySize($ptr);
    
    // TODO: send evicted item to a victim cache
    evictToVictimCache(ptr, $ptr);
    
    if (num != numEntries - 1) {
      int toMove = dataSize(ptr) + this.indexBlockHeaderSize - off;
      UnsafeAccess.copy($ptr + toDelete, $ptr, toMove);
    }
    incrDataSize(ptr, -toDelete);
    incrNumEntries(ptr, -1);
    //TODO: this works only if we promote item by +1 rank
    int numSegments = cache.getEngine().getNumberOfSegments();
    int rank = this.evictionPolicy.getSegmentForIndex(numSegments, num, numEntries);
    int sid1 = getSegmentIdForEntry(ptr, num);
    // Update stats
    cache.getEngine().updateStats(sid1, -1, -(numSegments - rank));
  }

  private void evictToVictimCache(long ptr, long $ptr) {
    Cache victim = cache.getVictimCache();
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
      cache.transfer(ptr, $ptr);

    } catch (IOException e) {
      LOG.error(e);
    }
  }

  /**
   * Insert hash - value entry
   *
   * @param ptr index block address
   * @param hash hash
   * @param indexPtr index data pointer (not relevant for AQ, for MQ - 12 byte data)
   */
  private void insertEntry(long ptr, long hash, long indexPtr, int indexSize) {
    int numEntries = numEntries(ptr);
    int insertIndex = evictionPolicy.getInsertIndex(ptr, numEntries);
    // TODO: entry size can be variable
    int off = offsetFor(ptr, insertIndex);
    int toMove = dataSize(ptr) + this.indexBlockHeaderSize - off;
    int itemSize = this.indexType == Type.AQ? Utils.SIZEOF_LONG: indexSize;
    UnsafeAccess.copy(ptr + off, ptr + off + itemSize, toMove);
    // Insert new entry
    
    if (this.indexType == Type.AQ) {
    UnsafeAccess.putLong(ptr + off, hash);
    } else {
      UnsafeAccess.copy(indexPtr, ptr + off, indexSize);
    }
    // Update number of elements
    setNumEntries(ptr, numEntries + 1);
    // Update used size
    incrDataSize(ptr, indexSize);
    //TODO: make it a separate method
    //Update stats
    int numSegments = cache.getEngine().getNumberOfSegments();
    int rank = this.evictionPolicy.getSegmentForIndex(numSegments, insertIndex, numEntries);
    int sid1 = getSegmentIdForEntry(ptr, insertIndex);
    // Update stats
    cache.getEngine().updateStats(sid1, 1, (numSegments - rank));
    for (int r = rank + 1; r < numSegments; r++) {
      int idx = this.evictionPolicy.getStartIndexForSegment(numSegments, r, numEntries);
      int sid = getSegmentIdForEntry(ptr, idx);
      cache.getEngine().updateStats(sid, 0, -(numSegments - rank));

    }
  }

 
  private void rehashSlot(int slot) {
    // We keep write lock on parent slot - so we are safe to
    // work with rehash index
    // We keep global write lock in case of a blocked rehashing
    try {
      
      readLock(slot);
      long ptr = ref_index_base.get()[slot];
      int numEntries = numEntries(ptr);
      // TODO: again variable sized indexes
      int blockSize = getMaximumBlockSize();
      int indexSize = ref_index_base.get().length;
      long[] rehash_index = ref_index_base_rehash.get();
      if (rehash_index == null) {
        rehash_index = new long[2 * indexSize];
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
    } finally {
      readUnlock(slot);
    }
  }
  
  
  /**
   * Add-if Absent-Remove-if Present 
   * Atomic operation
   * @param key key
   * @param off offset
   * @param len key length
   * @return true - if was added, false - if was deleted
   */
  public boolean aarp(byte[] key, int off, int len) {
    try {  
      writeLock(key, off, len);
      // get hashed key value
      long hash = Utils.hash8(key, off, len);
      return aarp(hash);
    } finally {
      writeUnlock(key, off, len);
    }
  }
  
  /**
   * Add-if Absent-Remove-if Present 
   * Atomic operation
   * @param key key
   * @return true - if was added, false - if was deleted
   */
  public boolean aarp(long keyPtr, int keySize) {
    try {
      writeLock(keyPtr, keySize);
      // get hashed key value
      long hash = Utils.hash8(keyPtr, keySize);
      return aarp(hash);
    } finally {
      writeUnlock(keyPtr, keySize);
    }
  }

  /**
   * Add-if Absent-Remove-if Present Atomic operation - implementation
   *
   * @param hash hashed key (8 bytes)
   * @return true - if was added, false - if was deleted
   */
  public boolean aarp(long hash) {
    // Get slot number
    long[] index = ref_index_base.get();
    int $slot = getSlotNumber(hash, index.length);
    long ptr = index[$slot];

    if (ptr == 0) {
      ptr = UnsafeAccess.mallocZeroed(getMinimumBlockSize());
      index[$slot] = ptr;
    }
    // try to delete first
    boolean result = delete(hash);
    if (result) {
      return false; // Deleted
    }
    long $ptr = insert0(ptr, hash, 0L /* not used for AQ*/, 0);
    if ($ptr != ptr && $ptr > 0) {
      // Possible block expansion or rehash (0)
      // update index segment address
      index[$slot] = $ptr;
    } else if ($ptr == FULL_REHASH_REQUEST) {
      // full rehash is required
      // writeUnlock($slot);
      $slot = -1;
      rehashAll();
      // repeat call - can lead to potential issues
      // if two threads in parallel will try to add item
      // but in our case - this should not be the problem
      return aarp(hash);
    }
    return true;
  }

  @Override
  public void save(OutputStream os) throws IOException {
    
    DataOutputStream dos = Utils.toDataOutputStream(os); 
    try {
      // TODO: locking index?
      mainReadLock();
      /* Index format */
      indexFormat.save(dos);
      /* Type */
      dos.writeInt(this.indexType.ordinal());
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
    } finally {
      mainReadUnlock();
    }
  }

  @Override
  public void load(InputStream is) throws IOException {
    DataInputStream dis = Utils.toDataInputStream(is);
    // Load index format
    indexFormat.load(dis);
    // Read index type
    int ord = dis.readInt();
    Type type = Type.values()[ord];
    setType(type);
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
