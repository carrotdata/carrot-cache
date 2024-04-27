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
package com.onecache.core.util;

import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLongArray;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.LongAdder;

/**
 *
 * Concurrent, lock-free hash table with open addressing and liner probing
 * TODO: timed CAS and wait to avoid potential deadlock in a very small maps  
 */
public class Long2LongHashMap {
  
  static class RehashGroup {
    AtomicLongArray mainData;
    AtomicLongArray rehashData;
    AtomicInteger rehashReadOffset = new AtomicInteger();;
    volatile int rehashConfirmedOffset = 0;
  }
  
  public static interface Deallocator {
    public void deallocate(long v);
  }
  
  private static final long RANDOM_LONG =
      ThreadLocalRandom.current().nextLong();
  /*
   * Limitation value can not be equals to ERROR
   * but in our use case values are memory pointers
   * and they can not be negative.
   */
  public final static long ERROR = Long.MIN_VALUE;
  
  public final static long NULL = 0;
  
  private final static long DELETED = -1;
  
  private final static long LOCKED = -2;
  
  /**
   * Orphaned resource deallocator
   */
  private Deallocator deallocator;
  
  /*
   *  Hash slot array (main)
   */
  private AtomicReference<AtomicLongArray> dataRef = 
        new AtomicReference<AtomicLongArray>();
  
  /*
   * Rehashing is In Progress
   */
  //private volatile boolean rip;
  
  /*
   *  4 k-v pairs ( 64 bytes)
   */
  private volatile int rehashChunkSize = 4;
  
  /*
   * Rehash check mask (reverse probability)
   */
  private int rcmask = 0xff;
  
  /**
   * Rehashing group
   */
  private AtomicReference<RehashGroup> rehashGroup = 
      new AtomicReference<RehashGroup>();
  /**
   * If true - do not hash keys
   */
  private final boolean keysHashed;
  
  /**
   * Keeps track of alive objects
   */
  private LongAdder alive = new LongAdder();
  
  /**
   *  Keeps track of delete tombstones
   */
  private LongAdder tombstones = new LongAdder();
   
  /**
   * Used as an exclusive object during rehashing start
   */
  private AtomicReference<Thread> rehashStarter = new AtomicReference<Thread>();
  /**
   * Constructor
   * @param initialCapacity initial number of k-v pairs
   */
  public Long2LongHashMap(int initialCapacity) {
    this(initialCapacity, false);
  }
  
  public volatile boolean trace = false;
  /**
   * Constructor
   * @param initialCapacity initial number of k-v 
   * @param keyHashed do hash keys
   */
  public Long2LongHashMap(int initialCapacity, boolean keyHashed) {
    long size = Utils.nextPow2(2L * initialCapacity);
    if (size >= Integer.MAX_VALUE) {
      throw new IllegalArgumentException("Requested initial capacity after adjusting to the next power of 2 value is too big");
    }
 
    this.dataRef.set(new AtomicLongArray((int) size));
    this.keysHashed = keyHashed;
    updateRehashCheckMask();
  }
  
  private void updateRehashCheckMask() {
    int n = dataRef.get().length() / 2;
    if (n < (1 << 16)) {
      rcmask = 0xf;
    } else {
      rcmask = 0xffff;
    }
  }
  
  /**
   *  Get total number of alive objects (k-v pairs) 
   * @return number of alive objects
   */
  public long size() {
    return alive.longValue();
  }
  
  /**
   * Get total number of delete tombstones 
   * @return number of delete tombstones
   */
  public long totalTombstoneObjects() {
    return tombstones.longValue();
  }
  
  /**
   * Size of a map (capacity)
   * @return
   */
  public long capacity() {
    return dataRef.get().length();
  }
  
  /**
   * Is map re-hashing is in progress
   * @return true or false
   */
  public boolean isRehashingInProgress() {
    return this.rehashGroup.get() != null;
  }
  
  /**
   * Set resource deallocator
   * @param deallocator deallocator
   */
  public void setDeallocator(Deallocator d) {
    this.deallocator = d;
  }
  /**
   * Get deallocator
   * @return deallocator
   */
  public Deallocator getDeallocator() {
    return this.deallocator;
  }
  
  /**
   * Put key - value
   * @param key key
   * @param value value
   * @return previous value or NULL
   */
  public final long put(long key, long value) {
    RehashGroup rg = rehashGroup.get();
    if (rg != null) {
      nextRehashingRun();
      // When rehash is in progress we always put
      // into rehash storage only. If k-v was added from
      // main storage - it will be overwritten, if was not added yet
      // the add will fail in a future, Delete operation first deletes
      // from main and only after that from rehash
      // This is racy operation: it can return NULL during rehashing
      // b/c it does not do put into main storage, where previous value
      // for the key can be stored
      // There should be some STRICT mode for the class to avoid
      // such race condition
      // If returned value is not NULL it can be safely deallocated
      // by caller. The system will deallocate "orphaned" resources
      // during data migration from main to rehash table. There will
      // be no resource leakage as long as Deallocator instance is
      // provided

      // Even if rehashing is completed by now, it is safe to use
      // rg.rehashData b/c it is main data storage now
      AtomicLongArray data = rg.rehashData;
      if (data != null) {
        return put(data, key, value);
      }
    } else {
      maybeStartRehashing();
    }
    return put(dataRef.get(), key, value);
  }
  
  /**
   * Put key - value for a given storage
   * 
   * @param data storage
   * @param key key
   * @param value value
   * @return previous value for a key or NULL
   */
  private long put(AtomicLongArray data, long key, long value) {
   
    if (key == NULL || key == DELETED || key == LOCKED) {
      key = RANDOM_LONG - key;
    }
    final long hash = keysHashed? key: Utils.squirrel3(key);
    final int mask = (int) (data.length() - 1) & 0xfffffffe;
    int index = (int) (hash & mask);
    long oldKey = NULL; // missing key
    long oldValue = NULL;
    
    /*
     * We start our probing run with the index='index'
     * We have to lock it before starting search for a 
     * cell for the key='key', locking initial index guarantees that
     * no other read/write/delete operation with the same key will be possible in 
     * parallel with this one, thus preventing race condition with duplicate keys
     * Locking start index serializes read/write/delete operations for the key
     * 
     */
    long startIndexKey;
    final int startIndex = index;
    while(true) {    
      while ((startIndexKey = data.get(startIndex)) == LOCKED); 
      if (data.compareAndSet(startIndex,  startIndexKey, LOCKED)) {
        break;
      }
    }
    oldKey = startIndexKey;

    if (oldKey == DELETED || oldKey == NULL) {
      data.set(index + 1, value);
      // Unlock index with new key
      data.set(index,  key);
      // Update counters
      alive.increment();
      if (oldKey == DELETED) {
        tombstones.decrement();
      }
      return oldValue;
    } else if (oldKey == key) {
      data.set(index + 1, value);
      // Unlock index with new key
      data.set(index,  key);
      // No counters update
      return oldValue;
    }
    // Advance index because
    // startIndex is locked
    index = (startIndex + 2) & mask;
    try {
      outer:
      while (true) {      
        while ((oldKey = data.get(index)) != NULL) {
          if (key == oldKey || oldKey == DELETED) {
            // We break if found the same key or deleted or empty
            break;
          } else if (oldKey == LOCKED) {
            // We can not pass over LOCKED
            // Two options: busy wait until unlocked or 
            // start again from the beginning
            while ((oldKey = data.get(index)) == LOCKED);
            if (oldKey == key || 
                  oldKey == DELETED || 
                    oldKey == NULL) {
              break;
            }
          }
          // Linear probing
          index = (int) ((index + 2) & mask);
        }
        // Here oldKey == key OR NULL OR DELETED
        if (oldKey == key) {
          // Loop until compareAndSet succeed (index could be LOCKED)
          // When unlocked oldKey will be restored, so eventually
          // this operation will succeed 
          while(!data.compareAndSet(index, oldKey, key));
          oldValue = data.getAndSet(index + 1, value);
          break;
        }
        // Now we have oldKey either NULL or DELETED
        // But it could be locked at this point of execution
        // If it was locked by 'put' we have to move on,
        // but if it was locked by 'delete' we have to wait
        // until its unlocked, because 'delete' will restore 
        // previous key, which is either NULL or DELETED
        long waitKey;
        while (!data.compareAndSet(index, oldKey, key)) {
          waitKey = data.get(index);
          if (waitKey != NULL && waitKey != DELETED && waitKey != LOCKED) {
            // It was locked by other thread PUT operation - move on
            index = (int) ((index + 2) & mask);
            continue outer;
          }
        }
        oldValue = data.getAndSet(index + 1, value);
        break;
      }
      // Update counters
      if (oldKey != key) {
        alive.increment();
      }
      if (oldKey == DELETED) {
        tombstones.decrement();
      }
      return oldValue;
    } finally {
      // Now unlock start index 
      data.set(startIndex, startIndexKey);
    }
  }
  
  /**
   * Put if absent (used internally during map rehashing)
   * @param data data array storage
   * @param key key
   * @param value value
   * @return true on success, false otherwise
   */
  private boolean add(AtomicLongArray data, long key, long value) {
      
    if (key == NULL || key == DELETED || key == LOCKED) {
      key = RANDOM_LONG - key;
    }
    final long hash = keysHashed? key: Utils.squirrel3(key);
    final int mask = (int) (data.length() - 1) & 0xfffffffe;
    int index = (int) (hash & mask);
    long oldKey = NULL; // missing key
    
    /*
     * We start our probing run with the index='index'
     * We have to lock it before starting search for a 
     * cell for the key='key', locking initial index guarantees that
     * no other read/write/delete operation with the same key will be possible in 
     * parallel with this one, thus preventing race condition with duplicate keys
     * Locking start index serializes read/write/delete operations for the key
     * 
     */
    long startIndexKey;
    final int startIndex = index;
    while(true) {    
      while ((startIndexKey = data.get(startIndex)) == LOCKED); 
      if (data.compareAndSet(startIndex,  startIndexKey,  LOCKED)) {
        break;
      }
    }
    oldKey = startIndexKey;

    if (oldKey == DELETED || oldKey == NULL) {
      data.set(index + 1, value);
      // Unlock index with new key
      data.set(index, key);
      if (oldKey == DELETED) {
        tombstones.decrement();
      }
      return true;
    } else if (oldKey == key) {
      // Key exists - unlock and return
      data.set(index, key);
      return false;
    }
    // Advance index because
    // startIndex is locked
    index = (startIndex + 2) & mask;
    try {
      outer:
      while (true) {      
        while ((oldKey = data.get(index)) != NULL) {
          if (key == oldKey || oldKey == DELETED) {
            // We break if found the same key or deleted or empty
            break;
          } else if (oldKey == LOCKED) {
            // We can not pass over LOCKED
            // Two options: busy wait until unlocked or 
            // start again from the beginning
            while ((oldKey = data.get(index)) == LOCKED);
            if (oldKey == key || 
                  oldKey == DELETED || 
                    oldKey == NULL) {
              break;
            }
          }
          // Linear probing
          index = (int) ((index + 2) & mask);
        }
        // Here oldKey == key OR NULL OR DELETED
        if (oldKey == key) {
         // Key exists
         return false;
        }
        // Now we have oldKey either NULL or DELETED
        // But it could be locked at this point of execution
        // If it was locked by 'put' we have to move on,
        // but if it was locked by 'delete' we have to wait
        // until its unlocked, because 'delete' will restore 
        // previous key, which is either NULL or DELETED
        long waitKey = 0;
        while (!data.compareAndSet(index, oldKey, key)) {
          waitKey = data.get(index);
          if (waitKey != NULL && waitKey != DELETED && waitKey != LOCKED) {
            // It was locked by other thread PUT operation - move on
            index = (int) ((index + 2) & mask);
            continue outer;
          }
        }
        if (waitKey == DELETED) {
          tombstones.decrement();
        }
        data.set(index + 1, value);
        break;
      }
      return true;
    } finally {
      // Now unlock start index 
      data.set(startIndex, startIndexKey);
    }
  }

  /**
   * Get value by key
   * @param key key
   * @return value or NULL
   */
  public final long get(long key) {
    RehashGroup rg = rehashGroup.get();
    if (rg != null) {
      nextRehashingRun();
      AtomicLongArray data = rg.rehashData;
      if (data != null) {
        long value = get(data, key);
        if (value != NULL) {
          return value;
        }
      }
    }
    // Read from main data if rip == false, rehahRef.get() returns NULL or
    // rehashRef data returns NULL
    return get(dataRef.get(), key);
  }
  
  /**
   * Get value by key
   * @param key key
   * @return value or NULL
   */
  private long get(AtomicLongArray data, long key) {
    if (key == NULL || key == DELETED || key == LOCKED) {
      key = RANDOM_LONG - key;
    }
    final long hash = keysHashed ? key : Utils.squirrel3(key);
    final int mask = (int) (data.length() - 1) & 0xfffffffe;
    int index = (int) (hash & mask);
    long startIndexKey = data.get(index);
    final int startIndex = index;
    /*
     * Serialize operations for the given key = 'key' Make sure that during search no mutations for
     * the same key can run in parallel - we lock start index
     */
    while (true) {
      while ((startIndexKey = data.get(startIndex)) == LOCKED);
      if (data.compareAndSet(startIndex, startIndexKey, LOCKED)) {
        break;
      }
    }
    try {
      // Check indexKey first
      if (startIndexKey == NULL) {
        return NULL; // Not found
      } else if (startIndexKey == key) {
        return data.get(startIndex + 1);
      }
      long found = NULL; // missing key
      // Advance index
      index = (startIndex + 2) & mask;
      while ((found = data.get(index)) != NULL) {
        if (key == found) {
          break;
        } else if (found == LOCKED) {
          // Our probing run can not include LOCKED cells
          // because we do not know the key of this cell
          // therefore we either start from beginning one more time
          // or busy wait on this cell - the latter one
          while ((found = data.get(index)) == LOCKED);
          if (found == key) {
            break;
          }
        }
        // We skip DELETED - not end our search
        // linear probing
        index = (int) ((index + 2) & mask);
      }
      // Here found value either correct value for the key if found == key
      // or NULL
      if (found == key) {
        found = data.get(index + 1);
      }
      return found;
    } finally {
      // Unlock key
      data.set(startIndex, startIndexKey);
    }
  }

  /**
   * Delete value by key
   * @param key key
   * @return deleted value or NULL
   */
  public final long delete(long key) {
    // Result of this operation - key will be deleted in both
    // storages. The only race condition if key exists
    // in both storages: main and rehash, the return value can be
    // any of two of them. This can happens when two threads: T1 and T2
    // delete the same key at the same time (whatever it means :) -
    // one can return v1 (from main) other one - v2 from rehash store.
    // To avoid resource leakage we perform the following algorithm:
    // If both storages returns non-null values we deallocate
    // one from 'main' storage (because we return value from rehash storage).
    // The method's returned value must be deallocated by a caller.
    // To avoid this race condition we must do locking of a key
    // but this is the race condition we can live with.

    RehashGroup rg = rehashGroup.get();
    if (rg != null) {
      nextRehashingRun();
      // if rip == false - we got correct value
      // if rip == true, possibly stale 
      long value = delete(dataRef.get(), key);
      AtomicLongArray data = rg.rehashData;// rehashRef.get();
      if (data != null) {
        // if rip == false, this is what main storage has
        // if rip == true - proceed
        // In any case this ref is from rehashRef
        long val = delete(data, key);
        // This is the latest value of key
        if (val != NULL && value != NULL) {
          alive.increment();
          //tombstones.decrement();
          if (deallocator != null) {
            deallocator.deallocate(value);
          }
          return val;
        }
        if (val != NULL) {
          return val;
        } else if (value != NULL) {
          return value;
        }
      } else {
        // Delete again from main
        AtomicLongArray dd = dataRef.get();
        long val = delete(dd, key);
        if (val != NULL && value != NULL) {
          alive.increment();
          //tombstones.decrement();
          if (deallocator != null) {
            deallocator.deallocate(value);
          }
          return val;
        }
        if (val != NULL) {
          return val;
        } else if (value != NULL) {
          return value;
        }
      }
      return value;
    } else {
      maybeStartRehashing();
    }
    return delete(dataRef.get(), key);
  }
  
  /**
   * Deletes key and returns the deleted value or NULL
   * @param key key to delete
   * @return value or NULL
   */
  
  private long delete(final AtomicLongArray data, long key) {
    
    boolean isMainArray = data == dataRef.get();
    RehashGroup rg = null;
    long rehashReadOffset = Long.MIN_VALUE;
    if (isMainArray) {
      rg = rehashGroup.get();
      if (rg != null) {
        rehashReadOffset = rg.rehashReadOffset.get();
      }
    }
    
    if (key == NULL || key == DELETED || key == LOCKED) {
      key = RANDOM_LONG - key;
    }
    final long hash = keysHashed ? key : Utils.squirrel3(key);
    final int mask = (int) (data.length() - 1) & 0xfffffffe;
    int index = (int) (hash & mask);
    long oldKey = NULL; // missing key
    long oldValue = NULL;
    long startIndexKey;
    final int startIndex = index;
    /*
     * Serialize operations for the given key = 'key' Make sure that during search no mutations for
     * the same key can run in parallel
     */
    while (true) {
      while ((startIndexKey = data.get(startIndex)) == LOCKED);
      if (data.compareAndSet(startIndex, startIndexKey, LOCKED)) {
        break;
      }
    }
    try {
      // Check startIndexKey first
      if (startIndexKey == NULL) {
        return NULL; // not found
      } else if (startIndexKey == key) {
        oldValue = data.getAndSet(startIndex + 1, NULL);
        startIndexKey = DELETED;
        // Update counters
        alive.decrement();
        if (!(isMainArray && rehashReadOffset > index)) {
          tombstones.increment();
        }
        return oldValue;
      }
      index = (startIndex + 2) & mask;
      while ((oldKey = data.get(index)) != NULL) {
        if (key == oldKey) {
          break;
        } else if (oldKey == LOCKED) {
          while ((oldKey = data.get(index)) == LOCKED);
          if (oldKey == key) {
            break;
          }
        }
        // Linear probing
        index = (int) ((index + 2) & mask);
      }
      if (oldKey == key) {
        // Here no other keys are possible because
        // we serialized mutation operations for a given key
        // Tip: key could not be deleted by other thread, key
        // could not be changed either because we do not allow such
        // operation
        oldValue = data.getAndSet(index + 1, NULL);
        // This cell can be locked now, busy loop until
        // operation succeeds
        while (!data.compareAndSet(index, oldKey, DELETED));
        // Update counters
        alive.decrement();
        if (!(isMainArray && rehashReadOffset > index)) {
          // Rehashing in progress and this tombstone will be cleared by rehashing later
          tombstones.increment();
        }      
      }
      return oldValue;
    } finally {
      // Unlock index
      data.set(startIndex, startIndexKey);
    }
  }

  /**
   * Checks probabilistically if we have to start rehashing
   * TODO: start rehashing in a separate thread
   * to avoid latency spikes
   */
  private void maybeStartRehashing() {
    if (rehashGroup.get() != null) {
      return;
    }
    ThreadLocalRandom rnd = ThreadLocalRandom.current();
    if ((rnd.nextInt() & rcmask) != 0) {
      // Probability of executing the code down below
      // is 1 / (float) rcmask, by default is 1 / 256
      // The reason - the code is expensive to run on
      // every operation (LongAdder.longValue() is expensive)
      return;
    }
    long aliveObj = alive.longValue();
    long deletedObj = tombstones.longValue();
    int len = dataRef.get().length();
    // boolean doCleaning = deletedObj > 0.5 * (len / 2);
    boolean doRehashing = aliveObj + deletedObj > 0.75 * (len / 2) || deletedObj > 0.375 * len / 2;
    // && aliveObj > 0.5 * (len / 2);
    // doCleaning = doCleaning || aliveObj + deletedObj > 0.75 * (len / 2)
    // && aliveObj > 0.5 * (len / 2);

    if (!doRehashing /* && ! doCleaning */) {
      return;
    }

    // Try to own exclusive lock
    if (!rehashStarter.compareAndSet(null, Thread.currentThread())) {
      // already owned by other thread - return
      return;
    }
    RehashGroup rg = new RehashGroup();
    rg.mainData = dataRef.get();
    AtomicLongArray rehashRef;
    int newLen = len;
    if (5 * aliveObj / 4 < len / 8) {
      newLen = len / 2;
    } else if (5 * aliveObj / 4 < len / 4) {
      newLen = len;
    } else {
      newLen = 2 * len;
    }
    rehashRef = new AtomicLongArray(newLen);
    this.rehashChunkSize = 8;

    rg.rehashData = rehashRef;
    rehashGroup.set(rg);
  }
  
  /**
   * Finalize rehashing
   */
  private void finishRehashing() {
    RehashGroup rg = rehashGroup.get();
    AtomicLongArray data = rg.rehashData;
    dataRef.set(data);
    updateRehashCheckMask();
    rehashGroup.set(null);
    rehashStarter.set(null);
  }
  
  /**
   * Rehashes next chunk of a main array
   */
  private void nextRehashingRun() {
    RehashGroup rg = rehashGroup.get();
    if (rg == null) {
      return;
    }
    AtomicInteger rehashReadOffset = rg.rehashReadOffset;
    AtomicLongArray data = rg.mainData;
    final int len = data.length();
    int startOff = rehashReadOffset.getAndAdd(rehashChunkSize * 2);
    if(startOff >= len) {
      // Rehashing is complete already
      return;
    }
    AtomicLongArray rehashData = rg.rehashData;
    int chunkSize = Math.min(len - startOff, rehashChunkSize * 2);
    outer:
    for (int i = startOff; i < startOff + chunkSize; i += 2) {
      long key = data.get(i);
      if (key == NULL || key == DELETED) {
        if (key == DELETED) {
          tombstones.decrement();
        }
        continue;
      }
      if (key == LOCKED) {
        // Wait until unlocked
        while((key = data.get(i)) == LOCKED);
      }
      if (key == NULL || key == DELETED) {
        // Special attention to DELETED
        // decrement tombstone
        if (key == DELETED) {
          tombstones.decrement();
        }
        continue;
      }
 
      // Lock this index - locking prevents race condition with possible deletes
      while(!data.compareAndSet(i, key, LOCKED)) {
        long read = data.get(i);
        if (read == DELETED) {
          tombstones.decrement();
          continue  outer;
        }
      }
      // Add  key-value (new version may exists in rehash storage and we 
      // do not want to overwrite it)
      long value = data.get(i + 1);
      boolean result = add(rehashData, key, value);
      if (!result) {
        if (deallocator != null) {
          deallocator.deallocate(value);
        }
        alive.decrement();
      } else {
        // set value to NULL
        data.set(i + 1,  NULL);
      }
      // Unlock
      data.set(i, key);
    }
    // Wait until all previous runs (in other threads) are complete
    while(rg.rehashConfirmedOffset != startOff);    
    // Now increment confirmed rehash offset
    rg.rehashConfirmedOffset += chunkSize;

    if (rg.rehashConfirmedOffset == len) {
      // This thread was the last one and it will finalize 
      // the map rehashing
      finishRehashing();
    }
  }
  
  /**
   * Used for testing only
   */
  public void dispose() {
    RehashGroup rg = rehashGroup.get();
    if (rg != null) {
      dispose(rg.rehashData);
      dispose(rg.mainData);
    } else {
      dispose(dataRef.get());
    }
  }
  
  private void dispose(AtomicLongArray data) {
    if (deallocator == null) {
      return;
    }
    for (int i = 0; i < data.length(); i += 2) {
      long value = data.get(i + 1);
      deallocator.deallocate(value);
    }
  }
  
  @SuppressWarnings("unused")
  private boolean cas(AtomicLongArray array, int index, long expValue, long newValue, long timeout) {
    long start = System.nanoTime();
    while (!array.compareAndSet(index, expValue, newValue)) {
      if (System.nanoTime() - start > timeout) {
        return false;
      }
    }
    return true;
  }
  
  @SuppressWarnings("unused")
  private long waitUntil(AtomicLongArray array, int index, long value, long timeout) {
    long v = 0;
    long start = System.nanoTime();
    while ((v = array.get(index)) == value) {
      if (System.nanoTime() - start > timeout) {
        break;
      }
    }
    return v;
  }
  
  public long search(long key) {
    AtomicLongArray data = dataRef.get();
    int len = data.length();
    final int mask = (int) (data.length() - 1) & 0xfffffffe;
    int index = (int) (key & mask);
    for(int i = index; i < len; i+=2) {
      long k = data.get(i);
      if (k == key) {
        return data.get(i + 1);
      } 
    }
    return -1;
  }
}
