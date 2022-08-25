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
package com.carrot.cache.io;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.carrot.cache.util.Persistent;
import com.carrot.cache.util.UnsafeAccess;
import com.carrot.cache.util.Utils;

/**
 * 
 * Segment encapsulates all the logic associated
 * with a memory allocation, packing cached entry data,
 * saving and loading to/from disk
 * 
 * Entry format:
 * 
 * VINT - key size
 * VINT - value size
 * Key
 * Value
 *
 */
public class Segment implements Persistent {
  
  /** Logger */
  @SuppressWarnings("unused")
  private static final Logger LOG = LogManager.getLogger(Segment.class);
  
  public final static int META_SIZE = Utils.SIZEOF_LONG;
  /**
   * 
   *  Class encapsulates data segment statistics:
   *  
   *  It keeps total number of cached items in the segment as well as 
   *  total items' rank (sum of ALL item's ranks)
   *  This information is used by Scavenger during recycling candidate
   *  selection: the segment with the minimum rank will be selected 
   *  for recycling.
   *  
   *  
   *  
   */
  public static class Info implements Persistent {
    
    /* Is segment sealed */
    private volatile boolean sealed;
    
    /* Segment rank */
    private int rank;
    
    /* Segment creation time */
    private long creationTime; // in ms
    
    /* Total number of cached items in the segment */
    private AtomicInteger totalItems = new AtomicInteger(0);
        
    /* Total items expected to expire */
    private AtomicInteger totalExpectedToExpireItems = new AtomicInteger(0);
    
    /* Total expired items */
    private AtomicInteger totalExpiredItems = new AtomicInteger(0);
    
    /* Total evicted and deleted items */
    private AtomicInteger totalEvictedItems = new AtomicInteger(0);
    
    /* Segment's id */
    private volatile int id;
    
    /* Segment's size*/
    private volatile long size;
    
    /* Segment data size */
    private AtomicLong dataSize = new AtomicLong(0);
    
    /* Segment block data size - to support block - based writers*/
    private AtomicLong blockDataSize = new AtomicLong(0);
    
    /* Is this segment off-heap. Every segment starts as offheap, but FileIOEngine it will be converted to a file*/
    private volatile boolean offheap;
    
    /* Tracks maximum item expiration time - absolute in ms since 01-01-1970 Jan 1st 12am*/
    private AtomicLong maxExpireAt = new AtomicLong(0);
    
    Info(){
    }
    
    /**
     * Constructor
     */
    Info (int id, int rank, long creationTime) { 
      this.id  = id;
      this.rank = rank;
      this.creationTime = creationTime;
    }
    
    /**
     * Update segment's statistics
     * @param itemIncrement total items to increment
     */
    public void updateEvictedDeleted(int itemIncrement) {
      this.totalEvictedItems.addAndGet(itemIncrement);
    }
    
    /**
     * Is this segment off-heap
     * @return true or false
     */
    public boolean isOffheap() {
      return this.offheap;
    }
    
    /**
     * Set off-heap
     * @param b true or false
     */
    public void setOffheap(boolean b) {
      this.offheap = b;
    }
    
    /**
     * Is sealed
     * @return true or false
     */
    public boolean isSealed() {
      return this.sealed;
    }
    
    /**
     * Set sealed
     * @param b sealed
     */
    public void setSealed(boolean b) {
      this.sealed = b;
    }
    
    
    /**
     * Get total number of cached items in this segment
     * @return total number of cached items
     */
    public int getTotalItems() {
      return this.totalItems.get();
    }
    
    /**
     * Get total number of active items (which are still accessible)
     * @return number
     */
    public int getTotalActiveItems() {
      return this.totalItems.get() - this.totalEvictedItems.get() - this.totalExpiredItems.get();
    }
    
    /**
     * Set total number of items
     * @param num total number of items
     */
    public void setTotalItems(int num) {
      this.totalItems.set(num);
    }
        
    /**
     * Get segment size
     * @return segment size
     */
    public long getSegmentSize() {
      return this.size;
    }
    
    /**
     * Sets segment size
     * @param size segment size
     */
    public void setSegmentSize(long size) {
      this.size = size;
    }
    
    /**
     * Get segment data size
     * @return segment data size
     */
    public long getSegmentDataSize() {
      return this.dataSize.get();
    }
    
    /**
     * Sets segment data size
     * @param size segment data size
     */
    public void setSegmentDataSize(long size) {
      this.dataSize.set(size);
    }
    
    /**
     * Get segment block data size
     * @return segment block data size
     */
    public long getSegmentBlockDataSize() {
      return this.blockDataSize.get();
    }
    
    /**
     * Sets segment block data size
     * @param size segment data size
     */
    public void setSegmentBlockDataSize(long size) {
      this.dataSize.set(size);
    }
    
    /**
     * Get segment's id
     * @return segment's id
     */
    public int getId() {
      return this.id;
    }
    
    /**
     * Set segments id
     * @param id segment's id
     */
    public void setId(int id) {
      this.id = id;
    }
    
    /**
     * Get segment creation time
     * @return segment creation time
     */
    public long getCreationTime() {
      return this.creationTime;
    }
    
    /**
     * Set creation time
     * @param time segments creation time
     */
    public void setCreationTime(long time) {
      this.creationTime = time;
    }
    
    /**
     * Get segment rank 
     * @return segment rank
     */
    public int getRank() {
      return this.rank;
    }
    
    /**
     * Sets segment's rank
     * @param rank segments's rank
     */
    public void setRank(int rank) {
      this.rank = rank;
    }
    
    /**
     * Get number of expired items
     */
    public int getNumberExpiredItems() {
      return this.totalExpiredItems.get();
    }
    
    
    /**
     * Get number of expected to expire items
     */
    public int getNumberExpectedToExpireItems() {
      return this.totalExpectedToExpireItems.get();
    }
    
    /**
     * Get number of evicted - deleted items
     * @return number of evicted - deleted items
     */
    public int getNumberEvictedDeletedItems() {
      return this.totalEvictedItems.get();
    }
    
    /**
     * Expire one item
     */
    public void updateExpired() {
      this.totalExpiredItems.incrementAndGet();
    }

    /**
     * Increment data size
     * @param incr increment
     * @return new data size
     */
    public long incrementDataSize(int incr) {
      return this.dataSize.addAndGet(incr);
    }
     
    /**
     * Increment block data size
     * @param incr increment
     * @return new data size
     */
    public long incrementBlockDataSize(int incr) {
      return this.blockDataSize.addAndGet(incr);
    }
    
    /**
     * Get maximum item expiration time
     * @return max expiration time
     */
    public long getMaxExpireAt() {
      return this.maxExpireAt.get();
    }
    
    /**
     * Set maximum expiration time
     * @param expected expected time
     * @param newValue new value
     * @return true on success, false - otherwise
     */
    public boolean setMaxExpireAt(long expected, long newValue) {
      return maxExpireAt.compareAndSet(expected, newValue);
    }
    
    @Override
    /**
     * Save segment to output stream
     * @param dos output stream
     * @throws IOException
     */
    
    public void save(OutputStream os) throws IOException {
      
        DataOutputStream dos = Utils.toDataOutputStream(os);
        // Write meta
        // Sealed
        dos.writeBoolean(isSealed());
        // Segment Id
        dos.writeInt(getId());
        // Rank
        dos.writeInt(getRank());
        // Creation time 
        dos.writeLong(getCreationTime());
        // Segment size
        dos.writeLong(getSegmentSize());
        // Data size
        dos.writeLong(getSegmentDataSize());
        // Number of entries
        dos.writeInt(getTotalItems());
        // Total number of expected to expire items
        dos.writeInt(getNumberExpectedToExpireItems());
        //Total number of expired items
        dos.writeInt(getNumberExpiredItems());
        // Total evicted and deleted (not expired)
        dos.writeInt(getNumberEvictedDeletedItems());
        // Off-heap
        dos.writeBoolean(isOffheap());
        // Block data size
        dos.writeLong(this.blockDataSize.get());
        dos.flush();
    }

    @Override
    public void load(InputStream is) throws IOException {
      DataInputStream dis = Utils.toDataInputStream(is);
      this.sealed = dis.readBoolean();
      this.id = dis.readInt();
      this.rank = dis.readInt();
      this.creationTime = dis.readLong();
      this.size = dis.readLong();
      this.dataSize.set(dis.readLong());
      this.totalItems.set(dis.readInt());
      this.totalExpectedToExpireItems.set(dis.readInt());
      this.totalExpiredItems.set(dis.readInt());
      this.totalEvictedItems.set(dis.readInt());
      this.offheap = dis.readBoolean();
      this.blockDataSize.set(dis.readLong());
    }
  }
  
  /*
   * Default segment size 
   */
  public final static int DEFAULT_SEGMENT_SIZE = 4 * 1024 * 1024;
  /**
   * Segment's address (if in RAM)
   */
  private long address;
  
  /**
   * Segment size (Not USED)
   */
  private int size;
  
  /**
   * Write lock prevents multiple threads from appending data
   * concurrently
   */
  private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
  
  /* Segment info */
  volatile private Info info;
  
  /* Data writer */
  DataWriter dataWriter;
  
  /* Is valid segment */
  private volatile boolean valid = true;
  /**
   * 
   * Default constructor
   * @param info
   */
  Segment(){
  }
    
  /**
   * Private constructor
   * @param address address of a segment
   * @param size size of a segment
   * @param id segment id
   * @param rank segment's rank (0- based, 0 - maximum rank)
   */
  Segment(long address, int size, int id, int rank) {
    this.address = address;
    this.size = size;
    this.info = new Info(id, rank, System.currentTimeMillis());
    this.info.setSegmentSize(size);
    setOffheap(true);
  }
  
  /**
   * Sets data appender implementation
   * @param da data appender
   */
  public void setDataWriter(DataWriter da) {
    this.dataWriter = da;
  }
  
  /**
   * Is valid
   * @return true or false
   */
  public boolean isValid() {
    return this.valid;
  }
  
  /**
   * Used for testing
   */
  public void dispose() {
    if (isOffheap()) {
      UnsafeAccess.free(this.address);
    }
    this.valid = false;
  }
  
  /**
   * Reuse segment - for off-heap only
   * @param id
   * @param rank
   * @param creationTime
   */
  public void reuse(int id, int rank, long creationTime) {
    this.info = new Info(id, rank, creationTime);
  }
  
  /**
   * Create new segment
   * @param size requested size
   * @param id segment id
   * @param rank segment's rank
   * @param creationTime  segment's creation time
   * @return new segment
   */
  public static Segment newSegment(int size, int id, int rank) {
    long ptr = UnsafeAccess.mallocZeroed(size);
    return new Segment(ptr, size, id, rank);
  }
  
  /**
   * Create new segment
   * @param ptr segment memory address
   * @param size requested size
   * @param id segment id
   * @param rank segment's rank
   * @param creationTime  segment's creation time   
   * @return new segment
   */
  public static Segment newSegment(long ptr, int size, int id, int rank) {
    return new Segment(ptr, size, id, rank);
  }
  
  /**
   * Is this segment off-heap
   * @return true or false
   */
  public boolean isOffheap() {
    return this.info.isOffheap();
  }
  
  /**
   * Set off-heap
   * @param b true or false 
   */
  public void setOffheap(boolean b) {
    this.info.setOffheap(b);
  }
  
  /**
   * Increment data size
   * @param incr increment
   * @return new data size
   */
  public long incrDataSize(int incr) {
    return this.info.incrementDataSize(incr);
  }
   
  /**
   * Increment block data size
   * @param incr increment
   * @return new data size
   */
  public long incrBlockDataSize(int incr) {
    return this.info.incrementBlockDataSize(incr);
  }
  
  /**
   * Increment number of entries
   * @param incr increment
   * @return new number of entries
   */
  private int incrNumEntries(int incr) {
    return this.info.totalItems.addAndGet(incr);
  }
  
  /**
   * Increment expected to expire items
   * @param incr increment
   * @return new value
   */
  private int incrExpectedToExpire(int incr) {
    return this.info.totalExpectedToExpireItems.addAndGet(incr);
  }
  
  /**
   * Get segmemt's id
   * @return segment's id
   */
  public int getId() {
    return this.info.getId();
  }
  
  /**
   * Sets segment's id
   * @param id segment's id
   */
  public void setId(int id) {
    this.info.setId(id);
  }
  
  /**
   * Get segment info
   * @return segment info
   */
  public Info getInfo() {
    return this.info;
  }
  
  /**
   * Get segment's size
   * @return segment's size
   */
  public int getSize() {
    return this.size;
  }
  
  /**
   * Set info
   * @param info
   */
  public void setInfo(Info info) {
    this.info = info;
  }
  
  /**
   * Is segment sealed
   * @return true if - yes, false - otherwise
   */
  public boolean isSealed() {
    return this.info.isSealed();
  }
  
  /**
   * Seal segment
   */
  public void seal() {
    this.info.setSealed(true);
  }
  
  /**
   * Get segment's address (if in memory)
   * @return segment address
   */
  public long getAddress() {
     return this.address;
  }
  
  /**
   * Set address
   * @param ptr address
   */
  public void setAddress(long ptr) {
    this.address = ptr;
  }
  
  /**
   * Get segment's size
   * @return size
   */
  public long size() {
    return this.info.getSegmentSize();
  }
  
  /**
   * Get segment's data size
   * @return segment's data size
   */
  public long getSegmentDataSize() {
    return this.info.getSegmentDataSize();
  }
  
  /**
   * Get segment's block data size
   * @return segment's data size
   */
  public long getSegmentBlockDataSize() {
    return this.info.getSegmentBlockDataSize();
  }
  
  /**
   * Get total number of cached items in this segment 
   * @return number
   */
  public int getTotalItems() {
    return this.info.getTotalItems();
  }
  
  /**
   * Get total number of alive items in the segment
   * @return
   */
  public int getAliveItems() {
    return getTotalItems() - getNumberEvictedDeletedItems() - getNumberExpiredItems();
  }
  
  /**
   * Get number of evicted or explicitly deleted items
   * @return number
   */
  public int getNumberEvictedDeletedItems() {
    return this.info.getNumberEvictedDeletedItems();
  }
  
  /**
   * Get number of expired (reported) items 
   * @return number
   */
  public int getNumberExpiredItems() {
    return this.info.getNumberExpiredItems();
  }
  
  /**
   * Get expected to expire numbers
   * @return number
   */
  public int getNumberExpectedExpireItems() {
    return this.getNumberExpectedExpireItems();
  }
  
  /**
   * Read lock the segment
   */
  public void readLock() {
    lock.readLock().lock();
  }
  
  /**
   * Read unlock the segment
   */
  public void readUnlock() {
    lock.readLock().unlock();
  }
  
  /**
   * Write lock the segment
   */
  public void writeLock() {
    lock.writeLock().lock();
  }
  
  /**
   * Write unlock the segment
   */
  public void writeUnlock() {
    if (lock.isWriteLockedByCurrentThread()) {
      lock.writeLock().unlock();
    }
  }
    
  /**
   * Append new cached item to this segment
   * @param key item key
   * @param item item itself
   * @param expire item expiration time in ms (absolute)
   * @return cached item address (-1 means segment is sealed)
   */
  public long append(byte[] key, byte[] item, long expire) {
    return append(key, 0, key.length, item, 0, item.length, expire);
  }

  /**
   * Append new cached item to this segment
   * @param key item key
   * @param keyOffset key offset
   * @param keySize key size
   * @param item item itself
   * @param itemOffset item offset
   * @param itemSize item size
   * @param expire expiration time
   * @return cached item offset (-1 means segment is sealed)
   */
  public long append(byte[] key, int keyOffset, int keySize, byte[] item, int itemOffset, 
      int itemSize, long expire) {
    if (isSealed()) {
      //TODO: check return value
      return -1;
    }
    try {
      writeLock();
      long offset = this.dataWriter.append(this, key, keyOffset, keySize, item, itemOffset, itemSize);
      if (offset < 0) {
        return -1;
      }
      processExpire(expire);
      incrNumEntries(1);
      if (expire > 0) {
        incrExpectedToExpire(1);
      }
      return offset/* offset in a segment*/;
    } finally {
      writeUnlock();
    }
  }
  
  /**
   * Checks max expire against given expire
   * and set max to a new value if:
   * a. old max value > 0
   * b. new expire is greater than old max value
   * @param expire
   */
  private final void processExpire(long expire) {
    long max = this.info.getMaxExpireAt();
    if (max < 0) return; // do nothing
    boolean result = false;
    if (expire == 0) {
      while(!result) {
        max = this.info.getMaxExpireAt();
        // Signals that this block has some items w/o expiration
        result = this.info.setMaxExpireAt(max, -1);
      }
    } else if (max < expire) {
      while(!result) {
        max = this.info.getMaxExpireAt();
        if (max > expire) return;
        result = this.info.setMaxExpireAt(max, expire);
      }
    }
  }
  
  /**
   * Checks if all items have expiration in thsi segments
   * @return true - yes, false - no
   */
  public boolean isAllExpireSegment() {
    return this.info.getMaxExpireAt() > 0;
  }
  
  /**
   * Append new cached item to this segment
   * @param keyPtr key address
   * @param keySize key size
   * @param itemPtr item address
   * @param itemSize item size
   * @param expire expiration time
   * @return cached entry offset in a segment or -1
   */
  public long append(long keyPtr, int keySize, long itemPtr, int itemSize, long expire) {
    if (isSealed()) {
      //TODO: check return value
      return -1;
    }
    try {
      writeLock();
      long offset = (int) this.dataWriter.append(this, keyPtr, keySize, itemPtr, itemSize);
      if (offset < 0) {
        return -1;
      }
      processExpire(expire);
      // data writer MUST set dataSize in a segment
      incrNumEntries(1);
      if (expire > 0) {
        incrExpectedToExpire(1);
      }
      return offset;
    } finally {
      writeUnlock();
    }
  }

  /**
   * Update segment's statistics
   * @param itemIncrement total items to increment
   */
  public void updateEvictedDeleted() {
    this.info.updateEvictedDeleted(1);
  }
  
  /**
   * Update expired counter and total rank
   * @param rank rank of an expired item
   * @param expire expiration time
   */
  public void updateExpired(long expire) {
    if (this.info.getCreationTime() > expire) {
      return; // do nothing - segment was recycled recently
    }
    this.info.updateExpired();
  }
    

  @Override
  public void save(OutputStream os) throws IOException {

    DataOutputStream dos = Utils.toDataOutputStream(os);
    try {
      readLock();
      // Segment MUST be sealed
      seal();
      // Save info
      this.info.save(dos);
      if (!isOffheap()) {
        return;
      }
      // Write segment size
      long size = size();
      dos.writeLong(size);
      
      int bufSize = (int) Math.min(size, 1024 * 1024);
      byte[] buffer = new byte[bufSize];
      long written = 0;
      while (written < size) {
        int toCopy = (int) Math.min(bufSize, size - written);
        UnsafeAccess.copy(this.address + written, buffer, 0, toCopy);
        written += toCopy;
        dos.write(buffer, 0, toCopy);
      }
    } finally {
      dos.flush();
      readUnlock();
    }
  }

  public void save(RandomAccessFile file) throws IOException {
    try {
      readLock();
      // Segment MUST be sealed
      seal();
      // Write segment size
      long size = size();
      file.writeLong(size);
      
      int bufSize = (int) Math.min(size, 1024 * 1024);
      byte[] buffer = new byte[bufSize];
      long written = 0;
      while (written < size) {
        int toCopy = (int) Math.min(bufSize, size - written);
        UnsafeAccess.copy(this.address + written, buffer, 0, toCopy);
        written += toCopy;
        file.write(buffer, 0, toCopy);
      }
    } finally {
      readUnlock();
    }
  }
  
  @Override
  public void load(InputStream is) throws IOException {
    DataInputStream dis = Utils.toDataInputStream(is);
    this.info = new Info();
    this.info.load(dis);
    
    if (isOffheap()) {
      long size = dis.readLong();
      long ptr = UnsafeAccess.mallocZeroed(size);
      int bufSize = (int) Math.min(1024 * 1024, size);
      byte[] buffer = new byte[bufSize];
      int read = 0;
    
      while(read < size) {
        int toRead = (int) Math.min(size - read, bufSize);
        dis.readFully(buffer, 0, toRead);
        UnsafeAccess.copy(buffer, 0, ptr + read, toRead);
        read += toRead;
      }
      this.setAddress(ptr);
    }
  }
}
