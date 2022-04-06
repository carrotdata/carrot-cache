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
 * 8 bytes - expiration time
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
    
    /* Total rank of all items in this segment */
    private AtomicLong totalRank = new AtomicLong(0);
    
    /* Total items expected to expire */
    private AtomicInteger totalExpectedToExpireItems = new AtomicInteger(0);
    
    /* Total expired items */
    private AtomicInteger totalExpiredItems = new AtomicInteger(0);
    
    /* Segment's id */
    private int id;
    
    /* Segment's size*/
    private long size;
    
    /* Segment data size */
    private AtomicLong dataSize = new AtomicLong(0);
    
    /* Is this segment off-heap*/
    private boolean offheap;
    
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
     * @param rankIncrement total rank to increment
     */
    public void update(int itemIncrement, int rankIncrement) {
      this.totalItems.addAndGet(itemIncrement);
      this.totalRank.addAndGet(rankIncrement);
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
     * Get segments' (average) rank
     * @return segment's average rank
     */
    
    public double getAverageRank() {
      return (double) this.totalRank.get() / this.totalItems.get();
    }
    
    /**
     * Get total number of cached items in this segment
     * @return total number of cached items
     */
    public int getTotalItems() {
      return this.totalItems.get();
    }
    
    /**
     * Set total number of items
     * @param num total number of items
     */
    public void setTotalItems(int num) {
      this.totalItems.set(num);
    }
    
    /**
     * Get total rank of the segment
     * @return
     */
    public long getTotalRank() {
      return this.totalRank.get();
    }
    
    /**
     * Set total rank
     * @param totalRank total rank
     */
    public void setTotalRank(long totalRank) {
      this.totalRank.set(totalRank);
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
     * Sets number of expired items
     * @param num number of expired items
     */
    public void setNumberOfExpiredItems(int num) {
      this.totalExpiredItems.set(num);
    }
    
    /**
     * Get number of expected to expire items
     */
    public int getNumberExpectedToExpireItems() {
      return this.totalExpectedToExpireItems.get();
    }
    
    /**
     * Sets number of expected to expire items
     * @param num number of expected to expire items
     */
    public void setNumberOfExpectedToExpireItems(int num) {
      this.totalExpectedToExpireItems.set(num);
    }
    
    /**
     * Expire one item
     * @return current number of expired
     */
    public int expire() {
      return this.totalExpiredItems.incrementAndGet();
    }
    
    /**
     * Expected to expire item
     * @return current number of expected to expire
     */
    
    public int expectedExpire() {
      return this.totalExpectedToExpireItems.incrementAndGet();
    }

    /**
     * Increment data size
     * @param incr increment
     * @return new data size
     */
    public long incrementDataSize(int incr) {
      return this.dataSize.addAndGet(incr);
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
        // Total rank
        dos.writeLong(getTotalRank());
        // Total number of expected to expire items
        dos.writeInt(getNumberExpectedToExpireItems());
        //Total number of expired items
        dos.writeInt(getNumberExpiredItems());
        // Off-heap
        dos.writeBoolean(isOffheap());
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
      this.totalRank.set(dis.readLong());
      this.totalExpectedToExpireItems.set(dis.readInt());
      this.totalExpiredItems.set(dis.readInt());
      this.offheap = dis.readBoolean();
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
  private Info info;
  
  /**
   * 
   * Default constructor
   * @param info
   */
  Segment(){
  }
  
  /**
   *  Used for off-heap only
   * @param address memory address
   * @param size segment size
   */
  Segment(long address, int size){
    this.address = address;
    this.size = size;
  }
  
  /**
   * Private constructor
   * @param address address of a segment
   * @param size size of a segment
   */
  Segment(long address, int size, int id, int rank, long creationTime) {
    this.address = address;
    this.size = size;
    this.info = new Info(id, rank, creationTime);
  }
  
  
  /**
   * Create new segment
   * @param size requested size
   * @return new segment
   */
  public static Segment newSegment(int size, int id, int rank, long creationTime) {
    long ptr = UnsafeAccess.malloc(size);
    return new Segment(ptr, size, id, rank, creationTime);
  }
  
  /**
   * Create new segment
   * @param size segment size
   * @return new segment instance
   */
  public static Segment newSegment(int size) {
    long ptr = UnsafeAccess.malloc(size);
    return new Segment(ptr, size);
  }
  
  /**
   * Create new empty segment
   * @return new segment
   */
  public static Segment newSegment() {
    return new Segment();
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
  private long incrDataSize(int incr) {
    return this.info.incrementDataSize(incr);
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
   * Get number of cached entries
   * @return number of cached entries
   */
  public int numberOfEntries() {
    return this.info.getTotalItems();
  }
  
  /**
   * Get segment's data size
   * @return segment's data size
   */
  public long dataSize() {
    return this.info.getSegmentDataSize();
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
    lock.writeLock().unlock();
  }
  
  /**
   * Append new cached item to this segment
   * @param key item key
   * @param item item itself
   * @return cached item address (-1 means segment is sealed)
   */
  public long append(byte[] key, byte[] item, long expire) {
    return append(key, 0, key.length, item, 0, item.length, expire);
  }

  /**
   * Append new cached item to this segment
   * @param key item key
   * @param item item itself
   * @return cached item offset (-1 means segment is sealed)
   */
  public long append(byte[] key, int keyOffset, int keySize, byte[] item, int itemOffset, int itemSize, long expire) {
    if (isSealed()) {
      //TODO: check return value
      return -1;
    }
    try {
      writeLock();
      int requiredSize = requiredSize(keySize, itemSize);
      if (requiredSize + dataSize() /*+ HEADER_SIZE */> size()) {
        seal();
        return -1;
      }
      long off = dataSize();
      long addr = this.address + dataSize() /*+ HEADER_SIZE*/;
      // Copy data
      // Expire
      UnsafeAccess.putLong(addr, expire);
      
      incrDataSize(Utils.SIZEOF_LONG);
      
      addr += Utils.SIZEOF_LONG;
      // Key size
      Utils.writeUVInt(addr, keySize);
      int kSizeSize = Utils.sizeUVInt(keySize);

      incrDataSize(kSizeSize);
      addr += kSizeSize;
      // Value size
      Utils.writeUVInt(addr, itemSize);
      int vSizeSize = Utils.sizeUVInt(itemSize);
      incrDataSize(vSizeSize);
      addr += vSizeSize;
      // Copy key
      UnsafeAccess.copy(key, keyOffset, addr, keySize);
      incrDataSize(keySize);
      addr += keySize;
      // Copy value (item)
      UnsafeAccess.copy(item, itemOffset, addr, itemSize);
      incrDataSize(itemSize);
      addr += itemSize;
      // Increment number of entries
      incrNumEntries(1);
      return off /* offset in a segment*/;
    } finally {
      writeUnlock();
    }
  }
  /**
   * Append new cached item to this segment
   * @param keyPtr key address
   * @param keySize key size
   * @param itemPtr item address
   * @param itemSize item size
   * @param expire expiration time
   * @return cached entry address or -1
   */
  public long append(long keyPtr, int keySize, long itemPtr, int itemSize, long expire) {
    if (isSealed()) {
      //TODO: check return value
      return -1;
    }
    try {
      writeLock();
      int requiredSize = requiredSize(keySize, itemSize);
      if (requiredSize + dataSize() /*+ HEADER_SIZE*/ > size()) {
        seal();
        return -1;
      }
      long off = dataSize();
      long addr = this.address + dataSize() /*+ HEADER_SIZE*/;
      // Copy data
      // Expire
      UnsafeAccess.putLong(addr, expire);
      incrDataSize(Utils.SIZEOF_LONG);
      addr += Utils.SIZEOF_LONG;
      // Key size
      Utils.writeUVInt(addr, keySize);
      int kSizeSize = Utils.sizeUVInt(keySize);
      incrDataSize(kSizeSize);
      addr += kSizeSize;
      // Value size
      Utils.writeUVInt(addr, itemSize);
      int vSizeSize = Utils.sizeUVInt(itemSize);
      incrDataSize(vSizeSize);
      addr += vSizeSize;
      // Copy key
      UnsafeAccess.copy(keyPtr, addr, keySize);
      incrDataSize(keySize);
      addr += keySize;
      // Copy value (item)
      UnsafeAccess.copy(itemPtr, addr, itemSize);
      incrDataSize(itemSize);
      addr += itemSize;
      // Increment number of entries
      incrNumEntries(1);
      return off;
    } finally {
      writeUnlock();
    }
  }

  /**
   * Update segment's statistics
   * @param itemIncrement total items to increment
   * @param rankIncrement total rank to increment
   */
  public void update(int itemIncrement, int rankIncrement) {
    this.info.update(itemIncrement, rankIncrement);
  }
  
  /**
   * Get segments' (average) rank
   * @return segment's average rank
   */
  
  public double getRank() {
    return this.info.getAverageRank();
  }
  
  /**
   * Calculate required size for a cached item
   * @param keyLength key length
   * @param valueLength value length
   * @return required size
   */
  private int requiredSize(int keyLength, int valueLength) {
    return Utils.SIZEOF_LONG + Utils.sizeUVInt(keyLength) + 
        Utils.sizeUVInt(valueLength) + keyLength + valueLength;
  }

  @Override
  public void save(OutputStream os) throws IOException {
    DataOutputStream dos = Utils.toDataOutputStream(os);
    try {
      
      readLock();
      // Write data size
      dos.writeLong(dataSize());
      
      int size = (int) Math.min(dataSize(), 1024 * 1024);
      byte[] buffer = new byte[size];
      long written = 0;
      long dataSize = dataSize() /*+ HEADER_SIZE*/;
      while (written < dataSize) {
        int toCopy = (int) Math.min(size, dataSize - written);
        UnsafeAccess.copy(this.address + written, buffer, 0, toCopy);
        written += toCopy;
        dos.write(buffer, 0, toCopy);
      }
      dos.flush();
    } finally {
      readUnlock();
    }
  }

  @Override
  public void load(InputStream is) throws IOException {
    DataInputStream dis = Utils.toDataInputStream(is);
    long size = dis.readLong();
    long ptr = UnsafeAccess.malloc(size);
    int bufSize = (int) Math.min(1024 * 1024, size);
    byte[] buffer = new byte[bufSize];
    int read = 0;
    
    while(read < size) {
      int toRead = (int) Math.min(size - read, bufSize);
      dis.readFully(buffer, 0, toRead);
      UnsafeAccess.copy(ptr + read, buffer, 0, toRead);
      read += toRead;
    }
    this.setAddress(ptr);
  }
}
