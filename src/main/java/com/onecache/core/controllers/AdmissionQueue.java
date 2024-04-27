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
package com.onecache.core.controllers;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.onecache.core.Cache;
import com.onecache.core.index.MemoryIndex;
import com.onecache.core.util.CacheConfig;
import com.onecache.core.util.Persistent;
import com.onecache.core.util.Utils;


/**
 * 
 * Cache admission queue (AQ)
 * 
 * All new items first must be added to the AQ. If item is already in AQ
 * it gets deleted from AQ and added to the main cache.
 * 
 * The major purpose of adding AQ is the cache admission control to minimize SSD cells wearing.
 * We put into the main cache (write to SSD) only items, which are "worthy". AQ prevents 
 * cache pollution due to long scan operations, as well as pollution of the cache by items
 * which are not popular enough.
 * 
 * By varying AQ size we can control sustained cache write speed as well. The larger size of AQ - the 
 * more items will get into the main cache and vice versa: the smaller AQ size is the less items will be 
 * added to the main cache. 
 * 
 */
public class AdmissionQueue implements Persistent {
  /** Logger */
  @SuppressWarnings("unused")
  private static final Logger LOG = LoggerFactory.getLogger(AdmissionQueue.class);
  
  /* Maximum AQ current size */
  protected double currentMaxSizeRatio;
  
  /* Global maximum AQ size */
  protected double globalMaxSizeRatio;
  
  /* Global minimum AQ size */
  protected double globalMinSizeRatio;
  
  /* Memory index - the queue itself */
  protected MemoryIndex index;
  
  /* Cache */
  protected Cache cache;
  
  /* Cache name */
  protected String cacheName;
  
  /* Tracks total insert number */
  private AtomicLong totalPuts = new AtomicLong();
  
  /* Total size of all inserted items */
  private AtomicLong totalSize = new AtomicLong();
  
  /* Maximum cache size */
  protected long maxCacheSize;
  
  /**
   * Public constructor
   * 
   * @param cache parent cache
   */
  public AdmissionQueue(Cache cache) {
    this.cache = cache;
    CacheConfig conf = this.cache.getCacheConfig();
    this.cacheName = this.cache.getName();
    this.currentMaxSizeRatio = conf.getAdmissionQueueStartSizeRatio(this.cacheName);
    this.globalMaxSizeRatio = conf.getAdmissionQueueMaxSizeRatio(this.cacheName);
    this.globalMinSizeRatio = conf.getAdmissionQueueMinSizeRatio(this.cacheName);
    this.maxCacheSize = conf.getCacheMaximumSize(this.cacheName);
    this.index = new MemoryIndex(this.cacheName, MemoryIndex.Type.AQ);
  }
  
  /**
   * Constructor for testing
   */
  public AdmissionQueue() {
    this.cacheName = "default";
    CacheConfig conf = CacheConfig.getInstance();
    this.currentMaxSizeRatio = conf.getAdmissionQueueStartSizeRatio(this.cacheName);
    this.globalMaxSizeRatio = conf.getAdmissionQueueMaxSizeRatio(this.cacheName);
    this.globalMinSizeRatio = conf.getAdmissionQueueMinSizeRatio(this.cacheName);
    this.maxCacheSize = conf.getCacheMaximumSize(this.cacheName);
    this.index = new MemoryIndex(this.cacheName, MemoryIndex.Type.AQ);
    
  }
  
  /**
   * Constructor for testing
   * @param conf cache configuration
   */
  public AdmissionQueue(CacheConfig conf) {
    this.cacheName = "default";
    this.currentMaxSizeRatio = conf.getAdmissionQueueStartSizeRatio(this.cacheName);
    this.globalMaxSizeRatio = conf.getAdmissionQueueMaxSizeRatio(this.cacheName);
    this.globalMinSizeRatio = conf.getAdmissionQueueMinSizeRatio(this.cacheName);
    this.maxCacheSize = conf.getCacheMaximumSize(this.cacheName);
    this.index = new MemoryIndex(this.cacheName, MemoryIndex.Type.AQ);
  }
  
  /**
   * Get memory index for this admission queue
   * @return memory index
   */
  public MemoryIndex getMemoryIndex() {
    return this.index;
  }
  
  /**
   * Current size of the AQ
   * @return size
   */
  public long size() {
    return this.index.size();
  }
  
  /**
   * Get current maximum AQ size ratio as a fraction of 
   * a maximum cache size
   * @return maximum size
   */
  public double getCurrentMaxSizeRatio() {
    return this.currentMaxSizeRatio;
  }
  
  /**
   * Sets maximum AQ size
   * @param max new maximum size
   */
  public void setCurrentMaxSizeRatio(double max) {
    if (max > this.globalMaxSizeRatio || max < this.globalMinSizeRatio) {
      throw new IllegalArgumentException(String.format("requested maximum queue size %f is out of allowed range [%f, %f]", 
        max, this.globalMinSizeRatio, this.globalMaxSizeRatio));
    }
    this.currentMaxSizeRatio = max;
    double avgItemSize = (double) this.totalSize.get() / this.totalPuts.get();
    long maxItems = (long) (this.maxCacheSize * this.currentMaxSizeRatio / avgItemSize);
    if (maxItems > 0) {
      this.index.setMaximumSize(maxItems);
    }
  }
  
  
  /**
   * Get global maximum AQ size ratio
   * @return global maximum size ratio
   */
  public double getGlobalMaxSizeRatio() {
    return this.globalMaxSizeRatio;
  }
  
  /**
   * Sets global maximum AQ size ratio
   * @param max new maximum size
   */
  public void setGlobalMaxSizeRatio(double max) {
    this.globalMaxSizeRatio = max;
  }
  
  /**
   * Get global minimum AQ size ratio
   * @return global minimum size ratio
   */
  public double getGlobalMinSizeRatio() {
    return this.globalMinSizeRatio;
  }
  
  /**
   * Sets global minimum AQ size ratio
   * @param min  new global minimum size ratio
   */
  public void setGlobalMinSizeRatio(double min) {
    this.globalMinSizeRatio = min;
  }
  
  /**
   * Add new key to the AQ. The key can be added only
   * if it is not present in the AQ. If it is already in the AQ
   * it is deleted. - Atomic operation
   * @param key key array
   * @param valueSize value size
   * @return true - if key was added, false - existed and deleted
   */
  
  public boolean addIfAbsentRemoveIfPresent(byte[] key, int valueSize) {
    updateStats(key.length, valueSize);
    checkEviction();
    return index.aarp(key, 0, key.length) == MemoryIndex.MutationResult.DELETED? false: true;
  }
  
  private void checkEviction() {
    double avgItemSize = (double) this.totalSize.get() / this.totalPuts.get();
    long maxItems = (long) (this.maxCacheSize * this.currentMaxSizeRatio / avgItemSize);
    if (maxItems <= this.index.size() && !this.index.isEvictionEnabled()) {
      this.index.setEvictionEnabled(true);
    } else if (maxItems * 0.95 >= this.index.size() && this.index.isEvictionEnabled()) {
      this.index.setEvictionEnabled(false);

    }
  }

  private void updateStats(int keySize, int valueSize) {
    int size = Utils.kvSize(keySize, valueSize);
    this.totalPuts.incrementAndGet();
    this.totalSize.addAndGet(size);
  }
  /**
   * Add new key to the AQ. The key can be added only
   * if it is not present in the AQ. If it is already in the AQ
   * it is deleted. Atomic operation
   * @param key key array
   * @param off offset
   * @param len length
   * @param valueSize value size
   * @return true - if key was added, false - existed and deleted
   */
  
  public boolean addIfAbsentRemoveIfPresent(byte[] key, int off, int len, int valueSize) {
    updateStats(len, valueSize);
    checkEviction();
    return index.aarp(key, off, len) == MemoryIndex.MutationResult.DELETED? false: true;
  }
  
  /**
   * Add new key to the AQ. The key can be added only
   * if it is not present in the AQ. If it is already in the AQ
   * it is deleted. Atomic operation.
   * @param keyPtr key address
   * @param keySize key length
   * @param valueSize value size
   * @return true - if key was added, false - existed and deleted
   */
  
  public boolean addIfAbsentRemoveIfPresent(long keyPtr, int keySize, int valueSize) {
    updateStats(keySize, valueSize);
    checkEviction();
    return index.aarp(keyPtr, keySize) == MemoryIndex.MutationResult.DELETED? false: true;
  }
  
  @Override
  public void save(OutputStream os) throws IOException {
    DataOutputStream dos = Utils.toDataOutputStream(os);
    dos.writeDouble(this.currentMaxSizeRatio);
    dos.writeLong(totalPuts.get());
    dos.writeLong(totalSize.get());
    index.save(dos);
    dos.flush();
  }

  @Override
  public void load(InputStream is) throws IOException {
    DataInputStream dis = Utils.toDataInputStream(is);
    this.currentMaxSizeRatio = dis.readDouble();
    this.totalPuts = new AtomicLong(dis.readLong());
    this.totalSize = new AtomicLong(dis.readLong());
    //TODO: set parent Cache after loading
    this.index = new MemoryIndex();
    this.index.load(dis);
  }
  
  /**
   * Dispose the AQ
   */
  public void dispose() {
    this.index.dispose();
  }
}
