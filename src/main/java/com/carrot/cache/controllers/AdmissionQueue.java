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
package com.carrot.cache.controllers;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.carrot.cache.Cache;
import com.carrot.cache.index.MemoryIndex;
import com.carrot.cache.util.CacheConfig;
import com.carrot.cache.util.Persistent;
import com.carrot.cache.util.Utils;


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
  private static final Logger LOG = LogManager.getLogger(AdmissionQueue.class);
  
  /* Maximum AQ current size */
  private long currentMaxSize;
  
  /* Global maximum AQ size */
  private long globalMaxSize;
  
  /* Global minimum AQ size */
  private long globalMinSize;
  
  /* Memory index - the queue itself */
  private MemoryIndex index;
  
  /* Cache */
  private Cache cache;
  
  private String cacheName;
  
  /**
   * Public constructor
   * @param initialMaxSize initial maximum size
   */
  public AdmissionQueue(Cache cache) {
    this.cache = cache;
    CacheConfig conf = this.cache.getCacheConfig();
    this.cacheName = this.cache.getName();
    this.currentMaxSize = conf.getAdmissionQueueStartSize(this.cacheName);
    this.globalMaxSize = conf.getAdmissionQueueMaxSize(this.cacheName);
    this.globalMinSize = conf.getAdmissionQueueMinSize(this.cacheName);
    this.index = new MemoryIndex(this.cache, MemoryIndex.Type.AQ);
    this.index.setMaximumSize(this.currentMaxSize);
  }
  
  /**
   * Constructor for testing
   */
  public AdmissionQueue() {
    this.cacheName = "default";
    CacheConfig conf = CacheConfig.getInstance();
    this.currentMaxSize = conf.getAdmissionQueueStartSize(this.cacheName);
    this.globalMaxSize = conf.getAdmissionQueueMaxSize(this.cacheName);
    this.globalMinSize = conf.getAdmissionQueueMinSize(this.cacheName);
    this.index = new MemoryIndex(this.cacheName, MemoryIndex.Type.AQ);
    this.index.setMaximumSize(this.currentMaxSize);
    
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
   * Get maximum AQ size
   * @return maximum size
   */
  public long getMaximumSize() {
    return this.currentMaxSize;
  }
  
  /**
   * Sets maximum AQ size
   * @param max new maximum size
   */
  public void setMaximumSize(long max) {
    if (max > this.globalMaxSize || max < this.globalMinSize) {
      throw new IllegalArgumentException(String.format("requested maximum queue size %d is out of allowed range [%d, %d]", 
        max, this.globalMinSize, this.globalMaxSize));
    }
    this.currentMaxSize = max;
    this.index.setMaximumSize(max);
  }
  
  
  /**
   * Get global maximum AQ size
   * @return global maximum size
   */
  public long getGlobalMaximumSize() {
    return this.globalMaxSize;
  }
  
  /**
   * Sets maximum AQ size
   * @param max new maximum size
   */
  public void setGlobalMaximumSize(long max) {
    this.globalMaxSize = max;
  }
  
  /**
   * Get global minimum AQ size
   * @return global minimum size
   */
  public long getGlobalMinimumSize() {
    return this.globalMinSize;
  }
  
  /**
   * Sets global minimum AQ size
   * @param  new global minimum size
   */
  public void setGlobalMinimumSize(long min) {
    this.globalMinSize = min;
  }
  
  /**
   * Add new key to the AQ. The key can be added only
   * if it is not present in the AQ. If it is already in the AQ
   * it is deleted. - Atomic operation
   * @param key key array
   * @return true - if key was added, false - existed and deleted
   */
  
  public boolean addIfAbsentRemoveIfPresent(byte[] key) {
    return addIfAbsentRemoveIfPresent(key, 0, key.length);
  }
  /**
   * Add new key to the AQ. The key can be added only
   * if it is not present in the AQ. If it is already in the AQ
   * it is deleted. Atomic operation
   * @param key key array
   * @param off offset
   * @param len length
   * @return true - if key was added, false - existed and deleted
   */
  
  public boolean addIfAbsentRemoveIfPresent(byte[] key, int off, int len) {
    return index.aarp(key, off, len) == MemoryIndex.MutationResult.DELETED? false: true;
  }
  
  /**
   * Add new key to the AQ. The key can be added only
   * if it is not present in the AQ. If it is already in the AQ
   * it is deleted. Atomic operation.
   * @param keyPtr key address
   * @param keySize key length
   * @return true - if key was added, false - existed and deleted
   */
  
  public boolean addIfAbsentRemoveIfPresent(long keyPtr, int keySize) {
    return index.aarp(keyPtr, keySize) == MemoryIndex.MutationResult.DELETED? false: true;
  }
  
  @Override
  public void save(OutputStream os) throws IOException {
    DataOutputStream dos = Utils.toDataOutputStream(os);
    dos.writeLong(currentMaxSize);
    index.save(dos);
    dos.flush();
  }

  @Override
  public void load(InputStream is) throws IOException {
    DataInputStream dis = Utils.toDataInputStream(is);
    this.currentMaxSize = dis.readLong();
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
