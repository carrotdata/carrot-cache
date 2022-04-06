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

import com.carrot.cache.util.Persistent;

/**
 * Admission controller decides which queue new item must be admitted or re-admitted after eviction:
 * Admission queue or main queue or must be discarded
 */
public interface AdmissionController extends Persistent{
  static enum Type {
    AQ, MQ, DUMP
  }
  
  /**
   * Returns queue (or dump) for a new item
   * @param keyPtr key's address
   * @param keySize item's key size
   * @return queue to admit to
   */
  public Type admit(long keyPtr, int keySize);
  
  /**
   * Returns queue (or dump) for a new item
   * @param key item key buffer
   * @param off item key buffer offset
   * @param size item's key size
   * @return queue to admit to
   */
  
  public Type admit(byte[] key, int off, int size);
  /**
   * Returns queue (or dump) for an evicted item
   * @param keyPtr key address
   * @param keySize item's key size
   * @return queue to admit to (either AQ or DUMP)
   */
  public Type readmit(long keyPtr, int keySize);
  
  /**
   * Returns queue (or dump) for an evicted item
   * @param key item key buffer
   * @param off item key buffer offset
   * @param size item's key size
   * @return queue to admit to
   */
  public Type readmit(byte[] key, int off, int size);
  
  /**
   * Called on each items access
   * @param key keys buffer
   * @param off offset
   * @param size keys size
   */
  public void access(byte[] key, int off, int size);
  
  /**
   * Called on each items access
   * @param keyPtr key's address
   * @param keySize key's size
   */
  public void access(long keyPtr, int keySize);
  
  /**
   * Adjust item rank based on its current rank and expiration time (ms)
   * The lower expiration time - the lower rank should be to guarantee
   * that items with low expiration time must be recycled first
   * @param rank current rank
   * @param expire expiration time in ms
   * @return new rank
   */
  public int adjustRank(int rank, long expire);
  
  /**
   * During compaction process, scavenger calls this method to update
   * segment's life-time statistics.
   * @param rank segment's rank
   * @param ttl segment's life-time in ms
   */
  public void registerSegmentTTL(int rank, long ttl);
}
