/*
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
package com.carrotdata.cache.controllers;

import java.io.IOException;

import com.carrotdata.cache.Cache;
import com.carrotdata.cache.util.Persistent;

/**
 * Admission controller decides which queue new item must be admitted or re-admitted after eviction:
 * Admission queue or main queue or must be discarded
 */
public interface AdmissionController extends Persistent, ThroughputControllable {

  public default void setCache(Cache cache) throws IOException {
  }

  /**
   * Returns if item should be admitted to the cache
   * @param keyPtr key's address
   * @param keySize item's key size
   * @param valueSize value size
   * @return true if item must be admitted to the cache, false - otherwise
   */
  public default boolean admit(long keyPtr, int keySize, int valueSize) {
    return true;
  }

  /**
   * Returns if item should be admitted to the cache
   * @param key item key buffer
   * @param keyOffset item key buffer offset
   * @param keySize item's key size
   * @param valueSize value size
   * @return true if item must be admitted to the cache, false - otherwise
   */

  public default boolean admit(byte[] key, int keyOffset, int keySize, int valueSize) {
    return true;
  }

  /**
   * Called on each items access
   * @param key keys buffer
   * @param off offset
   * @param size keys size
   */
  public default void access(byte[] key, int off, int size) {
  }

  /**
   * Called on each items access
   * @param keyPtr key's address
   * @param keySize key's size
   */
  public default void access(long keyPtr, int keySize) {
  }

  /**
   * Adjust item rank based on its current rank and value expiration time (ms) The lower expiration
   * time - the lower rank should be to guarantee that items with low expiration time must be
   * recycled first
   * @param popularityRank current rank
   * @param groupRank group rank
   * @param expire expiration time in ms
   * @return new rank
   */
  public default int adjustRank(int popularityRank, int groupRank, long expire) {
    return popularityRank;
  }

  /**
   * Some controller can adjust expiration time (decrease)
   * @param expire expiration time
   * @return new expiration time
   */
  public default long adjustExpirationTime(long expire) {
    return expire;
  }

  /**
   * Should item be evicted to the victim cache
   * @param ibPtr index block address
   * @param ptr item index address
   * @return true - yes, false - otherwise
   */
  public default boolean shouldEvictToVictimCache(long ibPtr, long ptr) {
    return true;
  }
}
