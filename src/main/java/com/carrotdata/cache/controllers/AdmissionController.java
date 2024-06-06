/*
 * Copyright (C) 2024-present Carrot Data, Inc. 
 * <p>This program is free software: you can redistribute it
 * and/or modify it under the terms of the Server Side Public License, version 1, as published by
 * MongoDB, Inc.
 * <p>This program is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
 * PURPOSE. See the Server Side Public License for more details. 
 * <p>You should have received a copy of the Server Side Public License along with this program. If not, see
 * <http://www.mongodb.com/licensing/server-side-public-license>.
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
