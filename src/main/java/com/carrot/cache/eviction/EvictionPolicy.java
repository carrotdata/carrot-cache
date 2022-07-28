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
package com.carrot.cache.eviction;

public interface EvictionPolicy {
  static enum Action {
    PROMOTE, DELETE
  }
  
  public default void setCacheName(String cacheName) {
    
  }
  /**
   * Get new index for item being promoted
   * @param cacheItemPtr current item address
   * @param cacheItemIndex index of a current item (0- based) 
   * @param totalItems total number of items in Index-Data-Block
   * @return new index for promoted item
   */
  public int getPromotionIndex(long cacheItemPtr, int cacheItemIndex, int totalItems);
  /**
   * Get item for eviction
   * @param idbPtr Index-Data-Block pointer
   * @param totalItems total number of items in this IDB
   * @return index of an item for eviction
   */
  public int getEvictionCandidateIndex(long idbPtr, int totalItems);
  
  /**
   * Get index to insert new item 
   * @param idbPtr Index-Data-Block pointer
   * @param totalItems total number of items in this IDB
   * @return index to insert new item
   */
  public int getInsertIndex (long idbPtr, int totalItems);
  
  /**
   * What to do on item hit
   * @return PROMOTE or DELETE
   */
  public default Action actionOnHit() {
    return Action.PROMOTE;
  }
  
  /**
   * TODO: move this code out
   * Get rank for a given index (0 - based). 0 - maximum
   * @param numRanks total number of ranks
   * @param cacheItemIndex
   * @param totalItems
   * @return segment number
   */
  public default int getRankForIndex(int numRanks, int cacheItemIndex, int totalItems) {
    if (totalItems == 0) return 0;
    return cacheItemIndex * numRanks / totalItems;
  }
  
  /**
   * TODO: move this code out
   * Get start index for a given segment
   * @param total number of ranks
   * @param rank rank
   * @param totalItems total items in Index-Data-Block
   * @return start index
   */
  public default int getStartIndexForRank(int numRanks, int rank, int totalItems) {
    return rank * totalItems / numRanks;
  }
  
  /**
   * get default rank for insert operation
   * @return default rank
   */
  public default int getDefaultRankForInsert() {
    return 0;
  }
}
