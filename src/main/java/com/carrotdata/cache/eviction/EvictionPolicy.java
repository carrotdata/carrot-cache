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
package com.carrotdata.cache.eviction;

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
  public int getInsertIndex(long idbPtr, int totalItems);

  /**
   * What to do on item hit
   * @return PROMOTE or DELETE
   */
  public default Action actionOnHit() {
    return Action.PROMOTE;
  }

  /**
   * TODO: move this code out Get rank for a given index (0 - based). 0 - maximum
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
   * TODO: move this code out Get start index for a given segment
   * @param numRanks number of ranks
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
