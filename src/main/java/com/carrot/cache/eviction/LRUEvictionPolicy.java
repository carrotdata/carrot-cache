package com.carrot.cache.eviction;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class LRUEvictionPolicy implements EvictionPolicy {
  private static final Logger LOG = LogManager.getLogger(LRUEvictionPolicy.class);

  
  public LRUEvictionPolicy() {
  }
  
  @Override
  public int getPromotionIndex(long cacheItemPtr, int cacheItemIndex, int totalItems) {
    // promote always into the head
    return 0;
  }

  @Override
  public int getEvictionCandidateIndex(long idbPtr, int totalItems) {
    // evict last one
    return totalItems -1;
  }

  @Override
  public int getInsertIndex(long idbPtr, int totalItems) {
    // Insert always into the head
    return 0;
  }
}
