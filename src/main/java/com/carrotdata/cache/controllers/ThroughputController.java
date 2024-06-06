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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotdata.cache.Cache;
import com.carrotdata.cache.util.Persistent;

/**
 * Throughput controller controls sustained write rate and makes recommendations on how to stay
 * within the limits
 */
public interface ThroughputController extends Persistent {

  public static final Logger LOG = LoggerFactory.getLogger(ThroughputController.class);

  public default void setCache(Cache cache) {

  }

  /**
   * Record number of bytes written
   * @param bytes
   */
  public void record(long bytes);

  /**
   * Get controller start time
   * @return controller start time
   */
  public long getStartTime();

  /**
   * Throughput goal in bytes/seconds
   * @return throughput goal in bytes/seconds
   */
  public long getThroughputGoal();

  /**
   * Sets throughput goal
   * @param goal throughput goal
   */
  public void setThrougputGoal(long goal);

  /**
   * Throughput goal in bytes/seconds
   * @return throughput goal in bytes/seconds
   */
  public long getCurrentThroughput();

  /**
   * Get total bytes written
   * @return total bytes written
   */
  public long getTotalBytesWritten();

  /**
   * Adjust parameters (AQ size and compaction discard threshold )
   * @return true, if adjustment was made, false - otherwise
   */
  public boolean adjustParameters();

  public default void printStats() {
    LOG.info("Goal = " + getThroughputGoal() + " current = " + getCurrentThroughput());
  }

}
