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
   * @param bytes number of bytes written
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
