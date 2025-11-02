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
package com.carrotdata.cache.jmx;

public interface CacheJMXSinkMBean {

  /**
   * Epoch start time
   * @return epoch start time as a formatted string
   */
  String getepoch_start_time();

  /**
   * Get cache type : DISK or MEMORY
   * @return cache type
   */
  String gettype();

  /**
   * Get cache maximum size in bytes
   * @return maximum size
   */
  long getmax_size_bytes();

  /**
   * Allocated size in bytes
   * @return allocated size
   */
  long getallocated_size_bytes();

  /**
   * Used cache size
   * @return used cache size
   */
  long getused_size_bytes();

  /**
   * Index size bytes
   * @return index size
   */
  long getindex_size_bytes();

  /**
   * Raw data size (before compression)
   * @return raw data size
   */
  long getraw_size_bytes();

  /**
   * Get cache allocated size ratio
   * @return ratio
   */
  double getallocated_size_ratio();

  /**
   * Get cache used size ratio
   * @return used size ratio
   */
  double getused_size_ratio();

  /**
   * Get cache active data set size (estimated)
   * @return active data set size
   */
  long getactive_dataset_size();

  /**
   * Get active data set size ratio relative to used storage size
   * @return active data set ratio
   */
  double getactive_dataset_size_ratio();

  /**
   * Get number of items in the cache
   * @return number of items
   */
  long getitems_total();

  /**
   * Get number of active items
   * @return number of active items
   */
  long getitems_active();

  /**
   * Cache total number of put operations (including victim cache)
   * @return total puts
   */
  long gettotal_puts();

  /**
   * Cache total number of insert operations (including victim cache)
   * @return total inserts
   */
  long gettotal_inserts();

  /**
   * Cache total number of update operations (including victim cache)
   * @return total updates
   */
  long gettotal_updates();

  /**
   * Cache total number of delete operations (including victim cache)
   * @return total deletes
   */
  long gettotal_deletes();

  /**
   * Cache total GET operations
   * @return total gets
   */
  long gettotal_gets();

  /**
   * Get cache total hits
   * @return cache total hits
   */
  long gettotal_hits();

  /**
   * Is cache hybrid
   * @return true or false
   */
  boolean getis_hybrid();

  /**
   * For hybrid caches
   * @return victim cache name
   */
  String getvictim_cache_name();

  /**
   * Get cache get operations
   * @return number of get operations
   */
  long getgets();

  /**
   * Get cache writes operations
   * @return number of writes
   */
  long getwrites();

  /**
   * Hit ratio
   * @return hit ratio
   */
  double gethit_ratio();

  /**
   * Overall hit ratio including victim cache
   * @return overall hit ratio
   */
  double getoverall_hit_ratio();

  /**
   * 
   * Get total bytes written (including GC)
   * @return total bytes written
   */
  long gettotal_written_bytes();

  /**
   * Average write rate in MB/sec including GC
   * @return write rate
   */
  double gettotal_avg_write_rate();

  /**
   * Get cache bytes written so far
   * @return cache bytes written
   */
  long getwritten_bytes();

  /**
   * Get cache total bytes read
   * @return total bytes read
   */
  long getread_bytes();

  /**
   * Get overall bytes read including victim cache
   * @return overall bytes read
   */
  long getoverall_read_bytes();

  /**
   * Get cache average read rate in MB/s
   * @return average read rate
   */
  double getavg_read_rate_mbps();

  /**
   * Get cache overall read rate (including victim cache)
   * @return overall average read rate
   */
  double getoverall_avg_read_rate();

  /**
   * Average write rate by cache in MB/sec (formatted string)
   * @return write rate
   */
  double getavg_write_rate_mbps();

  /**
   * Get total bytes written (including GC)
   * @return total bytes written by GC
   */
  long getgc_total_written_bytes();

  /**
   * Average write rate in MB/sec including GC
   * @return write rate
   */
  double getgc_avg_write_rate_mbps();

  /**
   * Number of runs of GC
   * @return number of runs
   */
  long getgc_number_of_runs();

  /**
   * Get GC bytes scanned
   * @return bytes scanned
   */
  long getgc_scanned_bytes();

  /**
   * Get GC bytes freed
   * @return bytes freed
   */
  long getgc_freed_bytes();

  /**
   * Get IO average read operation duration in microseconds
   * @return duration
   */
  long getio_avg_read_duration_us();

  /**
   * Get IO average read size bytes
   * @return average read size
   */
  long getio_avg_read_size_bytes();

  /**************************************
   * Compression
   *************************************/

  /**
   * Is compression enabled
   * @return true or false
   */
  boolean getcompression_enabled();

  /**
   * Get compression codec name
   * @return codec name
   */
  String getcompression_codec();

  /**
   * Get compression level
   * @return compression level
   */
  int getcompression_level();

  /**
   * Is compression dictionary enabled
   * @return true or false
   */
  boolean getcompression_dictionary_enabled();

  /**
   * Get compression dictionary size
   * @return dictionary size
   */
  int getcompression_dictionary_size_bytes();

  /**
   * Get compressed data size bytes
   * @return compressed data size
   */
  long getcompressed_size_bytes();

  /**
   * Compression block size
   * @return compression block size
   */
  int getcompression_block_size_bytes();

  /**
   * Get compression keys enabled
   * @return true or false
   */
  boolean getcompression_keys_enabled();

  /**
   * Get compression ratio
   * @return compression ratio
   */
  double getcompression_ratio();
}