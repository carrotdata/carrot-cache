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
package com.carrot.cache.util;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import com.carrot.cache.Cache;
import com.carrot.cache.controllers.AdmissionController;
import com.carrot.cache.controllers.BaseThroughputController;
import com.carrot.cache.controllers.RecyclingSelector;
import com.carrot.cache.controllers.ThroughputController;
import com.carrot.cache.eviction.EvictionPolicy;
import com.carrot.cache.eviction.SLRUEvictionPolicy;
import com.carrot.cache.index.AQIndexFormat;
import com.carrot.cache.index.IndexFormat;
import com.carrot.cache.index.MQIndexFormat;

public class CacheConfig {

  /* List of all caches logical names, comma-separated*/
  public final static String CACHES_NAME_LIST_KEY = "caches.name.list";
  
  /* By default we have only one cache */
  public final static String DEFAULT_CACHES_NAME_LIST ="cache"; // only one cache
  
  /* Caches types ('offheap', 'file' only supported), comma-separated */
  public final static String CACHES_TYPES_LIST_KEY ="caches.types.list";
  
  /* By default cache type is offheap */
  public final static String DEFAULT_CACHES_TYPES_LIST = "offheap";
  
  /* 
   * Cache victim name. If cache name is C1, then to lookup for its victim name 
   * we must request 'C1.victim.name' property value 
   */
  public final static String CACHE_VICTIM_NAME_KEY = "victim.name";
  
  
  /* File name for cache snapshot data */
  public final static String CACHE_SNAPSHOT_NAME = "cache.data";
  
  /* File name for admission controller snapshot data */
  public final static String ADMISSION_CONTROLLER_SNAPSHOT_NAME = "ac.data";
  
  /* File name for throughput controller snapshot data */
  public final static String THROUGHPUT_CONTROLLER_SNAPSHOT_NAME = "tc.data";
  
  /* File name for recycling selector snapshot data */
  public final static String RECYCLING_SELECTOR_SNAPSHOT_NAME = "rc.data";
  
  /* File name for admission queue snapshot data */
  public final static String ADMISSION_QUEUE_SNAPSHOT_NAME = "aq.data";

  /* File name for scavenger statistics snapshot data */
  public final static String SCAVENGER_STATS_SNAPSHOT_NAME = "scav.data";
  
  /* File name for cache engine snapshot data */
  public final static String CACHE_ENGINE_SNAPSHOT_NAME = "engine.data";
  
  /* Default cache configuration file name */
  public final static String DEFAULT_CACHE_CONFIG_FILE_NAME = "cache.conf";
  
  /* Default cache configuration directory name - the only directory for all caches */
  public final static String DEFAULT_CACHE_CONFIG_DIR_NAME = "conf";
  
  /* Cache snapshot directory - where to save index and statistics info */
  public final static String CACHE_SNAPSHOT_DIR_NAME_KEY = "snapshot.dir.name";
  
  /* Default cache snapshot directory name */
  public final static String DEFAULT_CACHE_SNAPSHOT_DIR_NAME = "snapshot";
  
  /* Cache data directory - where to save cached data */
  public final static String CACHE_DATA_DIR_NAME_KEY = "data.dir.name";
  
  /* Default cache data directory name */
  public final static String DEFAULT_CACHE_DATA_DIR_NAME = "data";
  
  //TODO - location for data and system snapshots
  
  /* For in RAM cache  - segment size */
  public static final String CACHE_SEGMENT_SIZE_KEY = "segment.size";
  
  /* For in RAM cache  - maximum memory limit to use for cache */
  public static final String CACHE_MAXIMUM_SIZE_KEY = "maximum.size";
  
  /* When to start GC (garbage collection) - size of the cache as a fraction of the maximum cache size */
  public static final String SCAVENGER_START_RUN_RATIO_KEY = "scavenger.start.ratio";

  /* When to stop GC (garbage collection) - size of the cache as a fraction of the maximum cache size */
  public static final String SCAVENGER_STOP_RUN_RATIO_KEY = "scavenger.stop.ratio";
  
  /* Discard cached entry if it in this lower percentile - start value */
  public static final String SCAVENGER_DUMP_ENTRY_BELOW_START_KEY = "scavenger.dump.entry.below.start";

  /* Discard cached entry if it in this lower percentile - stop value (maximum) */
  public static final String SCAVENGER_DUMP_ENTRY_BELOW_STOP_KEY = "scavenger.dump.entry.below.stop";
  
  /* Number of admission ranks ( default - 8) */
  public static final String CACHE_ADMISSION_NUMBER_RANKS_KEY = "cache.admission.number.ranks";
  
  /* Number of popularity ranks ( default - 8) */
  public static final String CACHE_POPULARITY_NUMBER_RANKS_KEY = "cache.popularity.number.ranks";
  
  /* New item insertion point for SLRU (segment number 1- based)*/
  public static final String SLRU_CACHE_INSERT_POINT_KEY = "eviction.slru.insert.point";

  /* Number of segments in SLRU eviction policy */
  public static final String SLRU_NUMBER_SEGMENTS_KEY = "eviction.slru.number.segments";
  
  /* Admission Queue start size in fraction of a full cache size */
  public static final String ADMISSION_QUEUE_START_SIZE_KEY = "admission.queue.start.size";
  
  /* Admission Queue minimum size in fraction of a full cache size */
  public static final String ADMISSION_QUEUE_MIN_SIZE_KEY = "admission.queue.min.size";
  
  /* Admission Queue maximum size in fraction of a full cache size */
  public static final String ADMISSION_QUEUE_MAX_SIZE_KEY = "admission.queue.max.size";
  
  /* Readmission evicted item to AQ minimum hit count threshold */
  public static final String READMISSION_HIT_COUNT_MIN_KEY = "readmission.hit.count.min";
  
  /* Cumulative average write rate limit  (bytes/sec) */
  public static final String CACHE_WRITE_RATE_LIMIT_KEY = "write.rate.limit";
  
  /* Incremental index rehashing */
  public static final String INCREMENTRAL_INDEX_REHASHING_KEY = "incremental.index.rehashing";
  
  /*
   * Some file systems : ext4, xfs, APFS etc supports sparse files and so called 
   * "hole punching" - discarding  regions of files. We use different algorithm of compaction when file system 
   *  supports these features. Default: false.
   */
  public static final String SPARSE_FILES_SUPPORT_KEY = "sparse.files.support";
  
  /*
   * Index starting number of slots power of 2 - L ( N = 2**L) N - number of slots 
   */
  public static final String START_INDEX_NUMBER_OF_SLOTS_POWER_KEY = "index.slots.power";
  
  /*
   * Cache write throughput check interval key  
   */
  public static final String THROUGHPUT_CHECK_INTERVAL_MS_KEY = "throughput.check.interval.ms"; 
  
  /*
   * Cache write throughput controller tolerance limit
   */
  public static final String THROUGHPUT_CONTROLLER_TOLERANCE_LIMIT_KEY = "throughput.tolerance.limit";
  
  /*
   *  Throughput controller number of adjustment steps
   */
  public static final String THROUGHPUT_CONTROLLER_ADJUSTMENT_STEPS_KEY = "throughput.adjustment.steps";
  
  /**
   * Cache write rejection threshold - when cache size exceeds this fraction of a maximum cache size -
   * all new writes will be rejected until cache size decreases below this threshold
   */
  
  public static final String CACHE_WRITE_REJECTION_THRESHOLD_KEY = "write.rejection.limit";
  
  /**
   * Does index support memory embedding
   */
  public static final String INDEX_DATA_EMBEDDED_KEY = "index.data.embedded";
  
  /**
   * Maximum data size to embed   
   **/
  public static final String INDEX_DATA_EMBEDDED_SIZE_KEY = "index.data.embedded.size";
  
  /* Class name for main queue index format implementation */
  public static final String INDEX_FORMAT_MAIN_QUEUE_IMPL_KEY = "index.format.main.queue.impl";
  
  /* Class name for admission queue index format implementation */
  public static final String INDEX_FORMAT_ADMISSION_QUEUE_IMPL_KEY = "index.format.admission.queue.impl";

  /* Class name for cache eviction policy implementation */
  public static final String CACHE_EVICTION_POLICY_IMPL_KEY = "cache.eviction.policy.impl";
  
  /* Class name for cache admission controller implementation */
  public static final String CACHE_ADMISSION_CONTROLLER_IMPL_KEY = "cache.admission.controller.impl";
  
  /* Class name for cache admission controller implementation */
  public static final String CACHE_THROUGHPUT_CONTROLLER_IMPL_KEY = "cache.throughput.controller.impl";
  
  /* Class name for cache admission controller implementation */
  public static final String CACHE_RECYCLING_SELECTOR_IMPL_KEY = "cache.recycling.selector.impl";
  
  /* Defaults section */
  
  public static final long DEFAULT_CACHE_SEGMENT_SIZE = 256 * 1024 * 1024;

  public static final long DEFAULT_CACHE_MAXIMUM_SIZE = 0; // Unlimited
  
  public static final double DEFAULT_SCAVENGER_START_RUN_RATIO = 0.95;

  public static final double DEFAULT_SCAVENGER_STOP_RUN_RATIO =
      0.9; // Set it to 0 for continuous scavenger run

  /**
   * During scavenger run, all expired entries as well as entries with popularity below this config
   * value will be dumped, all others will be rewritten back to a cache. THis is the start value
   */
  public static final double DEFAULT_SCAVENGER_DUMP_ENTRY_BELOW_START = 0.1;
  
  /**
   * During scavenger run, all expired entries as well as entries with popularity below this config
   * value will be dumped, all others will be rewritten back to a cache. This is the stop value
   */
  public static final double DEFAULT_SCAVENGER_DUMP_ENTRY_BELOW_STOP = 0.5;
  
  /**
   * Limits write speed during scavenger run to 0.9 of scavenger cleaning memory rate Suppose
   * Scavenger frees memory with a rate of 900MB/sec. Incoming cache write requests will be limited
   * to a 900*0.9 MS/sec = 810MB/sec. This is necessary to guarantee that we won't get OOM errors
   * (or swapping)
   */
  public static final double DEFAULT_WRITES_LIMIT_RATIO = 0.9;

  /** Default number of admission ranks for data segments in the cache */
  public static final int DEFAULT_CACHE_ADMISSION_NUMBER_RANKS = 8;

  /** Default number of popularity ranks for data items in the cache */
  public static final int DEFAULT_CACHE_POPULARITY_NUMBER_RANKS = 8;
  
  /** Number of segments in segmented-LRU cache */
  public static final int DEFAULT_SLRU_NUMBER_SEGMENTS = 8;
  
  /** Default insertion point for SLRU - head of nth segments - 4*/
  public static final int DEFAULT_SLRU_CACHE_INSERT_POINT = 4;
  
  /** Default segment size */
  public static final long DEFAULT_CACHE_DISK_SEGMENT_SIZE = 256 * 1024 * 1024; // 64MB

  /* Default re-admission hit count min*/
  public final static int DEFAULT_READMISSION_HIT_COUNT_MIN = 1;
  
  /* Default sparse files support */
  public final static boolean DEFAULT_SPARSE_FILES_SUPPORT = false;
  
  /* Default AQ (admission queue) start size a fraction of a cache size */
  public final static long DEFAULT_ADMISSION_QUEUE_START_SIZE = 200000;
  
  /* Default AQ (admission queue) minimum size a fraction of a cache size */
  public final static long DEFAULT_ADMISSION_QUEUE_MIN_SIZE = 100000;
  
  /* Default AQ (admission queue) maximum size a fraction of a cache size */
  public final static long DEFAULT_ADMISSION_QUEUE_MAX_SIZE = 1000000;

  /* Default disk limit - 0 unlimited*/
  public static final long DEFAULT_DISK_LIMIT = 0; // Unlimited
  
  /* Default size as a power indicator of 2 for index N = 2**10 - start */ 
  public static final int DEFAULT_START_INDEX_NUMBER_OF_SLOTS_POWER = 10;
  
  /* Default incremental index rehashing */
  public static final boolean DEFAULT_INCREMENTRAL_INDEX_REHASHING = false;

  /* Default throughput check interval - 1 hour*/
  public static final long DEFAULT_THROUGHPUT_CHECK_INTERVAL_MS = 3600000;// 1h
  
  /* Default throughput controller tolerance limit*/
  public static final double DEFAULT_THROUGHPUT_CONTROLLER_TOLERANCE_LIMIT = 0.05;
  
  /* Cache write rejection threshold */
  public static final double DEFAULT_CACHE_WRITE_REJECTION_THRESHOLD = 0.99;
  
  /* Default cache write rate limit */
  public static final long DEFAULT_CACHE_WRITE_RATE_LIMIT = 50 * 1024 * 1024;
  
  /* Default number of adjustment steps */
  public static final int DEFAULT_THROUGHPUT_CONTROLLER_ADJUSTMENT_STEPS = 8;
  
  /* Default for index data embedding */
  public final static boolean DEFAULT_INDEX_DATA_EMBEDDED = false;
  
  /* Default index data embedding maximum size */
  public final static int DEFAULT_INDEX_DATA_EMBEDDED_SIZE = 100;
  
  public final static String DEFAULT_CACHE_RECYCLING_SELECTOR_IMPL = 
      "com.carrot.cache.controllers.LowestRankRecyclingSelector";

  
  // Statics
  static CacheConfig instance;

  public static CacheConfig getInstance() {
    if (instance != null) {
      return instance;
    }
    synchronized (CacheConfig.class) {
      if (instance != null) {
        return instance;
      }
      instance = new CacheConfig();
    }
    return instance;
  }

  public static CacheConfig getInstance(String file) throws IOException {
    if (instance != null) {
      return instance;
    }
    synchronized (CacheConfig.class) {
      if (instance != null) {
        return instance;
      }
      instance = new CacheConfig(file);
    }
    return instance;
  }

  // Instance
  Properties props;

  /** Default constructor */
  private CacheConfig() {
    props = new Properties();
  }

  /**
   * Constructor from file
   *
   * @param file configuration file
   * @throws IOException
   */
  private CacheConfig(String file) throws IOException {
    this();
    if (file != null) {
      FileInputStream fis = new FileInputStream(file);
      props.load(fis);
      fis.close();
    }
  }

  /**
   * Constructor from properties
   *
   * @param props
   */
  CacheConfig(Properties props) {
    this.props = props;
  }

  public double getDoubleProperty(String name, double defValue) {
    String value = props.getProperty(name);
    if (value == null) return defValue;
    try {
      return Double.parseDouble(value);
    } catch (NumberFormatException e) {
      // TODO log error
      e.printStackTrace();
    }
    return defValue;
  }

  public long getLongProperty(String name, long defValue) {
    String value = props.getProperty(name);
    if (value == null) return defValue;
    try {
      return Long.parseLong(value);
    } catch (NumberFormatException e) {
      // TODO log error
      e.printStackTrace();
    }
    return defValue;
  }

  public String getProperty(String name, String defValue) {
    String value = props.getProperty(name);
    if (value == null) return defValue;
    return value;
  }
  
  public boolean getBooleanProperty(String name, boolean defValue) {
    String value = props.getProperty(name);
    if (value == null) return defValue;
    try {
      return Boolean.parseBoolean(value);
    } catch (NumberFormatException e) {
      // TODO log error
      e.printStackTrace();
    }
    return defValue;
  }
  
  /****************************************************************
   * GETTERS SECTION
   */

  /**
   * Get maximum size for a cache
   * @param cacheName cache name
   * @return memory limit (0 - no limit)
   */
  public long getCacheMaximumSize(String cacheName) {
    String value = props.getProperty(cacheName + "."+ CACHE_MAXIMUM_SIZE_KEY);
    if (value != null) {
      return Long.parseLong(value);
    }
    return getLongProperty(CACHE_MAXIMUM_SIZE_KEY, DEFAULT_CACHE_MAXIMUM_SIZE);
  }

  /**
   * Get segment size for a cache
   * @param cacheName cache name
   * @return segment size
   */
  public long getCacheSegmentSize(String cacheName) {
    String value = props.getProperty(cacheName + "."+ CACHE_SEGMENT_SIZE_KEY);
    if (value != null) {
      return Long.parseLong(value);
    }
    return getLongProperty(CACHE_SEGMENT_SIZE_KEY, DEFAULT_CACHE_SEGMENT_SIZE);
  }

  /**
   * Gets scavenger start memory ratio for a cache
   * @param cache name
   * @return start ratio
   */
  public double getScavengerStartMemoryRatio(String cacheName) {
    String value = props.getProperty(cacheName + "."+ SCAVENGER_START_RUN_RATIO_KEY);
    if (value != null) {
      return Double.parseDouble(value);
    }
    return getDoubleProperty(SCAVENGER_START_RUN_RATIO_KEY, DEFAULT_SCAVENGER_START_RUN_RATIO);
  }

  /**
   * Gets scavenger stop memory ratio
   * @param cacheName cache name
   * @return stop ratio
   */
  public double getScavengerStopMemoryRatio(String cacheName) {
    String value = props.getProperty(cacheName + "."+ SCAVENGER_STOP_RUN_RATIO_KEY);
    if (value != null) {
      return Double.parseDouble(value);
    }
    return getDoubleProperty(SCAVENGER_STOP_RUN_RATIO_KEY, DEFAULT_SCAVENGER_STOP_RUN_RATIO);
  }

  /**
   * Gets scavenger dump entry below - start
   * @param cacheName cache name
   * @return dump entry below ratio
   */
  public double getScavengerDumpEntryBelowStart(String cacheName) {
    String value = props.getProperty(cacheName + "."+ SCAVENGER_DUMP_ENTRY_BELOW_START_KEY);
    if (value != null) {
      return Double.parseDouble(value);
    }
    return getDoubleProperty(SCAVENGER_DUMP_ENTRY_BELOW_START_KEY, DEFAULT_SCAVENGER_DUMP_ENTRY_BELOW_START);
  }

  /**
   * Gets scavenger dump entry below - stop
   * @param cacheName cache name
   * @return dump entry below ratio
   */
  public double getScavengerDumpEntryBelowStop(String cacheName) {
    String value = props.getProperty(cacheName + "."+ SCAVENGER_DUMP_ENTRY_BELOW_STOP_KEY);
    if (value != null) {
      return Double.parseDouble(value);
    }
    return getDoubleProperty(SCAVENGER_DUMP_ENTRY_BELOW_STOP_KEY, DEFAULT_SCAVENGER_DUMP_ENTRY_BELOW_STOP);
  }

  /**
   * Get number of admission ranks for a cache
   * @param cacheName cache name
   * @return number of admission ranks
   */
  public int getNumberOfAdmissionRanks(String cacheName) {
    String value = props.getProperty(cacheName + "."+ CACHE_ADMISSION_NUMBER_RANKS_KEY);
    if (value != null) {
      return (int) Long.parseLong(value);
    }
    return (int) getLongProperty(CACHE_ADMISSION_NUMBER_RANKS_KEY, DEFAULT_CACHE_ADMISSION_NUMBER_RANKS);
  }
  
  /**
   * Get number of item popularity ranks for a cache
   * @param cacheName cache name
   * @return number of popularity ranks
   */
  public int getNumberOfPopularityRanks(String cacheName) {
    String value = props.getProperty(cacheName + "."+ CACHE_POPULARITY_NUMBER_RANKS_KEY);
    if (value != null) {
      return (int) Long.parseLong(value);
    }
    return (int) getLongProperty(CACHE_POPULARITY_NUMBER_RANKS_KEY, DEFAULT_CACHE_POPULARITY_NUMBER_RANKS);
  }
  
  /**
   * Get SLRU  insertion point (segment number to insert into the head of)
   * @param cacheName cache name
   * @return SLRU insertion point (segment number between 0 and getSLRUNumberOfSegments)
   */
  public int getSLRUInsertionPoint(String cacheName) {
    String value = props.getProperty(cacheName + "."+ SLRU_CACHE_INSERT_POINT_KEY);
    if (value != null) {
      return (int) Long.parseLong(value);
    }
    return (int) getLongProperty(SLRU_CACHE_INSERT_POINT_KEY, DEFAULT_SLRU_CACHE_INSERT_POINT);
  }
  
  
  /**
   * Get SLRU  number of segments 
   * @param cacheName cache name
   * @return SLRU number of segments
   */
  public int getSLRUNumberOfSegments(String cacheName) {
    String value = props.getProperty(cacheName + "."+ SLRU_NUMBER_SEGMENTS_KEY);
    if (value != null) {
      return (int) Long.parseLong(value);
    }
    return (int) getLongProperty(SLRU_NUMBER_SEGMENTS_KEY, DEFAULT_SLRU_NUMBER_SEGMENTS);
  }
  
  /**
   * Get sparse files support for a cache (only for 'file' type caches)
   * @param cacheName cache name
   * @return true / false
   */
  public boolean getSparseFilesSupport(String cacheName) {
    String value = props.getProperty(cacheName + "."+ SPARSE_FILES_SUPPORT_KEY);
    if (value != null) {
      return Boolean.parseBoolean(value);
    }
    return getBooleanProperty(SPARSE_FILES_SUPPORT_KEY, DEFAULT_SPARSE_FILES_SUPPORT);
  }
  
 
  /**
   * Get start index size for a cache
   * @param cacheName cache name
   * @return start index size for a given cache name
   */
  public int getStartIndexNumberOfSlotsPower (String cacheName) {
    String value = props.getProperty(cacheName + "."+ START_INDEX_NUMBER_OF_SLOTS_POWER_KEY);
    if (value != null) {
      return (int) Long.parseLong(value);
    }
    return (int) getLongProperty(START_INDEX_NUMBER_OF_SLOTS_POWER_KEY, 
      DEFAULT_START_INDEX_NUMBER_OF_SLOTS_POWER);
  }
  
  /**
   * Get incremental index rehashing for a cache
   * @param cacheName cache name
   * @return true or false
   */
  public boolean getIncrementalIndexRehashing (String cacheName) {
    String value = props.getProperty(cacheName + "."+ INCREMENTRAL_INDEX_REHASHING_KEY);
    if (value != null) {
      return Boolean.parseBoolean(value);
    }
    return getBooleanProperty(INCREMENTRAL_INDEX_REHASHING_KEY, DEFAULT_INCREMENTRAL_INDEX_REHASHING);
  }
  
  /**
   * Get snapshot directory location for a cache
   * @param cacheName cache name
   * @return snapshot directory name for a given cache name
   */
  public String getSnapshotDir(String cacheName) {
    String value = props.getProperty(cacheName + "."+ CACHE_SNAPSHOT_DIR_NAME_KEY);
    if (value != null) {
      return value;
    }
    return props.getProperty(CACHE_SNAPSHOT_DIR_NAME_KEY, DEFAULT_CACHE_SNAPSHOT_DIR_NAME);
  }
  
  /**
   * Get data directory location for a cache
   * @param cacheName cache name
   * @return data directory name for a given cache  name
   */
  public String getDataDir(String cacheName) {
    String value = props.getProperty(cacheName + "."+ CACHE_DATA_DIR_NAME_KEY);
    if (value != null) {
      return value;
    }
    return props.getProperty(CACHE_DATA_DIR_NAME_KEY, DEFAULT_CACHE_DATA_DIR_NAME);
  }
  
  /**
   * Get admission queue start size for a given cache name
   * @param cacheName cache name
   * @return AQ start size in cached items for a given cache name
   */
  public long getAdmissionQueueStartSize(String cacheName) {
    String value = props.getProperty(cacheName + "."+ ADMISSION_QUEUE_START_SIZE_KEY);
    if (value != null) {
      return (int) Long.parseLong(value);
    }
    return getLongProperty(ADMISSION_QUEUE_START_SIZE_KEY, DEFAULT_ADMISSION_QUEUE_START_SIZE);
  }
  
  /**
   * Get admission queue minimum size for a given cache name
   * @param cacheName cache name
   * @return AQ minimum size in cached items for a given cache name
   */
  public long getAdmissionQueueMinSize(String cacheName) {
    String value = props.getProperty(cacheName + "."+ ADMISSION_QUEUE_MIN_SIZE_KEY);
    if (value != null) {
      return (int) Long.parseLong(value);
    }
    return getLongProperty(ADMISSION_QUEUE_MIN_SIZE_KEY, DEFAULT_ADMISSION_QUEUE_MIN_SIZE);
  }
  
  /**
   * Get admission queue starts size for a given cache name
   * @param cacheName cache name
   * @return AQ maximum size in cached items for a given cache name
   */
  public long getAdmissionQueueMaxSize(String cacheName) {
    String value = props.getProperty(cacheName + "."+ ADMISSION_QUEUE_MAX_SIZE_KEY);
    if (value != null) {
      return (int) Long.parseLong(value);
    }
    return getLongProperty(ADMISSION_QUEUE_MAX_SIZE_KEY, DEFAULT_ADMISSION_QUEUE_MAX_SIZE);
  }
  
  /**
   * Get throughput check interval for a given cache name
   * @param cacheName cache name
   * @return interval in ms for a given cache name
   */
  public long getThroughputCheckInterval(String cacheName) {
    String value = props.getProperty(cacheName + "."+ THROUGHPUT_CHECK_INTERVAL_MS_KEY);
    if (value != null) {
      return (int) Long.parseLong(value);
    }
    return getLongProperty(THROUGHPUT_CHECK_INTERVAL_MS_KEY, DEFAULT_THROUGHPUT_CHECK_INTERVAL_MS);
  }
  
  /**
   * Get throughput controller tolerance limit for a given cache name
   * @param cacheName cache name
   * @return limit for a given cache name
   */
  public double getThroughputToleranceLimit(String cacheName) {
    String value = props.getProperty(cacheName + "."+ THROUGHPUT_CONTROLLER_TOLERANCE_LIMIT_KEY);
    if (value != null) {
      return Double.parseDouble(value);
    }
    return getDoubleProperty(THROUGHPUT_CONTROLLER_TOLERANCE_LIMIT_KEY, DEFAULT_THROUGHPUT_CONTROLLER_TOLERANCE_LIMIT);
  }
  
  /**
   * Get cache write rejection threshold for a given cache name
   * @param cacheName cache name
   * @return threshold for a given cache name
   */
  public double getCacheWriteRejectionThreshold(String cacheName) {
    String value = props.getProperty(cacheName + "."+ CACHE_WRITE_REJECTION_THRESHOLD_KEY);
    if (value != null) {
      return Double.parseDouble(value);
    }
    return getDoubleProperty(CACHE_WRITE_REJECTION_THRESHOLD_KEY, DEFAULT_CACHE_WRITE_REJECTION_THRESHOLD);
  }
  
  /**
   * Get cache write rate limit for a given cache name
   * @param cacheName cache name
   * @return cache write rate limit for a given cache name
   */
  public long getCacheWriteLimit(String cacheName) {
    String value = props.getProperty(cacheName + "."+ CACHE_WRITE_RATE_LIMIT_KEY);
    if (value != null) {
      return Long.parseLong(value);
    }
    return getLongProperty(CACHE_WRITE_RATE_LIMIT_KEY, DEFAULT_CACHE_WRITE_RATE_LIMIT);
  }
  
  /**
   * Get cache names list
   * @return cache names list
   */
  public String[] getCacheNames() {
    String s = props.getProperty(CACHES_NAME_LIST_KEY, DEFAULT_CACHES_NAME_LIST);
    return s.split(",");
  }
  
  /**
   * Get cache types list
   * @return cache types list
   */
  public String[] getCacheTypes() {
    String s = props.getProperty(CACHES_TYPES_LIST_KEY, DEFAULT_CACHES_TYPES_LIST);
    return s.split(",");
  }
  
  /**
   * Get victim cache name for cache
   * @param cacheName cache name
   * @return victim cache name
   */
  public String getVictimCacheName(String cacheName) {
    return props.getProperty(cacheName + "."+ CACHE_VICTIM_NAME_KEY);
  }
  
  /**
   * Get cache write rate limit for a given cache name
   * @param cacheName cache name
   * @return cache write rate limit for a given cache name
   */
  public int getThrougputControllerNumberOfAdjustmentSteps(String cacheName) {
    String value = props.getProperty(cacheName + "."+ THROUGHPUT_CONTROLLER_ADJUSTMENT_STEPS_KEY);
    if (value != null) {
      return (int) Long.parseLong(value);
    }
    return (int) getLongProperty(THROUGHPUT_CONTROLLER_ADJUSTMENT_STEPS_KEY, DEFAULT_THROUGHPUT_CONTROLLER_ADJUSTMENT_STEPS);
  }
  
  /**
   * Is index data embedding supported
   * @return true or false
   */
  public boolean isIndexDataEmbeddedSupported() {
    return getBooleanProperty(INDEX_DATA_EMBEDDED_KEY, DEFAULT_INDEX_DATA_EMBEDDED);
  }
  
  /**
   * Get data embedded size
   * @return data embedded size
   */
  public int getIndexDataEmbeddedSize() {
    return (int)getLongProperty(INDEX_DATA_EMBEDDED_SIZE_KEY, DEFAULT_INDEX_DATA_EMBEDDED_SIZE);
  }

  /**
   * Get admission queue index format implementation
   *
   * @throws ClassNotFoundException
   * @throws IllegalAccessException
   * @throws InstantiationException
   */
  public IndexFormat getAdmissionQueueIndexFormat(String cacheName)
      throws ClassNotFoundException, InstantiationException, IllegalAccessException {
    String value = props.getProperty(cacheName + "." + INDEX_FORMAT_ADMISSION_QUEUE_IMPL_KEY);
    if (value == null) {
      value = props.getProperty(INDEX_FORMAT_ADMISSION_QUEUE_IMPL_KEY);
    }
    if (value == null) {
      // default implementation;
      return new AQIndexFormat();
    }
    @SuppressWarnings("unchecked")
    Class<IndexFormat> clz = (Class<IndexFormat>) Class.forName(value);
    IndexFormat instance = clz.newInstance();
    instance.setCacheName(cacheName);
    return instance;
  }
  
  /**
   * Get main queue index format implementation
   *
   * @throws ClassNotFoundException
   * @throws IllegalAccessException
   * @throws InstantiationException
   */
  public IndexFormat getMainQueueIndexFormat(String cacheName)
      throws ClassNotFoundException, InstantiationException, IllegalAccessException {
    String value = props.getProperty(cacheName + "." + INDEX_FORMAT_MAIN_QUEUE_IMPL_KEY);
    if (value == null) {
      value = props.getProperty(INDEX_FORMAT_MAIN_QUEUE_IMPL_KEY);
    }
    if (value == null) {
      // default implementation;
      return new MQIndexFormat();
    }
    @SuppressWarnings("unchecked")
    Class<IndexFormat> clz = (Class<IndexFormat>) Class.forName(value);
    IndexFormat instance = clz.newInstance();
    instance.setCacheName(cacheName);
    return instance;
  }
  
  /**
   * Get cache eviction policy implementation by cache name
   *
   * @throws ClassNotFoundException
   * @throws IllegalAccessException
   * @throws InstantiationException
   */
  public EvictionPolicy getCacheEvictionPolicy(String cacheName)
      throws ClassNotFoundException, InstantiationException, IllegalAccessException {
    String value = props.getProperty(cacheName + "." + CACHE_EVICTION_POLICY_IMPL_KEY);
    if (value == null) {
      value = props.getProperty(CACHE_EVICTION_POLICY_IMPL_KEY);
    }
    if (value == null) {
      // default implementation;
      EvictionPolicy policy = new SLRUEvictionPolicy();
      policy.setCacheName(cacheName);
      return policy;
    }
    @SuppressWarnings("unchecked")
    Class<EvictionPolicy> clz = (Class<EvictionPolicy>) Class.forName(value);
    EvictionPolicy instance = clz.newInstance();
    instance.setCacheName(cacheName);
    return instance;
  }
  
  /**
   * Get cache admission controller implementation by cache name
   *
   * @throws ClassNotFoundException
   * @throws IllegalAccessException
   * @throws InstantiationException
   */
  public AdmissionController getAdmissionController(String cacheName)
      throws ClassNotFoundException, InstantiationException, IllegalAccessException {
    String value = props.getProperty(cacheName + "." + CACHE_ADMISSION_CONTROLLER_IMPL_KEY);
    if (value == null) {
      value = props.getProperty(CACHE_ADMISSION_CONTROLLER_IMPL_KEY);
    }
    if (value == null) {
      // default implementation;
      AdmissionController controller = new Cache.BaseAdmissionController();
      return controller;
    }
    @SuppressWarnings("unchecked")
    Class<AdmissionController> clz = (Class<AdmissionController>) Class.forName(value);
    AdmissionController instance = clz.newInstance();
    return instance;
  }
  
  /**
   * Get cache throughput controller implementation by cache name
   *
   * @throws ClassNotFoundException
   * @throws IllegalAccessException
   * @throws InstantiationException
   */
  public ThroughputController getThroughputController(String cacheName)
      throws ClassNotFoundException, InstantiationException, IllegalAccessException {
    String value = props.getProperty(cacheName + "." + CACHE_THROUGHPUT_CONTROLLER_IMPL_KEY);
    if (value == null) {
      value = props.getProperty(CACHE_THROUGHPUT_CONTROLLER_IMPL_KEY);
    }
    if (value == null) {
      // default implementation;
      ThroughputController controller = new BaseThroughputController();
      return controller;
    }
    @SuppressWarnings("unchecked")
    Class<ThroughputController> clz = (Class<ThroughputController>) Class.forName(value);
    ThroughputController instance = clz.newInstance();
    return instance;
  }
  
  /**
   * Get Scavenger recycling selector implementation by cache name
   *
   * @throws ClassNotFoundException
   * @throws IllegalAccessException
   * @throws InstantiationException
   */
  public RecyclingSelector getRecyclingSelector(String cacheName)
      throws ClassNotFoundException, InstantiationException, IllegalAccessException {
    String value = props.getProperty(cacheName + "." + CACHE_RECYCLING_SELECTOR_IMPL_KEY);
    if (value == null) {
      value = props.getProperty(CACHE_RECYCLING_SELECTOR_IMPL_KEY, DEFAULT_CACHE_RECYCLING_SELECTOR_IMPL);
    }
    
    @SuppressWarnings("unchecked")
    Class<RecyclingSelector> clz = (Class<RecyclingSelector>) Class.forName(value);
    RecyclingSelector instance = clz.newInstance();
    return instance;
  }
}
