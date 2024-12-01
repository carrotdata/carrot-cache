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
package com.carrotdata.cache.util;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotdata.cache.controllers.AdmissionController;
import com.carrotdata.cache.controllers.PromotionController;
import com.carrotdata.cache.controllers.RecyclingSelector;
import com.carrotdata.cache.controllers.ThroughputController;
import com.carrotdata.cache.eviction.EvictionPolicy;
import com.carrotdata.cache.eviction.SLRUEvictionPolicy;
import com.carrotdata.cache.expire.ExpireSupport;
import com.carrotdata.cache.index.AQIndexFormat;
import com.carrotdata.cache.index.BaseIndexFormat;
import com.carrotdata.cache.index.IndexFormat;
import com.carrotdata.cache.io.DataReader;
import com.carrotdata.cache.io.DataWriter;

@SuppressWarnings("deprecation")
public class CacheConfig {

  public static final String CHARS_TO_REMOVE_REGEX = "[_,]";

  static class EnvProperties extends Properties {

    private static final long serialVersionUID = 1L;

    public EnvProperties() {
      super();
    }

    public EnvProperties(Properties p) {
      super(p);
    }

    @Override
    public String getProperty(String name) {
      String v = System.getenv(name);
      if (v != null) return v;
      return super.getProperty(name);
    }
  }

  private static final Logger LOG = LoggerFactory.getLogger(CacheConfig.class);

  /** List of all caches logical names, comma-separated */
  public final static String CACHES_NAME_LIST_KEY = "cache.names";

  /** By default we have only one cache */
  public final static String DEFAULT_CACHES_NAME_LIST = ""; // only one cache

  /** Caches types ('memory', 'file' only supported), comma-separated */
  public final static String CACHES_TYPES_LIST_KEY = "cache.types";

  /** By default cache type is memory */
  public final static String DEFAULT_CACHES_TYPES_LIST = "";

  /**
   * Cache victim name. If cache name is C1, then to lookup for its victim name we must request
   * 'C1.victim.name' property value
   */
  public final static String CACHE_VICTIM_NAME_KEY = "victim.name";

  /** File name for cache snapshot data */
  public final static String CACHE_SNAPSHOT_NAME = "cache.data";

  /** File name for admission controller snapshot data */
  public final static String ADMISSION_CONTROLLER_SNAPSHOT_NAME = "ac.data";

  /** File name for throughput controller snapshot data */
  public final static String THROUGHPUT_CONTROLLER_SNAPSHOT_NAME = "tc.data";

  /** File name for recycling selector snapshot data */
  public final static String RECYCLING_SELECTOR_SNAPSHOT_NAME = "rc.data";

  /** File name for admission queue snapshot data */
  public final static String ADMISSION_QUEUE_SNAPSHOT_NAME = "aq.data";

  /** File name for scavenger statistics snapshot data */
  public final static String SCAVENGER_STATS_SNAPSHOT_NAME = "scav.data";

  /** File name for cache engine snapshot data */
  public final static String CACHE_ENGINE_SNAPSHOT_NAME = "engine.data";

  /** File name for cache engine snapshot data */
  public final static String CACHE_INDEX_SNAPSHOT_NAME = "index.data";
  
  /** Dictionary directory name (there can be multiple dictionary versions) */
  public final static String DICTIONARY_DIR_NAME = "dict";

  /** Compression dictionary file name extension */
  public final static String DICTIONARY_FILE_EXT = "dict";

  /** Default cache configuration file name */
  public final static String DEFAULT_CACHE_CONFIG_FILE_NAME = "cache.conf";

  /** Default cache configuration directory name - the only directory for all caches */
  public final static String DEFAULT_CACHE_CONFIG_DIR_NAME = "conf";

  /** Cache root directory - where to save cached data */
  public final static String CACHE_DATA_DIR_PATHS_KEY = "data.dir.paths";

  /** Default cache root directory path */
  public final static String DEFAULT_CACHE_DIR_PATHS = "." + File.separator + "data";

  /** Data segment size */
  public static final String CACHE_SEGMENT_SIZE_KEY = "data.segment.size";

  /** Maximum storage limit to use for cache */
  public static final String CACHE_MAXIMUM_SIZE_KEY = "storage.size.max";

  /**
   * When to start GC (garbage collection) - size of the cache as a fraction of the maximum cache
   * size
   */
  public static final String SCAVENGER_START_RUN_RATIO_KEY = "scavenger.ratio.start";

  /**
   * When to stop GC (garbage collection) - size of the cache as a fraction of the maximum cache
   * size
   */
  public static final String SCAVENGER_STOP_RUN_RATIO_KEY = "scavenger.ratio.stop";

  /** Discard cached entry if it in this lower percentile - start value */
  public static final String SCAVENGER_DUMP_ENTRY_BELOW_MIN_KEY = "scavenger.dump.entry.below.min";

  /** Discard cached entry if it in this lower percentile - stop value (maximum) */
  public static final String SCAVENGER_DUMP_ENTRY_BELOW_MAX_KEY = "scavenger.dump.entry.below.max";

  /** Adjustment step for scavenger */
  public static final String SCAVENGER_DUMP_ENTRY_BELOW_STEP_KEY =
      "scavenger.dump.entry.below.step";

  /** Scavenger number of threads */
  public static final String SCAVENGER_NUMBER_THREADS_KEY = "scavenger.number.threads";

  /** Number of popularity ranks ( default - 8) */
  public static final String CACHE_POPULARITY_NUMBER_RANKS_KEY = "popularity.number.ranks";

  /** Keep active data set fraction above this threshold */
  public static final String CACHE_MINIMUM_ACTIVE_DATA_SET_RATIO_KEY =
      "active.dataset.ratio.min";
  /** IO storage pool size */
  public static final String CACHE_IO_STORAGE_POOL_SIZE_KEY = "storage.io.pool.size";

  /** New item insertion point for SLRU (segment number 1- based) */
  public static final String SLRU_CACHE_INSERT_POINT_KEY = "eviction.slru.insert.point";

  /** Number of segments in SLRU eviction policy */
  public static final String SLRU_NUMBER_SEGMENTS_KEY = "eviction.slru.number.segments";

  /** Admission Queue start size in fraction of a full cache size */
  public static final String ADMISSION_QUEUE_START_SIZE_RATIO_KEY =
      "admission.queue.size.ratio.start";

  /** Admission Queue minimum size in fraction of a full cache size */
  public static final String ADMISSION_QUEUE_MIN_SIZE_RATIO_KEY = "admission.queue.size.ratio.min";

  /** Admission Queue maximum size in fraction of a full cache size */
  public static final String ADMISSION_QUEUE_MAX_SIZE_RATIO_KEY = "admission.queue.size.ratio.max";

  /** Promotion Queue start size in fraction of a full cache size */
  public static final String PROMOTION_QUEUE_START_SIZE_RATIO_KEY =
      "promotion.queue.size.ratio.start";

  /** Promotion Queue minimum size in fraction of a full cache size */
  public static final String PROMOTION_QUEUE_MIN_SIZE_RATIO_KEY = "promotion.queue.size.ratio.min";

  /** Promotion Queue maximum size in fraction of a full cache size */
  public static final String PROMOTION_QUEUE_MAX_SIZE_RATIO_KEY = "promotion.queue.size.ratio.max";

  /* Random configuration parameter for random promotion controller */
  public static final String CACHE_RANDOM_PROMOTION_PROBABILITY_KEY = "random.promotion.probability";

  /** Cumulative average write rate limit (bytes/sec) */
  public static final String CACHE_WRITE_RATE_LIMIT_KEY = "write.avg.rate.limit";

  /** Promotion on hit from victim to main cache */
  public static final String CACHE_VICTIM_PROMOTION_ON_HIT_KEY = "victim.promotion.on.hit";

  /** Evict all to victim cache. Default: false - only objects with hits > 0 */
  public static final String VICTIM_EVICT_ALL_KEY = "victim.evict.all";

  /*
   * Some file systems : ext4, xfs, APFS etc supports sparse files and so called "hole punching" -
   * discarding regions of files. We use different algorithm of compaction when file system supports
   * these features. Default: false.
   */
  public static final String SPARSE_FILES_SUPPORT_KEY = "sparse.files.support";

  /*
   * Index starting number of slots (Hash Table size) power of 2 - L ( N = 2**L) N - number of slots
   */
  public static final String START_INDEX_NUMBER_OF_SLOTS_POWER_KEY = "index.slots.power";

  /*
   * Cache write throughput check interval key
   */
  public static final String THROUGHPUT_CHECK_INTERVAL_SEC_KEY = "throughput.check.interval.sec";

  /*
   * Scavenger run interval key (seconds)
   */
  public static final String SCAVENGER_RUN_INTERVAL_SEC_KEY = "scavenger.run.interval.sec";


  /*
   * Cache write throughput controller tolerance limit
   */
  public static final String THROUGHPUT_CONTROLLER_TOLERANCE_LIMIT_KEY =
      "throughput.tolerance.limit";

  /*
   * Throughput controller number of adjustment steps
   */
  public static final String THROUGHPUT_CONTROLLER_ADJUSTMENT_STEPS_KEY =
      "throughput.adjustment.steps";

  /**
   * Does index support memory embedding (not implemented yet)
   */
  public static final String INDEX_DATA_EMBEDDED_KEY = "index.data.embedded";

  /**
   * Maximum data size to embed (includes both: key and value)
   **/
  public static final String INDEX_DATA_EMBEDDED_SIZE_KEY = "index.data.embedded.size.max";

  /** Class name for main queue index format implementation */
  public static final String INDEX_FORMAT_MAIN_QUEUE_IMPL_KEY = "index.format.impl";

  /** Class name for admission queue index format implementation */
  public static final String INDEX_FORMAT_ADMISSION_QUEUE_IMPL_KEY =
      "index.format.aq.impl";

  /** Class name for cache eviction policy implementation */
  public static final String CACHE_EVICTION_POLICY_IMPL_KEY = "eviction.policy.impl";

  /** Class name for cache admission controller implementation */
  public static final String CACHE_ADMISSION_CONTROLLER_IMPL_KEY = "admission.controller.impl";

  /** Class name for cache promotion controller implementation */
  public static final String CACHE_PROMOTION_CONTROLLER_IMPL_KEY = "promotion.controller.impl";

  /** Class name for cache throughput controller implementation */
  public static final String CACHE_THROUGHPUT_CONTROLLER_IMPL_KEY = "throughput.controller.impl";

  /** Class name for cache recycling controller implementation */
  public static final String CACHE_RECYCLING_SELECTOR_IMPL_KEY = "recycling.selector.impl";

  /** Class name for cache data appender implementation */
  public static final String CACHE_DATA_WRITER_IMPL_KEY = "data.writer.impl";

  /** Class name for cache data reader implementation (RAM) */
  public static final String CACHE_MEMORY_DATA_READER_IMPL_KEY = "memory.data.reader.impl";

  /** Class name for cache data reader implementation (File) */
  public static final String CACHE_FILE_DATA_READER_IMPL_KEY = "file.data.reader.impl";

  /** Block writer block size key */
  public static final String CACHE_BLOCK_WRITER_BLOCK_SIZE_KEY = "block.writer.block.size";

  /** File prefetch buffer size */
  public static final String FILE_PREFETCH_BUFFER_SIZE_KEY = "file.prefetch.buffer.size";

  /** Cache expiration support implementation key */
  public static final String CACHE_EXPIRE_SUPPORT_IMPL_KEY = "expire.support.impl";

  /** Random admission controller ratio start key */
  public static final String CACHE_RANDOM_ADMISSION_RATIO_START_KEY =
      "random.admission.ratio.start";

  /** Random admission controller ratio key */
  public static final String CACHE_RANDOM_ADMISSION_RATIO_STOP_KEY = "random.admission.ratio.stop";

  /** For expiration based admission controller */
  public static final String CACHE_EXPIRATION_BIN_START_VALUE_KEY = "expire.bin.value.start";

  /** Bin value multiplier */
  public static final String CACHE_EXPIRATION_MULTIPLIER_VALUE_KEY = "expire.multiplier.value";

  /** Eviction disabled mode */
  public static final String CACHE_EVICTION_DISABLED_MODE_KEY = "eviction.disabled";

  /** Hybrid cache mode of operation */
  public static final String CACHE_HYBRID_INVERSE_MODE_KEY = "hybrid.inverse.mode";

  /** Victim cache promotion threshold */
  public static final String CACHE_VICTIM_PROMOTION_THRESHOLD_KEY = "victim.promotion.threshold";

  /** Spin wait time on high pressure in nanoseconds */
  public static final String CACHE_SPIN_WAIT_TIME_NS_KEY = "spin.wait.time.ns";

  /** JMX metrics domain name */
  public static final String CACHE_JMX_METRICS_DOMAIN_NAME_KEY = "jmx.metrics.domain.name";

  /** JMX metrics enable - disable */
  public static final String CACHE_JMX_METRICS_ENABLED_KEY = "jmx.metrics.enabled";
  
  /* Do not expose */
  public static final String CACHE_STREAMING_SUPPORT_BUFFER_SIZE_KEY =
      "streaming.buffer.size";

  /* Maximum wait time on put operation in ms */
  public static final String CACHE_MAX_WAIT_ON_PUT_MS_KEY = "cache.wait.put.max.ms";

  /* Maximum Key Value size to cache */
  public static final String CACHE_MAX_KEY_VALUE_SIZE_KEY = "cache.max.kv.size";

  /* Object Cache initial output buffer size - not relevant for server */
  public final static String OBJECT_CACHE_INITIAL_BUFFER_SIZE_KEY =
      "objectcache.buffer.size.start";

  /* Object Cache maximum output buffer size - not relevant for server */
  public final static String OBJECT_CACHE_MAX_BUFFER_SIZE_KEY = "objectcache.buffer.size.max";

  /* Is Thread-Local-Storage supported */
  public final static String CACHE_TLS_SUPPORTED_KEY = "tls.supported";

  /* TLS buffer initial size key */
  public final static String CACHE_TLS_INITIAL_BUFFER_SIZE_KEY = "cache.tls.buffer.size.start";

  /* TLS buffer maximum key size */
  public final static String CACHE_TLS_MAXIMUM_BUFFER_SIZE_KEY = "cache.tls.buffer.size.max";

  /* Compression configuration */

  public final static String CACHE_COMPRESSION_ENABLED_KEY = "compression.enabled";

  public final static String CACHE_COMPRESSION_BLOCK_SIZE_KEY = "compression.block.size";

  public final static String CACHE_COMPRESSION_DICTIONARY_SIZE_KEY =
      "compression.dictionary.size";

  public final static String CACHE_COMPRESSION_LEVEL_KEY = "compression.level";

  public final static String CACHE_COMPRESSION_CODEC_KEY = "compression.codec";

  public final static String CACHE_COMPRESSION_DICTIONARY_ENABLED_KEY =
      "compression.dictionary.enabled";

  public final static String CACHE_COMPRESSION_KEYS_ENABLED_KEY = "compression.keys.enabled";

  public final static String CACHE_COMPRESSION_DICTIONARY_TRAINING_ASYNC_KEY =
      "compression.dictionary.training.async";

  public final static String CACHE_SAVE_ON_SHUTDOWN_KEY = "save.on.shutdown";

  public final static String CACHE_ESTIMATED_AVG_KV_SIZE_KEY = "estimated.avg.kv.size";

  public final static String CACHE_PROACTIVE_EXPIRATION_FACTOR_KEY = "proactive.expiration.factor";

  public final static String VACUUM_CLEANER_INTERVAL_SEC_KEY = "vacuum.cleaner.interval";
  
  public final static String CACHE_ASYNC_IO_POOL_SIZE_KEY = "async.io.pool.size";

  /** Defaults section */

  public static final long DEFAULT_CACHE_SEGMENT_SIZE = 4 * 1024 * 1024;

  public static final long DEFAULT_CACHE_MAXIMUM_SIZE = 1_073_741_824; // 1GB

  public static final double DEFAULT_SCAVENGER_START_RUN_RATIO = 0.99;

  public static final double DEFAULT_SCAVENGER_STOP_RUN_RATIO = 0.9; // Set it to 0 for continuous
                                                                     // scavenger run

  /**
   * During scavenger run, all expired entries as well as entries with popularity below this config
   * value will be dumped, all others will be rewritten back to the cache. This is the start value
   */
  public static final double DEFAULT_SCAVENGER_DUMP_ENTRY_BELOW_MIN = 0.1;

  /**
   * During scavenger run, all expired entries as well as entries with popularity below this config
   * value will be dumped, all others will be rewritten back to a cache. This is the stop value
   */
  public static final double DEFAULT_SCAVENGER_DUMP_ENTRY_BELOW_MAX = 0.5;

  /**
   * Default value for scavenger adjustment step
   */
  public static final double DEFAULT_SCAVENGER_DUMP_ENTRY_BELOW_STEP = 0.1;

  /**
   * Default number of Scavenger (Garbage collector) threads
   */
  public static final int DEFAULT_SCAVENGER_NUMBER_THREADS = 1;

  /**
   * Limits write speed during scavenger run to 0.9 of scavenger cleaning memory rate Suppose
   * Scavenger frees memory with a rate of 900MB/sec. Incoming cache write requests will be limited
   * to a 900*0.9 MS/sec = 810MB/sec. This is necessary to guarantee that we won't get OOM errors
   * (or swapping)
   */
  public static final double DEFAULT_WRITES_LIMIT_RATIO = 0.9;

  /** Default number of popularity ranks for data items in the cache */
  public static final int DEFAULT_CACHE_POPULARITY_NUMBER_RANKS = 8;

  /** Number of segments in segmented-LRU cache */
  public static final int DEFAULT_SLRU_NUMBER_SEGMENTS = 8;

  /** Default insertion point for SLRU - head of nth segments - 4 */
  public static final int DEFAULT_SLRU_CACHE_INSERT_POINT = 4;

  /** Default segment size */
  public static final long DEFAULT_CACHE_DISK_SEGMENT_SIZE = 256 * 1024 * 1024; // 64MB

  /** Default re-admission hit count min */
  public final static int DEFAULT_READMISSION_HIT_COUNT_MIN = 1;

  /** Default sparse files support */
  public final static boolean DEFAULT_SPARSE_FILES_SUPPORT = false;

  /** Default AQ (admission queue) start size a fraction of a cache size */
  public final static double DEFAULT_ADMISSION_QUEUE_START_SIZE_RATIO = 0.5;

  /** Default AQ (admission queue) minimum size a fraction of a cache size */
  public final static double DEFAULT_ADMISSION_QUEUE_MIN_SIZE_RATIO = 0.1;

  /** Default AQ (admission queue) maximum size a fraction of a cache size */
  public final static double DEFAULT_ADMISSION_QUEUE_MAX_SIZE_RATIO = 0.5;

  /** Default AQ (admission queue) start size a fraction of a cache size */
  public final static double DEFAULT_PROMOTION_QUEUE_START_SIZE_RATIO = 0.5;

  /** Default AQ (admission queue) minimum size a fraction of a cache size */
  public final static double DEFAULT_PROMOTION_QUEUE_MIN_SIZE_RATIO = 0.1;

  /** Default AQ (admission queue) maximum size a fraction of a cache size */
  public final static double DEFAULT_PROMOTION_QUEUE_MAX_SIZE_RATIO = 0.5;

  /** Default disk limit - 0 unlimited */
  public static final long DEFAULT_DISK_LIMIT = 0; // Unlimited

  /** Default size as a power indicator of 2 for index N = 2**10 - start */
  public static final int DEFAULT_START_INDEX_NUMBER_OF_SLOTS_POWER = 10;

  /** Default throughput check interval - 1 hour */
  public static final long DEFAULT_THROUGHPUT_CHECK_INTERVAL_SEC = 3600;// 1h

  /** Default Scavenger run interval - 1 min */
  public static final long DEFAULT_SCAVENGER_RUN_INTERVAL_SEC = 60;// 1min

  /** Default Scavenger evict all to a victim cache */
  public static final boolean DEFAULT_VICTIM_EVICT_ALL = true;// 1min

  /** Default throughput controller tolerance limit */
  public static final double DEFAULT_THROUGHPUT_CONTROLLER_TOLERANCE_LIMIT = 0.05;

  /** Default cache write rate limit */
  public static final long DEFAULT_CACHE_WRITE_RATE_LIMIT = 50 * 1024 * 1024;

  /** Default number of adjustment steps */
  public static final int DEFAULT_THROUGHPUT_CONTROLLER_ADJUSTMENT_STEPS = 10;

  /** Default for index data embedding */
  public final static boolean DEFAULT_INDEX_DATA_EMBEDDED = false;

  /** Default index data embedding maximum size */
  public final static int DEFAULT_INDEX_DATA_EMBEDDED_SIZE = 100;

  /** Default implementation class for recycling selector */
  public final static String DEFAULT_CACHE_RECYCLING_SELECTOR_IMPL =
      "com.carrotdata.cache.controllers.MinAliveRecyclingSelector";
  /** Default */
  public final static String DEFAULT_CACHE_DATA_WRITER_IMPL =
      "com.carrotdata.cache.io.BaseDataWriter";

  /** Default reader for RAM segments */
  public final static String DEFAULT_CACHE_MEMORY_DATA_READER_IMPL =
      "com.carrotdata.cache.io.BaseMemoryDataReader";

  /** Default reader for file segments */
  public final static String DEFAULT_CACHE_FILE_DATA_READER_IMPL =
      "com.carrotdata.cache.io.BaseFileDataReader";

  /** Default expire support implementation */
  public final static String DEFAULT_CACHE_EXPIRE_SUPPORT_IMPL =
      "com.carrotdata.cache.expire.ExpireSupportSecondsMinutes";

  /** Default block writer block size */
  public final static int DEFAULT_CACHE_BLOCK_WRITER_BLOCK_SIZE = 4096;

  /** Increase this size if you want to cache items larger */
  public final static int DEFAULT_FILE_PREFETCH_BUFFER_SIZE = 4 * 1024 * 1024;

  /** Default value for random admission adjustment start */
  public static final double DEFAULT_CACHE_RANDOM_ADMISSION_RATIO_START = 1.d;

  /** Default value for random admission adjustment stop */
  public static final double DEFAULT_CACHE_RANDOM_ADMISSION_RATIO_STOP = 0.d;

  /** Default value for start bin limit in seconds */
  public static final long DEFAULT_CACHE_EXPIRATION_BIN_START_VALUE = 60;// in seconds

  /** Default value for cache expiration multiplier */
  public static final double DEFAULT_CACHE_EXPIRATION_MULTIPLIER_VALUE = 2;

  /** Default minimum active data set ratio */
  public final static double DEFAULT_CACHE_MINIMUM_ACTIVE_DATA_SET_RATIO = 0.9;

  /** Default IO pool size */
  public final static int DEFAULT_CACHE_IO_STORAGE_POOL_SIZE = 8;

  /** Default cache disabled mode */
  public final static boolean DEFAULT_CACHE_EVICTION_DISABLED_MODE = false;

  /** Rolling window counter bins default */
  public final static int DEFAULT_CACHE_ROLLING_WINDOW_COUNTER_BINS = 60;

  /** Rolling window counter window size in seconds default */
  public final static int DEFAULT_CACHE_ROLLING_WINDOW_COUNTER_DURATION = 3600;

  /** Victim cache promotion on hit default value */
  public final static boolean DEFAULT_CACHE_VICTIM_PROMOTION_ON_HIT = true;

  /** Default hybrid inverse mode of operation */
  public final static boolean DEFAULT_CACHE_HYBRID_INVERSE_MODE = false;

  /** Default victim promotion threshold */
  public final static double DEFAULT_CACHE_VICTIM_PROMOTION_THRESHOLD = 0.9;

  /** Default cache spin wait time on high pressure - PUT operation */
  public final static long DEFAULT_CACHE_SPIN_WAIT_TIME_NS = 10000;// 10000 nanoseconds

  /** Default domain name for JMX metrics */
  public final static String DEFAULT_CACHE_JMX_METRICS_DOMAIN_NAME = "com.carrotdata.cache";

  /** Default JMX metrics enable-disable */
  public final static boolean DEFAULT_CACHE_JMX_METRICS_ENABLED = false;

  /** Default streaming support buffer size */
  public final static int DEFAULT_CACHE_STREAMING_SUPPORT_BUFFER_SIZE = 1 << 22;

  /** Default cache maximum wait time on PUT (due to full storage) in ms */
  public final static int DEFAULT_CACHE_MAX_WAIT_ON_PUT_MS = 20; // Wait up to 20ms when storage is
                                                                 // full

  /** Default value for maximum key-value size */
  public final static int DEFAULT_CACHE_MAX_KEY_VALUE_SIZE = 0; // no limit, limit is defined by
                                                                // data segment size

  /** Default initial size for object cache output buffer */
  public final static int DEFAULT_OBJECT_CACHE_INITIAL_BUFFER_SIZE = 1 << 16;

  /** Default maximum size for object cache output buffer */
  public final static int DEFAULT_OBJECT_CACHE_MAX_BUFFER_SIZE = -1; // Unlimited; TODO change to
                                                                     // 2GB?

  /** Default Thread-Local-Storage supported */
  public final static boolean DEFAULT_CACHE_TLS_SUPPORTED = true;

  /** Default initial size for TLS buffer */
  public final static int DEFAULT_CACHE_TLS_INITIAL_BUFFER_SIZE = 1 << 16; // 64KB

  /** Default initial size for TLS buffer */
  public final static int DEFAULT_CACHE_TLS_MAXIMUM_BUFFER_SIZE = 268435456; // 256MB

  /** Is cache compression enabled */
  public final static boolean DEFAULT_CACHE_COMPRESSION_ENABLED = false;

  /** Is cache compression enabled */
  public final static boolean DEFAULT_CACHE_COMPRESSION_DICTIONARY_ENABLED = true;

  /** Is key cache compression enabled */
  public final static boolean DEFAULT_CACHE_COMPRESSION_KEYS_ENABLED = true;

  /** Compression block size */
  public final static int DEFAULT_CACHE_COMPRESSION_BLOCK_SIZE = 4 * 1024;

  /** Compression dictionary size */
  public final static int DEFAULT_CACHE_COMPRESSION_DICTIONARY_SIZE = 64 * 1024;

  /** Compression level */
  public final static int DEFAULT_CACHE_COMPRESSION_LEVEL = 3;

  /** Compression codec type */
  public final static String DEFAULT_CACHE_COMPRESSION_CODEC = "ZSTD";

  /** Compression dictionary training async */
  public final static boolean DEFAULT_CACHE_COMPRESSION_DICTIONARY_TRAINING_ASYNC = true;

  /** Default probability for random promotion controller */
  public final static double DEFAULT_CACHE_RANDOM_PROMOTION_PROBABILITY = 0.1d;

  /* Default save on shutdown */
  public final static boolean DEFAULT_CACHE_SAVE_ON_SHUTDOWN = false;

  /* Default average K-V size */
  public final static int DEFAULT_ESTIMATED_AVG_KV_SIZE = 10 * 1024;

  public final static double DEFAULT_CACHE_PROACTIVE_EXPIRATION_FACTOR = 0.25;

  public final static long DEFAULT_VACUUM_CLEANER_INTERVAL_SEC = 60; // seconds
  
  public final int DEFAULT_CACHE_ASYNC_IO_POOL_SIZE = 32;

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

      instance = load(file);
    }
    return instance;
  }

  /**
   * Used for testing only
   */
  public static void setInstance(CacheConfig config) {
    instance = config;
  }

  /**
   * Merge configuration to the main configuration
   * @param props properties
   */
  public static synchronized void merge(Properties props) {
    CacheConfig mainConfig = getInstance();
    for (Map.Entry<Object, Object> entry : props.entrySet()) {
      String key = (String) entry.getKey();
      String value = (String) entry.getValue();
      mainConfig.setProperty(key, value);
    }
  }

  /**
   * Load configuration from an input stream
   * @param file input stream
   * @return cache configuration object
   * @throws IOException
   */
  public static CacheConfig load(String file) throws IOException {
    FileInputStream is = new FileInputStream(file);
    Properties props = new Properties();
    DataInputStream dis = new DataInputStream(is);
    String line = null;
    while ((line = dis.readLine()) != null) {
      if (line.startsWith("#")) {
        continue;
      }
      String[] kv = line.split("=");
      if (kv.length != 2) {
        continue;
      }
      String key = kv[0].trim();
      String value = kv[1].trim();
      props.setProperty(key, value);
    }
    dis.close();
    return new CacheConfig(props);
  }

  // Instance
  EnvProperties props = new EnvProperties();

  /** Default constructor */
  protected CacheConfig() {
  }

  /**
   * Mockito - friendly
   */
  public void init() {
    this.props = new EnvProperties();
  }

  /**
   * Constructor from properties
   * @param props
   */
  CacheConfig(Properties props) {
    this.props = new EnvProperties();
    for (Map.Entry<Object, Object> e: props.entrySet()) {
      String key = (String) e.getKey();
      String value = (String) e.getValue();
      this.props.setProperty(key, value);
    }
  }

  /**
   * Save configuration
   * @param os output stream
   * @throws IOException
   */
  public void save(OutputStream os) throws IOException {
    this.props.store(os, "Cache configuration properties");
  }

  public double getDoubleProperty(String name, double defValue) {
    String value = props.getProperty(name);
    if (value == null) return defValue;
    return Double.parseDouble(value);
  }

  public long getLongProperty(String name, long defValue) {
    String value = props.getProperty(name);
    if (value == null) return defValue;

    value = value.trim().toLowerCase();
    // input string is first trimmed and converted to lowercase format
    long multiplier = 1;

    if (value.endsWith("k")) {
      multiplier = 1024L;
    } else if (value.endsWith("m")) {
      multiplier = 1024L * 1024L;
      // mega is 2 times kilo, giga is 3 times kilo, etc...
    } else if (value.endsWith("g")) {
      multiplier = 1024L * 1024L * 1024L;
    } else if (value.endsWith("t")) {
      multiplier = 1024L * 1024L * 1024L * 1024L;
    }
    if (multiplier > 1) {
      value = value.substring(0, value.length() - 1);
    }
    return Long.parseLong(value.replaceAll(CHARS_TO_REMOVE_REGEX, "")) * multiplier;
  }

  public long getLong(String value, long defValue) {
    if (value == null) {
      return defValue;
    }
    value = value.trim().toLowerCase();
    // input string is first trimmed and converted to lowercase format
    long multiplier = 1;

    if (value.endsWith("k")) {
      multiplier = 1024L;
    } else if (value.endsWith("m")) {
      multiplier = 1024L * 1024L;
      // mega is 2 times kilo, giga is 3 times kilo, etc...
    } else if (value.endsWith("g")) {
      multiplier = 1024L * 1024L * 1024L;
    } else if (value.endsWith("t")) {
      multiplier = 1024L * 1024L * 1024L * 1024L;
    }
    if (multiplier > 1) {
      value = value.substring(0, value.length() - 1);
    }
    return Long.parseLong(value.replaceAll(CHARS_TO_REMOVE_REGEX, "")) * multiplier;
  }

  public String getProperty(String name, String defValue) {
    String value = props.getProperty(name);
    if (value == null) return defValue;
    return value;
  }

  public void setProperty(String name, String value) {
    props.setProperty(name, value);
  }

  public boolean getBooleanProperty(String name, boolean defValue) {
    String value = props.getProperty(name);
    if (value == null) return defValue;
    return Boolean.parseBoolean(value);
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
    String value = props.getProperty(cacheName + "." + CACHE_MAXIMUM_SIZE_KEY);
    if (value != null) {
      return getLong(value, DEFAULT_CACHE_MAXIMUM_SIZE);
    }
    return getLongProperty(CACHE_MAXIMUM_SIZE_KEY, DEFAULT_CACHE_MAXIMUM_SIZE);
  }

  /**
   * Set cache maximum size
   * @param cacheName cache name
   * @param size maximum size in bytes
   */
  public void setCacheMaximumSize(String cacheName, long size) {
    props.setProperty(cacheName + "." + CACHE_MAXIMUM_SIZE_KEY, Long.toString(size));
  }

  /**
   * Get segment size for a cache
   * @param cacheName cache name
   * @return segment size
   */
  public long getCacheSegmentSize(String cacheName) {
    String value = props.getProperty(cacheName + "." + CACHE_SEGMENT_SIZE_KEY);
    if (value != null) {
      return getLong(value, DEFAULT_CACHE_SEGMENT_SIZE);
    }
    return getLongProperty(CACHE_SEGMENT_SIZE_KEY, DEFAULT_CACHE_SEGMENT_SIZE);
  }

  /**
   * Set cache segment size
   * @param cacheName cache name
   * @param size segment size in bytes
   */
  public void setCacheSegmentSize(String cacheName, long size) {
    props.setProperty(cacheName + "." + CACHE_SEGMENT_SIZE_KEY, Long.toString(size));
  }

  /**
   * Gets scavenger start memory ratio for a cache
   * @param cacheName cache name
   * @return start ratio for GC Scavenger
   */
  public double getScavengerStartMemoryRatio(String cacheName) {
    String value = props.getProperty(cacheName + "." + SCAVENGER_START_RUN_RATIO_KEY);
    if (value != null) {
      return Double.parseDouble(value);
    }
    return getDoubleProperty(SCAVENGER_START_RUN_RATIO_KEY, DEFAULT_SCAVENGER_START_RUN_RATIO);
  }

  /**
   * Set scavenger start memory ratio
   * @param cacheName cache name
   * @param ratio memory ration relative to a maximum cache size
   */
  public void setScavengerStartMemoryRatio(String cacheName, double ratio) {
    props.setProperty(cacheName + "." + SCAVENGER_START_RUN_RATIO_KEY, Double.toString(ratio));
  }

  /**
   * Gets scavenger stop memory ratio
   * @param cacheName cache name
   * @return stop ratio
   */
  public double getScavengerStopMemoryRatio(String cacheName) {
    String value = props.getProperty(cacheName + "." + SCAVENGER_STOP_RUN_RATIO_KEY);
    if (value != null) {
      return Double.parseDouble(value);
    }
    return getDoubleProperty(SCAVENGER_STOP_RUN_RATIO_KEY, DEFAULT_SCAVENGER_STOP_RUN_RATIO);
  }

  /**
   * Set scavenger stop memory ratio
   * @param cacheName cache name
   * @param ratio memory ration relative to a maximum cache size
   */
  public void setScavengerStopMemoryRatio(String cacheName, double ratio) {
    props.setProperty(cacheName + "." + SCAVENGER_STOP_RUN_RATIO_KEY, Double.toString(ratio));
  }

  /**
   * Gets scavenger dump entry below - start
   * @param cacheName cache name
   * @return dump entry below ratio
   */
  public double getScavengerDumpEntryBelowAdjStep(String cacheName) {
    String value = props.getProperty(cacheName + "." + SCAVENGER_DUMP_ENTRY_BELOW_STEP_KEY);
    if (value != null) {
      return Double.parseDouble(value);
    }
    return getDoubleProperty(SCAVENGER_DUMP_ENTRY_BELOW_STEP_KEY,
      DEFAULT_SCAVENGER_DUMP_ENTRY_BELOW_STEP);
  }

  /**
   * Sets scavenger dump entry below adjustment step
   * @param cacheName cache name
   * @param step entry below adjustment step
   */
  public void setScavengerDumpEntryBelowAdjStep(String cacheName, double step) {
    props.setProperty(cacheName + "." + SCAVENGER_DUMP_ENTRY_BELOW_STEP_KEY, Double.toString(step));
  }

  /**
   * Gets scavenger dump entry below - minimum
   * @param cacheName cache name
   * @return dump entry below ratio
   */
  public double getScavengerDumpEntryBelowMin(String cacheName) {
    String value = props.getProperty(cacheName + "." + SCAVENGER_DUMP_ENTRY_BELOW_MIN_KEY);
    if (value != null) {
      return Double.parseDouble(value);
    }
    return getDoubleProperty(SCAVENGER_DUMP_ENTRY_BELOW_MIN_KEY,
      DEFAULT_SCAVENGER_DUMP_ENTRY_BELOW_MIN);
  }

  /**
   * Sets scavenger dump entry below - minimum
   * @param cacheName cache name
   * @param ratio entry below ratio start
   */
  public void setScavengerDumpEntryBelowMin(String cacheName, double ratio) {
    props.setProperty(cacheName + "." + SCAVENGER_DUMP_ENTRY_BELOW_MIN_KEY, Double.toString(ratio));
  }

  /**
   * Gets scavenger dump entry below - maximum
   * @param cacheName cache name
   * @return dump entry below ratio
   */
  public double getScavengerDumpEntryBelowMax(String cacheName) {
    String value = props.getProperty(cacheName + "." + SCAVENGER_DUMP_ENTRY_BELOW_MAX_KEY);
    if (value != null) {
      return Double.parseDouble(value);
    }
    return getDoubleProperty(SCAVENGER_DUMP_ENTRY_BELOW_MAX_KEY,
      DEFAULT_SCAVENGER_DUMP_ENTRY_BELOW_MAX);
  }

  /**
   * Sets scavenger dump entry below - maximum
   * @param cacheName cache name
   * @param ratio entry below ratio stop
   */
  public void setScavengerDumpEntryBelowMax(String cacheName, double ratio) {
    props.setProperty(cacheName + "." + SCAVENGER_DUMP_ENTRY_BELOW_MAX_KEY, Double.toString(ratio));
  }

  /**
   * Get random admission controller start ratio
   * @param cacheName cache name
   * @return start ratio
   */
  public double getRandomAdmissionControllerStartRatio(String cacheName) {
    String value = props.getProperty(cacheName + "." + CACHE_RANDOM_ADMISSION_RATIO_START_KEY);
    if (value != null) {
      return Double.parseDouble(value);
    }
    return getDoubleProperty(CACHE_RANDOM_ADMISSION_RATIO_START_KEY,
      DEFAULT_CACHE_RANDOM_ADMISSION_RATIO_START);
  }

  /**
   * Set random admission controller start ratio
   * @param cacheName cache name
   * @param ratio start ratio
   */
  public void setRandomAdmissionControllerStartRatio(String cacheName, double ratio) {
    props.setProperty(cacheName + "." + CACHE_RANDOM_ADMISSION_RATIO_START_KEY,
      Double.toString(ratio));
  }

  /**
   * Get random admission controller stop ratio
   * @param cacheName cache name
   * @return stop ratio
   */
  public double getRandomAdmissionControllerStopRatio(String cacheName) {
    String value = props.getProperty(cacheName + "." + CACHE_RANDOM_ADMISSION_RATIO_STOP_KEY);
    if (value != null) {
      return Double.parseDouble(value);
    }
    return getDoubleProperty(CACHE_RANDOM_ADMISSION_RATIO_STOP_KEY,
      DEFAULT_CACHE_RANDOM_ADMISSION_RATIO_STOP);
  }

  /**
   * Set random admission controller stop ratio
   * @param cacheName cache name
   * @param ratio stop ratio
   */
  public void setRandomAdmissionControllerStopRatio(String cacheName, double ratio) {
    props.setProperty(cacheName + "." + CACHE_RANDOM_ADMISSION_RATIO_STOP_KEY,
      Double.toString(ratio));
  }

  /**
   * Get number of item popularity ranks (bins) for a cache
   * @param cacheName cache name
   * @return number of popularity ranks
   */
  public int getNumberOfPopularityRanks(String cacheName) {
    String value = props.getProperty(cacheName + "." + CACHE_POPULARITY_NUMBER_RANKS_KEY);
    if (value != null) {
      return (int) Long.parseLong(value.replaceAll(CHARS_TO_REMOVE_REGEX, ""));
    }
    return (int) getLongProperty(CACHE_POPULARITY_NUMBER_RANKS_KEY,
      DEFAULT_CACHE_POPULARITY_NUMBER_RANKS);
  }

  /**
   * Set number of item popularity ranks for a cache
   * @param cacheName cache name
   * @param num number of popularity ranks
   */
  public void setNumberOfPopularityRanks(String cacheName, int num) {
    props.setProperty(cacheName + "." + CACHE_POPULARITY_NUMBER_RANKS_KEY, Integer.toString(num));
  }

  /**
   * Get SLRU insertion point (segment number to insert into the head of)
   * @param cacheName cache name
   * @return SLRU insertion point (segment number between 0 and getSLRUNumberOfSegments)
   */
  public int getSLRUInsertionPoint(String cacheName) {
    String value = props.getProperty(cacheName + "." + SLRU_CACHE_INSERT_POINT_KEY);
    if (value != null) {
      return (int) Long.parseLong(value.replaceAll(CHARS_TO_REMOVE_REGEX, ""));
    }
    return (int) getLongProperty(SLRU_CACHE_INSERT_POINT_KEY, DEFAULT_SLRU_CACHE_INSERT_POINT);
  }

  /**
   * Set SLRU insertion point (segment number to insert into the head of)
   * @param cacheName cache name
   * @param n SLRU insertion point (segment number between 0 and getSLRUNumberOfSegments - 1)
   */
  public void setSLRUInsertionPoint(String cacheName, int n) {
    props.setProperty(cacheName + "." + SLRU_CACHE_INSERT_POINT_KEY, Integer.toString(n));
  }

  /**
   * Get SLRU number of segments
   * @param cacheName cache name
   * @return SLRU number of segments
   */
  public int getSLRUNumberOfSegments(String cacheName) {
    String value = props.getProperty(cacheName + "." + SLRU_NUMBER_SEGMENTS_KEY);
    if (value != null) {
      return (int) Long.parseLong(value.replaceAll(CHARS_TO_REMOVE_REGEX, ""));
    }
    return (int) getLongProperty(SLRU_NUMBER_SEGMENTS_KEY, DEFAULT_SLRU_NUMBER_SEGMENTS);
  }

  /**
   * Set SLRU number of segments
   * @param cacheName cache name
   * @param n SLRU number of segments
   */
  public void setSLRUNumberOfSegments(String cacheName, int n) {
    props.setProperty(cacheName + "." + SLRU_NUMBER_SEGMENTS_KEY, Integer.toString(n));
  }

  /**
   * Get evict all to victim
   * @param cacheName cache name
   * @return true / false
   */
  public boolean getVictimEvictAll(String cacheName) {
    String value = props.getProperty(cacheName + "." + VICTIM_EVICT_ALL_KEY);
    if (value != null) {
      return Boolean.parseBoolean(value);
    }
    return getBooleanProperty(VICTIM_EVICT_ALL_KEY, DEFAULT_VICTIM_EVICT_ALL);
  }

  /**
   * Set evict all to victim
   * @param cacheName cache name
   * @param v true or false
   */
  public void setVictimEvictAll(String cacheName, boolean v) {
    props.setProperty(cacheName + "." + VICTIM_EVICT_ALL_KEY, Boolean.toString(v));
  }

  /**
   * Get sparse files support for a cache (only for 'file' type caches)
   * @param cacheName cache name
   * @return true / false
   */
  public boolean getSparseFilesSupport(String cacheName) {
    String value = props.getProperty(cacheName + "." + SPARSE_FILES_SUPPORT_KEY);
    if (value != null) {
      return Boolean.parseBoolean(value);
    }
    return getBooleanProperty(SPARSE_FILES_SUPPORT_KEY, DEFAULT_SPARSE_FILES_SUPPORT);
  }

  /**
   * Set sparse files support for a cache (only for 'file' type caches)
   * @param cacheName cache name
   * @param v true or false
   */
  public void setSparseFilesSupport(String cacheName, boolean v) {
    props.setProperty(cacheName + "." + SPARSE_FILES_SUPPORT_KEY, Boolean.toString(v));
  }

  /**
   * Get start index size for a cache
   * @param cacheName cache name
   * @return start index size for a given cache name
   */
  public int getStartIndexNumberOfSlotsPower(String cacheName) {
    String value = props.getProperty(cacheName + "." + START_INDEX_NUMBER_OF_SLOTS_POWER_KEY);
    if (value != null) {
      return (int) Long.parseLong(value.replaceAll(CHARS_TO_REMOVE_REGEX, ""));
    }
    return (int) getLongProperty(START_INDEX_NUMBER_OF_SLOTS_POWER_KEY,
      DEFAULT_START_INDEX_NUMBER_OF_SLOTS_POWER);
  }

  /**
   * Set start index size for a cache
   * @param cacheName cache name
   * @param v start index size for a given cache name
   */
  public void setStartIndexNumberOfSlotsPower(String cacheName, int v) {
    props.setProperty(cacheName + "." + START_INDEX_NUMBER_OF_SLOTS_POWER_KEY, Integer.toString(v));
  }

  /**
   * Get snapshot directory location for a cache
   * @param cacheName cache name
   * @return snapshot directory name for a given cache name
   */
  public String getSnapshotDir(String cacheName) {
    String[] values = getCacheRootDirs(cacheName);
    String value = values[0];
    value += File.separator + cacheName + File.separator + "snapshot";
    Path p = Paths.get(value);
    if (Files.notExists(p)) {
      try {
        Files.createDirectories(p);
      } catch (IOException e) {
        LOG.error("Error:", e);
        return null;
      }
    }
    return value;
  }

  /**
   * Get data directory location for a cache
   * @param cacheName cache name
   * @return data directory name for a given cache name
   */
  public String[] getDataDirs(String cacheName) {
    String[] values = getCacheRootDirs(cacheName);
    for (int i = 0; i < values.length; i++) {
      values[i] += File.separator + cacheName + File.separator + "data";
      // check if directory exists
      Path p = Paths.get(values[i]);
      if (Files.notExists(p)) {
        try {
          Files.createDirectories(p);
        } catch (IOException e) {
          LOG.error("Error:", e);
          return null;
        }
      }
    }
    return values;
  }

  /**
   * Get root directory location for a cache
   * @param cacheName cache name
   * @return root directory name for a given cache name
   */
  public String[] getCacheRootDirs(String cacheName) {
    String value = props.getProperty(cacheName + "." + CACHE_DATA_DIR_PATHS_KEY);
    if (value == null) {
      value = props.getProperty(CACHE_DATA_DIR_PATHS_KEY, DEFAULT_CACHE_DIR_PATHS);
    } else {
      String[] values = value.split(",");
      return Arrays.stream(values).map(String::trim).toArray(String[]::new);
    }
    return new String[] {value.trim()};
  }

  /**
   * Set root directory location for a cache
   * @param cacheName cache name
   * @param dir data directory names for a given cache name
   */
  public void setCacheRootDirs(String cacheName, String[] dirs) {
    
    props.setProperty(cacheName + "." + CACHE_DATA_DIR_PATHS_KEY, String.join(",", dirs));
  }

  /**
   * Get dictionary directory for cache
   * @param cacheName cache name
   * @return dictionary directory
   */
  public String getCacheDictionaryDir(String cacheName) {
    String[] values = getCacheRootDirs(cacheName);
    String value = values[0];
    value += File.separator + cacheName + File.separator + DICTIONARY_DIR_NAME;
    // check if directory exists
    Path p = Paths.get(value);
    if (Files.notExists(p)) {
      try {
        Files.createDirectories(p);
      } catch (IOException e) {
        LOG.error("Error:", e);
        return null;
      }
    }
    return value;
  }

  /**
   * Set global root directory location for all caches
   * @param dir directory name
   */
  public void setGlobalCacheRootDir(String dir) {
    props.setProperty(CACHE_DATA_DIR_PATHS_KEY, dir);
  }

  /**
   * Get admission queue start size ratio for a given cache name
   * @param cacheName cache name
   * @return AQ start size ratio relative to the maximum cache size
   */
  public double getAdmissionQueueStartSizeRatio(String cacheName) {
    String value = props.getProperty(cacheName + "." + ADMISSION_QUEUE_START_SIZE_RATIO_KEY);
    if (value != null) {
      return Double.parseDouble(value);
    }
    return getDoubleProperty(ADMISSION_QUEUE_START_SIZE_RATIO_KEY,
      DEFAULT_ADMISSION_QUEUE_START_SIZE_RATIO);
  }

  /**
   * Set admission queue start size ratio for a given cache name
   * @param cacheName cache name
   * @param ratio AQ start size ratio relative to the maximum cache size
   */
  public void setAdmissionQueueStartSizeRatio(String cacheName, double ratio) {
    props.setProperty(cacheName + "." + ADMISSION_QUEUE_START_SIZE_RATIO_KEY,
      Double.toString(ratio));
  }

  /**
   * Get admission queue minimum size ratio for a given cache name
   * @param cacheName cache name
   * @return AQ minimum size ratio relative to the maximum cache size
   */
  public double getAdmissionQueueMinSizeRatio(String cacheName) {
    String value = props.getProperty(cacheName + "." + ADMISSION_QUEUE_MIN_SIZE_RATIO_KEY);
    if (value != null) {
      return Double.parseDouble(value);
    }
    return getDoubleProperty(ADMISSION_QUEUE_MIN_SIZE_RATIO_KEY,
      DEFAULT_ADMISSION_QUEUE_MIN_SIZE_RATIO);
  }

  /**
   * Set admission queue minimum size ratio for a given cache name
   * @param cacheName cache name
   * @param ratio AQ minimum size ratio relative to the maximum cache size
   */
  public void setAdmissionQueueMinSizeRatio(String cacheName, double ratio) {
    props.setProperty(cacheName + "." + ADMISSION_QUEUE_MIN_SIZE_RATIO_KEY, Double.toString(ratio));
  }

  /**
   * Get admission queue maximum size ratio for a given cache name
   * @param cacheName cache name
   * @return AQ maximum size ratio relative to the maximum cache size
   */
  public double getAdmissionQueueMaxSizeRatio(String cacheName) {
    String value = props.getProperty(cacheName + "." + ADMISSION_QUEUE_MAX_SIZE_RATIO_KEY);
    if (value != null) {
      return Double.parseDouble(value);
    }
    return getDoubleProperty(ADMISSION_QUEUE_MAX_SIZE_RATIO_KEY,
      DEFAULT_ADMISSION_QUEUE_MAX_SIZE_RATIO);
  }

  /**
   * Set admission queue maximum size ratio for a given cache name
   * @param cacheName cache name
   * @param ratio AQ maximum size ratio relative to the maximum cache size
   */
  public void setAdmissionQueueMaxSizeRatio(String cacheName, double ratio) {
    props.setProperty(cacheName + "." + ADMISSION_QUEUE_MAX_SIZE_RATIO_KEY, Double.toString(ratio));
  }

  /**
   * Get promotion queue start size ratio for a given cache name
   * @param cacheName cache name
   * @return AQ start size ratio relative to the maximum cache size
   */
  public double getPromotionQueueStartSizeRatio(String cacheName) {
    String value = props.getProperty(cacheName + "." + PROMOTION_QUEUE_START_SIZE_RATIO_KEY);
    if (value != null) {
      return Double.parseDouble(value);
    }
    return getDoubleProperty(PROMOTION_QUEUE_START_SIZE_RATIO_KEY,
      DEFAULT_PROMOTION_QUEUE_START_SIZE_RATIO);
  }

  /**
   * Set promotion queue start size ratio for a given cache name
   * @param cacheName cache name
   * @param ratio AQ start size ratio relative to the maximum cache size
   */
  public void setPromotionQueueStartSizeRatio(String cacheName, double ratio) {
    props.setProperty(cacheName + "." + PROMOTION_QUEUE_START_SIZE_RATIO_KEY,
      Double.toString(ratio));
  }

  /**
   * Get admission queue minimum size ratio for a given cache name
   * @param cacheName cache name
   * @return AQ minimum size ratio relative to the maximum cache size
   */
  public double getPromotionQueueMinSizeRatio(String cacheName) {
    String value = props.getProperty(cacheName + "." + PROMOTION_QUEUE_MIN_SIZE_RATIO_KEY);
    if (value != null) {
      return Double.parseDouble(value);
    }
    return getDoubleProperty(PROMOTION_QUEUE_MIN_SIZE_RATIO_KEY,
      DEFAULT_PROMOTION_QUEUE_MIN_SIZE_RATIO);
  }

  /**
   * Set promotion queue minimum size ratio for a given cache name
   * @param cacheName cache name
   * @param ratio AQ minimum size ratio relative to the maximum cache size
   */
  public void setPromotionQueueMinSizeRatio(String cacheName, double ratio) {
    props.setProperty(cacheName + "." + PROMOTION_QUEUE_MIN_SIZE_RATIO_KEY, Double.toString(ratio));
  }

  /**
   * Get promotion queue maximum size ratio for a given cache name
   * @param cacheName cache name
   * @return AQ maximum size ratio relative to the maximum cache size
   */
  public double getPromotionQueueMaxSizeRatio(String cacheName) {
    String value = props.getProperty(cacheName + "." + PROMOTION_QUEUE_MAX_SIZE_RATIO_KEY);
    if (value != null) {
      return Double.parseDouble(value);
    }
    return getDoubleProperty(PROMOTION_QUEUE_MAX_SIZE_RATIO_KEY,
      DEFAULT_PROMOTION_QUEUE_MAX_SIZE_RATIO);
  }

  /**
   * Set promotion queue maximum size ratio for a given cache name
   * @param cacheName cache name
   * @param ratio AQ maximum size ratio relative to the maximum cache size
   */
  public void setPromotionQueueMaxSizeRatio(String cacheName, double ratio) {
    props.setProperty(cacheName + "." + PROMOTION_QUEUE_MAX_SIZE_RATIO_KEY, Double.toString(ratio));
  }

  /**
   * Get promotion probability for a given cache name
   * @param cacheName cache name
   * @return probability
   */
  public double getRandomPromotionProbability(String cacheName) {
    String value = props.getProperty(cacheName + "." + CACHE_RANDOM_PROMOTION_PROBABILITY_KEY);
    if (value != null) {
      return Double.parseDouble(value);
    }
    return getDoubleProperty(CACHE_RANDOM_PROMOTION_PROBABILITY_KEY,
      DEFAULT_CACHE_RANDOM_PROMOTION_PROBABILITY);
  }

  /**
   * Set promotion probability for a given key
   * @param cacheName cache name
   * @param probability
   */
  public void setRandomPromotionProbability(String cacheName, double probability) {
    props.setProperty(cacheName + "." + PROMOTION_QUEUE_MAX_SIZE_RATIO_KEY,
      Double.toString(probability));
  }

  /**
   * Get throughput check interval for a given cache name
   * @param cacheName cache name
   * @return interval in ms for a given cache name
   */
  public long getThroughputCheckInterval(String cacheName) {
    String value = props.getProperty(cacheName + "." + THROUGHPUT_CHECK_INTERVAL_SEC_KEY);
    if (value != null) {
      return Long.parseLong(value.replaceAll(CHARS_TO_REMOVE_REGEX, "")) * 1000;
    }
    return getLongProperty(THROUGHPUT_CHECK_INTERVAL_SEC_KEY, DEFAULT_THROUGHPUT_CHECK_INTERVAL_SEC)
        * 1000;
  }

  /**
   * Set throughput check interval for a given cache name
   * @param cacheName cache name
   * @param interval in sec for a given cache name
   */
  public void setThroughputCheckInterval(String cacheName, int interval) {
    props.setProperty(cacheName + "." + THROUGHPUT_CHECK_INTERVAL_SEC_KEY, Long.toString(interval));
  }

  /**
   * Get vacuum cleaner interval for a given cache name
   * @param cacheName cache name
   * @return interval in ms for a given cache name
   */
  public long getVacuumCleanerInterval(String cacheName) {
    String value = props.getProperty(cacheName + "." + VACUUM_CLEANER_INTERVAL_SEC_KEY);
    if (value != null) {
      return Long.parseLong(value.replaceAll(CHARS_TO_REMOVE_REGEX, "")) * 1000;
    }
    return getLongProperty(VACUUM_CLEANER_INTERVAL_SEC_KEY, DEFAULT_VACUUM_CLEANER_INTERVAL_SEC)
        * 1000;
  }

  /**
   * Set vacuum cleaner interval for a given cache name
   * @param cacheName cache name
   * @param interval in sec for a given cache name
   */
  public void setVacuumCleanerInterval(String cacheName, int interval) {
    props.setProperty(cacheName + "." + VACUUM_CLEANER_INTERVAL_SEC_KEY, Long.toString(interval));
  }

  /**
   * Get Scavenger run interval for a given cache name
   * @param cacheName cache name
   * @return interval in seconds for a given cache name
   */
  public long getScavengerRunInterval(String cacheName) {
    String value = props.getProperty(cacheName + "." + SCAVENGER_RUN_INTERVAL_SEC_KEY);
    if (value != null) {
      return Long.parseLong(value.replaceAll(CHARS_TO_REMOVE_REGEX, "")) * 1000;
    }
    return getLongProperty(SCAVENGER_RUN_INTERVAL_SEC_KEY, DEFAULT_SCAVENGER_RUN_INTERVAL_SEC);
  }

  /**
   * Set Scavenger run interval for a given cache name
   * @param cacheName cache name
   * @param interval in seconds for a given cache name
   */
  public void setScavengerRunInterval(String cacheName, int interval) {
    props.setProperty(cacheName + "." + SCAVENGER_RUN_INTERVAL_SEC_KEY, Long.toString(interval));
  }

  /**
   * Get throughput controller tolerance limit for a given cache name
   * @param cacheName cache name
   * @return limit for a given cache name
   */
  public double getThroughputToleranceLimit(String cacheName) {
    String value = props.getProperty(cacheName + "." + THROUGHPUT_CONTROLLER_TOLERANCE_LIMIT_KEY);
    if (value != null) {
      return Double.parseDouble(value);
    }
    return getDoubleProperty(THROUGHPUT_CONTROLLER_TOLERANCE_LIMIT_KEY,
      DEFAULT_THROUGHPUT_CONTROLLER_TOLERANCE_LIMIT);
  }

  /**
   * Set throughput controller tolerance limit for a given cache name
   * @param cacheName cache name
   * @param limit for a given cache name
   */
  public void setThroughputToleranceLimit(String cacheName, double limit) {
    props.setProperty(cacheName + "." + THROUGHPUT_CONTROLLER_TOLERANCE_LIMIT_KEY,
      Double.toString(limit));
  }

  /**
   * Get cache write rate limit for a given cache name
   * @param cacheName cache name
   * @return cache write rate limit for a given cache name
   */
  public long getCacheWriteLimit(String cacheName) {
    String value = props.getProperty(cacheName + "." + CACHE_WRITE_RATE_LIMIT_KEY);
    if (value != null) {
      return getLong(value, DEFAULT_CACHE_WRITE_RATE_LIMIT);
    }
    return getLongProperty(CACHE_WRITE_RATE_LIMIT_KEY, DEFAULT_CACHE_WRITE_RATE_LIMIT);
  }

  /**
   * Set cache write rate limit for a given cache name
   * @param cacheName cache name
   * @param limit cache write rate limit for a given cache name
   */
  public void setCacheWriteLimit(String cacheName, long limit) {
    props.setProperty(cacheName + "." + CACHE_WRITE_RATE_LIMIT_KEY, Long.toString(limit));
  }

  /**
   * Get cache names list
   * @return cache names list
   */
  public String[] getCacheNames() {
    String s = props.getProperty(CACHES_NAME_LIST_KEY, DEFAULT_CACHES_NAME_LIST);
    return s.split(",");
  }

  public void setCacheNames(String names) {
    props.setProperty(CACHES_NAME_LIST_KEY, names);
  }

  public void addCacheNameType(String name, String type) {
    String s = props.getProperty(CACHES_NAME_LIST_KEY, DEFAULT_CACHES_NAME_LIST);
    s += ",";
    s += name;
    props.setProperty(CACHES_NAME_LIST_KEY, s);
    s = props.getProperty(CACHES_TYPES_LIST_KEY, DEFAULT_CACHES_TYPES_LIST);
    s += ",";
    s += type;
    props.setProperty(CACHES_TYPES_LIST_KEY, s);
  }
  
  /**
   * Get cache types list
   * @return cache types list
   */
  public String[] getCacheTypes() {
    String s = props.getProperty(CACHES_TYPES_LIST_KEY, DEFAULT_CACHES_TYPES_LIST);
    return s.split(",");
  }

  public void setCacheTypes(String types) {
    props.setProperty(CACHES_TYPES_LIST_KEY, types);
  }

  /**
   * Get victim cache name for cache
   * @param cacheName cache name
   * @return victim cache name
   */
  public String getVictimCacheName(String cacheName) {
    return props.getProperty(cacheName + "." + CACHE_VICTIM_NAME_KEY);
  }

  /**
   * Get cache throughput controller number of adjustment steps
   * @param cacheName cache name
   * @return throughput controller number of adjustment steps
   */
  public int getThrougputControllerNumberOfAdjustmentSteps(String cacheName) {
    String value = props.getProperty(cacheName + "." + THROUGHPUT_CONTROLLER_ADJUSTMENT_STEPS_KEY);
    if (value != null) {
      return (int) Long.parseLong(value.replaceAll(CHARS_TO_REMOVE_REGEX, ""));
    }
    return (int) getLongProperty(THROUGHPUT_CONTROLLER_ADJUSTMENT_STEPS_KEY,
      DEFAULT_THROUGHPUT_CONTROLLER_ADJUSTMENT_STEPS);
  }

  /**
   * Set throughput controller number of adjustment steps for cache
   * @param cacheName cache name
   * @param n throughput controller number of adjustment steps
   */
  public void setThrougputControllerNumberOfAdjustmentSteps(String cacheName, int n) {
    props.setProperty(cacheName + "." + THROUGHPUT_CONTROLLER_ADJUSTMENT_STEPS_KEY,
      Long.toString(n));
  }

  /**
   * Is index data embedding supported
   * @param cacheName cache name
   * @return true or false
   */
  public boolean isIndexDataEmbeddedSupported(String cacheName) {
    return getBooleanProperty(cacheName + "." + INDEX_DATA_EMBEDDED_KEY,
      DEFAULT_INDEX_DATA_EMBEDDED);
  }

  /**
   * Set index data embedding supported
   * @param cacheName cache name
   * @param v true or false
   */
  public void setIndexDataEmbeddedSupported(String cacheName, boolean v) {
    props.setProperty(cacheName + "." + INDEX_DATA_EMBEDDED_KEY, Boolean.toString(v));
  }

  /**
   * Get data embedded size
   * @param cacheName cache name
   * @return data embedded size
   */
  public int getIndexDataEmbeddedSize(String cacheName) {
    return (int) getLongProperty(cacheName + "." + INDEX_DATA_EMBEDDED_SIZE_KEY,
      DEFAULT_INDEX_DATA_EMBEDDED_SIZE);
  }

  /**
   * Set data embedded size
   * @param cacheName cache name
   * @param v data embedded size
   */
  public void setIndexDataEmbeddedSize(String cacheName, int v) {
    props.setProperty(cacheName + "." + INDEX_DATA_EMBEDDED_SIZE_KEY, Integer.toString(v));
  }

  /**
   * Get admission queue index format implementation
   * @param cacheName cache name
   * @return index format
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
   * Set admission queue index format implementation
   * @param cacheName cache name
   * @param className index format class name
   */
  public void setAdmissionQueueIndexFormat(String cacheName, String className) {
    props.setProperty(cacheName + "." + INDEX_FORMAT_ADMISSION_QUEUE_IMPL_KEY, className);
  }

  /**
   * Get main queue index format implementation
   * @param cacheName cache name
   * @return index format
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
      return new BaseIndexFormat();
    }
    @SuppressWarnings("unchecked")
    Class<IndexFormat> clz = (Class<IndexFormat>) Class.forName(value);
    IndexFormat instance = clz.newInstance();
    instance.setCacheName(cacheName);
    return instance;
  }

  /**
   * Set main queue index format implementation
   * @param cacheName cache name
   * @param className index format class name
   */
  public void setMainQueueIndexFormat(String cacheName, String className) {
    props.setProperty(cacheName + "." + INDEX_FORMAT_MAIN_QUEUE_IMPL_KEY, className);
  }

  /**
   * Get cache eviction policy implementation by cache name
   * @param cacheName cache name
   * @return eviction policy
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
   * Set cache eviction policy implementation by cache name
   * @param cacheName cache name
   * @param className eviction policy class name
   */
  public void setCacheEvictionPolicy(String cacheName, String className) {
    props.setProperty(cacheName + "." + CACHE_EVICTION_POLICY_IMPL_KEY, className);
  }

  /**
   * Get cache admission controller implementation by cache name
   * @param cacheName cache name
   * @return admission controller
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
      return null;
    }
    @SuppressWarnings("unchecked")
    Class<AdmissionController> clz = (Class<AdmissionController>) Class.forName(value);
    AdmissionController instance = clz.newInstance();
    return instance;
  }

  /**
   * Set cache admission controller implementation by cache name
   * @param cacheName cache name
   * @param className admission controller class name
   */
  public void setAdmissionController(String cacheName, String className) {
    props.setProperty(cacheName + "." + CACHE_ADMISSION_CONTROLLER_IMPL_KEY, className);
  }

  /**
   * Get cache promotion controller implementation by cache name
   * @param cacheName cache name
   * @return promotion controller instance
   * @throws ClassNotFoundException
   * @throws IllegalAccessException
   * @throws InstantiationException
   */
  public PromotionController getPromotionController(String cacheName)
      throws ClassNotFoundException, InstantiationException, IllegalAccessException {
    String value = props.getProperty(cacheName + "." + CACHE_PROMOTION_CONTROLLER_IMPL_KEY);
    if (value == null) {
      value = props.getProperty(CACHE_PROMOTION_CONTROLLER_IMPL_KEY);
    }
    if (value == null) {
      // default implementation;
      return null;
    }
    @SuppressWarnings("unchecked")
    Class<PromotionController> clz = (Class<PromotionController>) Class.forName(value);
    PromotionController instance = clz.newInstance();
    return instance;
  }

  /**
   * Set cache promotion controller implementation by cache name
   * @param cacheName cache name
   * @param className admission controller class name
   */
  public void setPromotionController(String cacheName, String className) {
    props.setProperty(cacheName + "." + CACHE_PROMOTION_CONTROLLER_IMPL_KEY, className);
  }

  /**
   * Get cache throughput controller implementation by cache name
   * @param cacheName cache name
   * @return throughput controller
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
      // No defaults
      return null;
    }
    @SuppressWarnings("unchecked")
    Class<ThroughputController> clz = (Class<ThroughputController>) Class.forName(value);
    ThroughputController instance = clz.newInstance();
    return instance;
  }

  /**
   * Set cache throughput controller implementation by cache name
   * @param cacheName cache name
   * @param className throughput controller class name
   */
  public void setThroughputController(String cacheName, String className) {
    props.setProperty(cacheName + "." + CACHE_THROUGHPUT_CONTROLLER_IMPL_KEY, className);
  }

  /**
   * Get Scavenger recycling selector implementation by cache name
   * @param cacheName cache name
   * @return recycling selector
   * @throws ClassNotFoundException
   * @throws IllegalAccessException
   * @throws InstantiationException
   */
  public RecyclingSelector getRecyclingSelector(String cacheName)
      throws ClassNotFoundException, InstantiationException, IllegalAccessException {
    String value = props.getProperty(cacheName + "." + CACHE_RECYCLING_SELECTOR_IMPL_KEY);
    if (value == null) {
      value = props.getProperty(CACHE_RECYCLING_SELECTOR_IMPL_KEY,
        DEFAULT_CACHE_RECYCLING_SELECTOR_IMPL);
    }

    @SuppressWarnings("unchecked")
    Class<RecyclingSelector> clz = (Class<RecyclingSelector>) Class.forName(value);
    RecyclingSelector instance = clz.newInstance();
    return instance;
  }

  /**
   * Set Scavenger recycling selector implementation by cache name
   * @param cacheName cache name
   * @param className recycling selector class name
   */
  public void setRecyclingSelector(String cacheName, String className) {
    props.setProperty(cacheName + "." + CACHE_RECYCLING_SELECTOR_IMPL_KEY, className);
  }

  /**
   * Get segment data writer (appender) implementation by cache name
   * @param cacheName cache name
   * @return data writer
   * @throws ClassNotFoundException
   * @throws IllegalAccessException
   * @throws InstantiationException
   */
  public DataWriter getDataWriter(String cacheName)
      throws ClassNotFoundException, InstantiationException, IllegalAccessException {
    String value = props.getProperty(cacheName + "." + CACHE_DATA_WRITER_IMPL_KEY);
    if (value == null) {
      value = props.getProperty(CACHE_DATA_WRITER_IMPL_KEY, DEFAULT_CACHE_DATA_WRITER_IMPL);
    }
    @SuppressWarnings("unchecked")
    Class<DataWriter> clz = (Class<DataWriter>) Class.forName(value);
    DataWriter instance = clz.newInstance();
    instance.init(cacheName);
    return instance;
  }

  /**
   * Set segment data writer (appender) implementation by cache name
   * @param cacheName cache name
   * @param className data writer class name
   */
  public void setDataWriter(String cacheName, String className) {
    props.setProperty(cacheName + "." + CACHE_DATA_WRITER_IMPL_KEY, className);
  }

  /**
   * Get segment data reader implementation (Memory) by cache name
   * @param cacheName cache name
   * @return data reader
   * @throws ClassNotFoundException
   * @throws IllegalAccessException
   * @throws InstantiationException
   */
  public DataReader getMemoryDataReader(String cacheName)
      throws ClassNotFoundException, InstantiationException, IllegalAccessException {
    String value = props.getProperty(cacheName + "." + CACHE_MEMORY_DATA_READER_IMPL_KEY);
    if (value == null) {
      value = props.getProperty(CACHE_MEMORY_DATA_READER_IMPL_KEY,
        DEFAULT_CACHE_MEMORY_DATA_READER_IMPL);
    }
    @SuppressWarnings("unchecked")
    Class<DataReader> clz = (Class<DataReader>) Class.forName(value);
    DataReader instance = clz.newInstance();
    instance.init(cacheName);
    return instance;
  }

  /**
   * Set segment data reader implementation (Memory) by cache name
   * @param cacheName cache name
   * @param className data reader class name
   */
  public void setMemoryDataReader(String cacheName, String className) {
    props.setProperty(cacheName + "." + CACHE_MEMORY_DATA_READER_IMPL_KEY, className);
  }

  /**
   * Get segment data reader implementation (File) by cache name
   * @param cacheName cache name
   * @throws ClassNotFoundException
   * @throws IllegalAccessException
   * @throws InstantiationException
   */
  public DataReader getFileDataReader(String cacheName)
      throws ClassNotFoundException, InstantiationException, IllegalAccessException {
    String value = props.getProperty(cacheName + "." + CACHE_FILE_DATA_READER_IMPL_KEY);
    if (value == null) {
      value =
          props.getProperty(CACHE_FILE_DATA_READER_IMPL_KEY, DEFAULT_CACHE_FILE_DATA_READER_IMPL);
    }
    @SuppressWarnings("unchecked")
    Class<DataReader> clz = (Class<DataReader>) Class.forName(value);
    DataReader instance = clz.newInstance();
    instance.init(cacheName);
    return instance;
  }

  /**
   * Set segment data reader implementation (File) by cache name
   * @param cacheName cache name
   */
  public void setFileDataReader(String cacheName, String className) {
    props.setProperty(cacheName + "." + CACHE_FILE_DATA_READER_IMPL_KEY, className);
  }

  /**
   * Get block writer block size by cache name
   * @param cacheName cache name
   * @return block size
   */
  public int getBlockWriterBlockSize(String cacheName) {
    String value = props.getProperty(cacheName + "." + CACHE_BLOCK_WRITER_BLOCK_SIZE_KEY);
    if (value == null) {
      return (int) getLongProperty(CACHE_BLOCK_WRITER_BLOCK_SIZE_KEY,
        DEFAULT_CACHE_BLOCK_WRITER_BLOCK_SIZE);
    } else {
      return (int) getLong(value, DEFAULT_CACHE_BLOCK_WRITER_BLOCK_SIZE);
    }
  }

  /**
   * Set block writer block size by cache name
   * @param cacheName cache name
   * @param size block size
   */
  public void setBlockWriterBlockSize(String cacheName, int size) {
    props.setProperty(cacheName + "." + CACHE_BLOCK_WRITER_BLOCK_SIZE_KEY, Integer.toString(size));
  }

  /**
   * Get file prefetch buffer by cache name
   * @param cacheName cache name
   * @return prefetch buffer size
   */
  public int getFilePrefetchBufferSize(String cacheName) {
    String value = props.getProperty(cacheName + "." + FILE_PREFETCH_BUFFER_SIZE_KEY);
    if (value == null) {
      return (int) getLongProperty(FILE_PREFETCH_BUFFER_SIZE_KEY,
        DEFAULT_FILE_PREFETCH_BUFFER_SIZE);
    } else {
      return (int) getLong(value, DEFAULT_FILE_PREFETCH_BUFFER_SIZE);
    }
  }

  /**
   * Set file prefetch buffer by cache name
   * @param cacheName cache name
   * @param prefetch buffer size
   */
  public void setFilePrefetchBufferSize(String cacheName, int prefetch) {
    props.setProperty(cacheName + "." + FILE_PREFETCH_BUFFER_SIZE_KEY, Integer.toString(prefetch));
  }

  /**
   * Get expiration support implementation by cache name
   * @param cacheName cache name
   * @return expire support implementation
   */
  public ExpireSupport getExpireSupport(String cacheName)
      throws ClassNotFoundException, InstantiationException, IllegalAccessException {
    String value = props.getProperty(cacheName + "." + CACHE_EXPIRE_SUPPORT_IMPL_KEY);
    if (value == null) {
      value = props.getProperty(CACHE_EXPIRE_SUPPORT_IMPL_KEY, DEFAULT_CACHE_EXPIRE_SUPPORT_IMPL);
    }

    @SuppressWarnings("unchecked")
    Class<ExpireSupport> clz = (Class<ExpireSupport>) Class.forName(value);
    ExpireSupport instance = clz.newInstance();
    return instance;
  }

  /**
   * Set expiration support implementation by cache name
   * @param cacheName cache name
   * @param className expire support class name
   */
  public void setExpireSupport(String cacheName, String className) {
    props.setProperty(cacheName + "." + CACHE_EXPIRE_SUPPORT_IMPL_KEY, className);
  }

  /**
   * Get start bin value for expiration - based admission controller
   * @param cacheName cache name
   * @return value
   */
  public int getExpireStartBinValue(String cacheName) {
    String value = props.getProperty(cacheName + "." + CACHE_EXPIRATION_BIN_START_VALUE_KEY);
    if (value == null) {
      return (int) getLongProperty(CACHE_EXPIRATION_BIN_START_VALUE_KEY,
        DEFAULT_CACHE_EXPIRATION_BIN_START_VALUE);
    } else {
      return Integer.parseInt(value.replaceAll(CHARS_TO_REMOVE_REGEX, ""));
    }
  }

  /**
   * Set start bin value for expiration - based admission controller
   * @param cacheName cache name
   * @param value (in seconds)
   */
  public void setExpireStartBinValue(String cacheName, int value) {
    props.setProperty(cacheName + "." + CACHE_EXPIRATION_BIN_START_VALUE_KEY,
      Integer.toString(value));
  }

  /**
   * Get expiration bin multiplier
   * @param cacheName cache name
   * @return value
   */
  public double getExpireBinMultiplier(String cacheName) {
    String value = props.getProperty(cacheName + "." + CACHE_EXPIRATION_MULTIPLIER_VALUE_KEY);
    if (value == null) {
      return getDoubleProperty(CACHE_EXPIRATION_MULTIPLIER_VALUE_KEY,
        DEFAULT_CACHE_EXPIRATION_MULTIPLIER_VALUE);
    } else {
      return Double.parseDouble(value);
    }
  }

  /**
   * Set expiration bin multiplier
   * @param cacheName cache name
   * @param multiplier value multiplier
   */
  public void setExpireBinMultiplier(String cacheName, double multiplier) {
    props.setProperty(cacheName + "." + CACHE_EXPIRATION_MULTIPLIER_VALUE_KEY,
      Double.toString(multiplier));
  }

  /**
   * Get minimum cache active data set threshold
   * @param cacheName cache name
   * @return value
   */
  public double getMinimumActiveDatasetRatio(String cacheName) {
    String value = props.getProperty(cacheName + "." + CACHE_MINIMUM_ACTIVE_DATA_SET_RATIO_KEY);
    if (value == null) {
      return getDoubleProperty(CACHE_MINIMUM_ACTIVE_DATA_SET_RATIO_KEY,
        DEFAULT_CACHE_MINIMUM_ACTIVE_DATA_SET_RATIO);
    } else {
      return Double.parseDouble(value);
    }
  }

  /**
   * Set minimum cache active data set threshold
   * @param cacheName cache name
   * @param ratio minimum cache active data set threshold
   */
  public void setMinimumActiveDatasetRatio(String cacheName, double ratio) {
    props.setProperty(cacheName + "." + CACHE_MINIMUM_ACTIVE_DATA_SET_RATIO_KEY,
      Double.toString(ratio));
  }

  /**
   * Get eviction disabled mode
   * @param cacheName cache name
   * @return true or false
   */
  public boolean getEvictionDisabledMode(String cacheName) {
    String value = props.getProperty(cacheName + "." + CACHE_EVICTION_DISABLED_MODE_KEY);
    if (value == null) {
      return getBooleanProperty(CACHE_EVICTION_DISABLED_MODE_KEY,
        DEFAULT_CACHE_EVICTION_DISABLED_MODE);
    } else {
      return Boolean.parseBoolean(value);
    }
  }

  /**
   * Set eviction disabled mode
   * @param cacheName cache name
   * @param mode true or false
   */
  public void setEvictionDisabledMode(String cacheName, boolean mode) {
    props.setProperty(cacheName + "." + CACHE_EVICTION_DISABLED_MODE_KEY, Boolean.toString(mode));
  }

  /**
   * Get I/O storage pool size - should be equal or greater than the maximum number of threads
   * serving IO requests in an application
   * @param cacheName cache name
   * @return size
   */
  public int getIOStoragePoolSize(String cacheName) {
    String value = props.getProperty(cacheName + "." + CACHE_IO_STORAGE_POOL_SIZE_KEY);
    if (value == null) {
      return (int) getLongProperty(CACHE_IO_STORAGE_POOL_SIZE_KEY,
        DEFAULT_CACHE_IO_STORAGE_POOL_SIZE);
    } else {
      return Integer.parseInt(value.replaceAll(CHARS_TO_REMOVE_REGEX, ""));
    }
  }

  /**
   * Set I/O storage pool size
   * @param cacheName cache name
   * @param n size
   */
  public void setIOStoragePoolSize(String cacheName, int n) {
    props.setProperty(cacheName + "." + CACHE_IO_STORAGE_POOL_SIZE_KEY, Integer.toString(n));
  }

  /**
   * Get promotion on hit for victim cache
   * @param cacheName cache name
   * @return true promote to main cache, false - do not
   */
  public boolean getVictimCachePromotionOnHit(String cacheName) {
    String value = props.getProperty(cacheName + "." + CACHE_VICTIM_PROMOTION_ON_HIT_KEY);
    if (value == null) {
      return getBooleanProperty(CACHE_VICTIM_PROMOTION_ON_HIT_KEY,
        DEFAULT_CACHE_VICTIM_PROMOTION_ON_HIT);
    } else {
      return Boolean.parseBoolean(value);
    }
  }

  /**
   * Set promotion on hit for victim cache
   * @param cacheName cache name
   * @param v true or false
   */
  public void setVictimCachePromotionOnHit(String cacheName, boolean v) {
    props.setProperty(cacheName + "." + CACHE_VICTIM_PROMOTION_ON_HIT_KEY, Boolean.toString(v));
  }

  /**
   * Get hybrid cache inverse mode
   * @param cacheName cache name
   * @return true if inverse mode ON, false - otherwise
   */
  public boolean getCacheHybridInverseMode(String cacheName) {
    String value = props.getProperty(cacheName + "." + CACHE_HYBRID_INVERSE_MODE_KEY);
    if (value == null) {
      return getBooleanProperty(CACHE_HYBRID_INVERSE_MODE_KEY, DEFAULT_CACHE_HYBRID_INVERSE_MODE);
    } else {
      return Boolean.parseBoolean(value);
    }
  }

  /**
   * Set hybrid cache inverse mode
   * @param cacheName cache name
   * @param v true or false
   */
  public void setCacheHybridInverseMode(String cacheName, boolean v) {
    props.setProperty(cacheName + "." + CACHE_HYBRID_INVERSE_MODE_KEY, Boolean.toString(v));
  }

  /**
   * Get victim cache promotion threshold
   * @param cacheName cache name
   * @return threshold
   */
  public double getVictimPromotionThreshold(String cacheName) {
    String value = props.getProperty(cacheName + "." + CACHE_VICTIM_PROMOTION_THRESHOLD_KEY);
    if (value == null) {
      return getDoubleProperty(CACHE_VICTIM_PROMOTION_THRESHOLD_KEY,
        DEFAULT_CACHE_VICTIM_PROMOTION_THRESHOLD);
    } else {
      return Double.parseDouble(value);
    }
  }

  /**
   * Set victim cache promotion threshold
   * @param cacheName cache name
   * @param v promotion threshold
   */
  public void setVictimPromotionThreshold(String cacheName, double v) {
    props.setProperty(cacheName + "." + CACHE_VICTIM_PROMOTION_THRESHOLD_KEY, Double.toString(v));
  }

  /**
   * Get cache spin wait time on high pressure (in nanoseconds)
   * @param cacheName cache name
   * @return time
   */
  public long getCacheSpinWaitTimeOnHighPressure(String cacheName) {
    String value = props.getProperty(cacheName + "." + CACHE_SPIN_WAIT_TIME_NS_KEY);
    if (value == null) {
      return getLongProperty(CACHE_SPIN_WAIT_TIME_NS_KEY, DEFAULT_CACHE_SPIN_WAIT_TIME_NS);
    } else {
      return Long.parseLong(value.replaceAll(CHARS_TO_REMOVE_REGEX, ""));
    }
  }

  /**
   * Set cache spin wait time on high pressure (in nanoseconds)
   * @param cacheName cache name
   * @param v wait time
   */
  public void setCacheSpinWaitTimeOnHighPressure(String cacheName, long v) {
    props.setProperty(cacheName + "." + CACHE_SPIN_WAIT_TIME_NS_KEY, Long.toString(v));
  }

  /**
   * Get JMX metrics domain name
   * @return domain name
   */
  public String getJMXMetricsDomainName() {
    String value = props.getProperty(CACHE_JMX_METRICS_DOMAIN_NAME_KEY);
    if (value == null) {
      value = DEFAULT_CACHE_JMX_METRICS_DOMAIN_NAME;
    }
    return value;
  }

  /**
   * Sets JMX metrics domain name
   * @param name domain name
   */
  public void setJMXMetricsDomainName(String name) {
    props.setProperty(CACHE_JMX_METRICS_DOMAIN_NAME_KEY, name);
  }

  /**
   * Sets JMX metrics enabled
   * @param cacheName cache name
   * @param v true or false
   */
  public void setJMXMetricsEnabled(String cacheName, boolean v) {
    props.setProperty(cacheName + "." + CACHE_JMX_METRICS_ENABLED_KEY, Boolean.toString(v));
  }
  
  /**
   * Get JMX metrics enabled
   * @param cacheName cache name
   * @return true or false
   */
  public boolean getJMXMetricsEnabled(String cacheName) {
    String value = props.getProperty(cacheName + "." + CACHE_JMX_METRICS_ENABLED_KEY);
    if (value == null) {
      return getBooleanProperty(CACHE_JMX_METRICS_ENABLED_KEY, DEFAULT_CACHE_JMX_METRICS_ENABLED);
    } else {
      return Boolean.parseBoolean(value);
    }
  }
  
  public int getCacheStreamingSupportBufferSize(String cacheName) {
    String value = props.getProperty(cacheName + "." + CACHE_STREAMING_SUPPORT_BUFFER_SIZE_KEY);
    if (value == null) {
      return (int) getLongProperty(CACHE_STREAMING_SUPPORT_BUFFER_SIZE_KEY,
        DEFAULT_CACHE_STREAMING_SUPPORT_BUFFER_SIZE);
    } else {
      return (int) getLong(value, DEFAULT_CACHE_STREAMING_SUPPORT_BUFFER_SIZE);
    }
  }

  /**
   * Cache streaming support buffer size
   * @param size
   */
  public void setCacheStreamingSupportBufferSize(int size) {
    this.props.setProperty(CACHE_STREAMING_SUPPORT_BUFFER_SIZE_KEY, Integer.toString(size));
  }

  /**
   * Cache streaming support number threads
   * @param cacheName
   * @param size
   */
  public void setCacheStreamingSupportBufferSize(String cacheName, int size) {
    this.props.setProperty(cacheName + "." + CACHE_STREAMING_SUPPORT_BUFFER_SIZE_KEY,
      Integer.toString(size));
  }

  /**
   * Get properties
   * @return properties
   */
  public Properties getProperties() {
    return props;
  }

  /**
   * Get Scavenger number of threads for the cache
   * @param cacheName cache name
   * @return number of threads
   */
  public int getScavengerNumberOfThreads(String cacheName) {
    String value = props.getProperty(cacheName + "." + SCAVENGER_NUMBER_THREADS_KEY);
    if (value == null) {
      return (int) getLongProperty(SCAVENGER_NUMBER_THREADS_KEY, DEFAULT_SCAVENGER_NUMBER_THREADS);
    } else {
      return Integer.parseInt(value.replaceAll(CHARS_TO_REMOVE_REGEX, ""));
    }
  }

  /**
   * Set scavenger number of threads
   * @param cacheName cache name
   * @param threads number
   */
  public void setScavengerNumberOfThreads(String cacheName, int threads) {
    this.props.setProperty(cacheName + "." + SCAVENGER_NUMBER_THREADS_KEY,
      Integer.toString(threads));
  }

  /**
   * Get maximum wait on PUT time in ms
   * @param cacheName cache name
   * @return wait time in ms
   */
  public long getCacheMaximumWaitTimeOnPut(String cacheName) {
    String value = props.getProperty(cacheName + "." + CACHE_MAX_WAIT_ON_PUT_MS_KEY);
    if (value == null) {
      return getLongProperty(CACHE_MAX_WAIT_ON_PUT_MS_KEY, DEFAULT_CACHE_MAX_WAIT_ON_PUT_MS);
    }
    return Long.parseLong(value.replaceAll(CHARS_TO_REMOVE_REGEX, ""));
  }

  /**
   * Set maximum wait time on PUT for a given cache
   * @param cacheName cache name
   * @param time wait time
   */
  public void setCacheMaximumWaitTimeOnPut(String cacheName, long time) {
    this.props.setProperty(cacheName + "." + CACHE_MAX_WAIT_ON_PUT_MS_KEY, Long.toString(time));
  }

  /**
   * Get maximum cached key-value size
   * @param cacheName cache name
   * @return maximum size, 0 - means, maximum size is defined by data segment size
   */
  public int getKeyValueMaximumSize(String cacheName) {
    String value = props.getProperty(cacheName + "." + CACHE_MAX_KEY_VALUE_SIZE_KEY);
    if (value != null) {
      return (int) getLong(value, DEFAULT_CACHE_MAX_KEY_VALUE_SIZE);
    }
    return (int) getLongProperty(CACHE_MAX_KEY_VALUE_SIZE_KEY, DEFAULT_CACHE_MAX_KEY_VALUE_SIZE);
  }

  /**
   * Sets cache maximum key-value size in bytes
   * @param cacheName cache name
   * @param size maximum size
   */
  public void setKeyValueMaximumSize(String cacheName, int size) {
    props.setProperty(cacheName + "." + CACHE_MAX_KEY_VALUE_SIZE_KEY, Integer.toString(size));
  }

  /**
   * Get object cache initial output buffer size
   * @param cacheName cache name
   * @return initial size
   */
  public int getObjectCacheInitialOutputBufferSize(String cacheName) {
    String value = props.getProperty(cacheName + "." + OBJECT_CACHE_INITIAL_BUFFER_SIZE_KEY);
    if (value != null) {
      return (int) getLong(value, DEFAULT_OBJECT_CACHE_INITIAL_BUFFER_SIZE);
    }
    return (int) getLongProperty(OBJECT_CACHE_INITIAL_BUFFER_SIZE_KEY,
      DEFAULT_OBJECT_CACHE_INITIAL_BUFFER_SIZE);
  }

  /**
   * Sets object cache initial output buffer size
   * @param cacheName cache name
   * @param size initial size
   */
  public void setObjectCacheInitialOutputBufferSize(String cacheName, int size) {
    props.setProperty(cacheName + "." + OBJECT_CACHE_INITIAL_BUFFER_SIZE_KEY,
      Integer.toString(size));
  }

  /**
   * Get object cache maximum output buffer size
   * @param cacheName cache name
   * @return maximum size (-1 - unlimited)
   */
  public int getObjectCacheMaxOutputBufferSize(String cacheName) {
    String value = props.getProperty(cacheName + "." + OBJECT_CACHE_MAX_BUFFER_SIZE_KEY);
    if (value != null) {
      return (int) getLong(value, DEFAULT_OBJECT_CACHE_MAX_BUFFER_SIZE);
    }
    return (int) getLongProperty(OBJECT_CACHE_MAX_BUFFER_SIZE_KEY,
      DEFAULT_OBJECT_CACHE_MAX_BUFFER_SIZE);
  }

  /**
   * Sets object cache maximum output buffer size
   * @param cacheName cache name
   * @param size maximum size
   */
  public void setObjectCacheMaxOutputBufferSize(String cacheName, int size) {
    props.setProperty(cacheName + "." + OBJECT_CACHE_INITIAL_BUFFER_SIZE_KEY,
      Integer.toString(size));
  }

  /**
   * Get Thread-Local-Storage supported
   * @param cacheName cache name
   * @return true if supported, false - otherwise
   */
  public boolean isCacheTLSSupported(String cacheName) {
    String value = props.getProperty(cacheName + "." + CACHE_TLS_SUPPORTED_KEY);
    if (value == null) {
      return getBooleanProperty(CACHE_TLS_SUPPORTED_KEY, DEFAULT_CACHE_TLS_SUPPORTED);
    } else {
      return Boolean.parseBoolean(value);
    }
  }

  /**
   * Set Thread-Local-Storage supported
   * @param cacheName cache name
   * @param v true or false
   */
  public void setCacheTLSSupported(String cacheName, boolean v) {
    props.setProperty(cacheName + "." + CACHE_TLS_SUPPORTED_KEY, Boolean.toString(v));
  }

  /**
   * Get cache TLS initial buffer size
   * @param cacheName cache name
   * @return maximum size
   */
  public int getCacheTLSInitialBufferSize(String cacheName) {
    String value = props.getProperty(cacheName + "." + CACHE_TLS_INITIAL_BUFFER_SIZE_KEY);
    if (value != null) {
      return (int) getLong(value, DEFAULT_CACHE_TLS_INITIAL_BUFFER_SIZE);
    }
    return (int) getLongProperty(CACHE_TLS_INITIAL_BUFFER_SIZE_KEY,
      DEFAULT_CACHE_TLS_INITIAL_BUFFER_SIZE);
  }

  /**
   * Sets cache TLS initial buffer size
   * @param cacheName cache name
   * @param size maximum size
   */
  public void setCacheTLSInitialBufferSize(String cacheName, int size) {
    props.setProperty(cacheName + "." + CACHE_TLS_INITIAL_BUFFER_SIZE_KEY, Integer.toString(size));
  }

  /**
   * Get cache TLS maximum buffer size
   * @param cacheName cache name
   * @return maximum size
   */
  public int getCacheTLSMaxBufferSize(String cacheName) {
    String value = props.getProperty(cacheName + "." + CACHE_TLS_MAXIMUM_BUFFER_SIZE_KEY);
    if (value != null) {
      return (int) getLong(value, DEFAULT_CACHE_TLS_MAXIMUM_BUFFER_SIZE);
    }
    return (int) getLongProperty(CACHE_TLS_MAXIMUM_BUFFER_SIZE_KEY,
      DEFAULT_CACHE_TLS_MAXIMUM_BUFFER_SIZE);
  }

  /**
   * Sets cache TLS maximum buffer size
   * @param cacheName cache name
   * @param size maximum size
   */
  public void setCacheTLSMaxBufferSize(String cacheName, int size) {
    props.setProperty(cacheName + "." + CACHE_TLS_MAXIMUM_BUFFER_SIZE_KEY, Integer.toString(size));
  }

  /**
   * Get cache compression enabled
   * @param cacheName cache name
   * @return true if supported, false - otherwise
   */
  public boolean isCacheCompressionEnabled(String cacheName) {
    String value = props.getProperty(cacheName + "." + CACHE_COMPRESSION_ENABLED_KEY);
    if (value == null) {
      return getBooleanProperty(CACHE_COMPRESSION_ENABLED_KEY, DEFAULT_CACHE_COMPRESSION_ENABLED);
    } else {
      return Boolean.parseBoolean(value);
    }
  }

  /**
   * Set cache compression enabled
   * @param cacheName cache name
   * @param v true or false
   */
  public void setCacheCompressionEnabled(String cacheName, boolean v) {
    props.setProperty(cacheName + "." + CACHE_COMPRESSION_ENABLED_KEY, Boolean.toString(v));
  }

  /**
   * Get cache compression block size
   * @param cacheName cache name
   * @return block size
   */
  public int getCacheCompressionBlockSize(String cacheName) {
    if (isCacheCompressionEnabled(cacheName)) {
      String value = props.getProperty(cacheName + "." + CACHE_COMPRESSION_BLOCK_SIZE_KEY);
      if (value != null) {
        return (int) getLong(value, DEFAULT_CACHE_COMPRESSION_BLOCK_SIZE);
      }
      return (int) getLongProperty(CACHE_COMPRESSION_BLOCK_SIZE_KEY,
        DEFAULT_CACHE_COMPRESSION_BLOCK_SIZE);
    } else {
      return -1;
    }
  }

  /**
   * Sets cache compression block size
   * @param cacheName cache name
   * @param size block size
   */
  public void setCacheCompressionBlockSize(String cacheName, int size) {
    props.setProperty(cacheName + "." + CACHE_COMPRESSION_BLOCK_SIZE_KEY, Integer.toString(size));
  }

  /**
   * Get cache compression dictionary size
   * @param cacheName cache name
   * @return dictionary size
   */
  public int getCacheCompressionDictionarySize(String cacheName) {
    if (isCacheCompressionEnabled(cacheName)) {
      String value = props.getProperty(cacheName + "." + CACHE_COMPRESSION_DICTIONARY_SIZE_KEY);
      if (value != null) {
        return (int) getLong(value, DEFAULT_CACHE_COMPRESSION_DICTIONARY_SIZE);
      }
      return (int) getLongProperty(CACHE_COMPRESSION_DICTIONARY_SIZE_KEY,
        DEFAULT_CACHE_COMPRESSION_DICTIONARY_SIZE);
    } else {
      return -1;
    }
  }

  /**
   * Sets cache compression dictionary size
   * @param cacheName cache name
   * @param size dictionary size
   */
  public void setCacheCompressionDictionarySize(String cacheName, int size) {
    props.setProperty(cacheName + "." + CACHE_COMPRESSION_DICTIONARY_SIZE_KEY,
      Integer.toString(size));
  }

  /**
   * Get cache compression level
   * @param cacheName cache name
   * @return compression level
   */
  public int getCacheCompressionLevel(String cacheName) {
    if (isCacheCompressionEnabled(cacheName)) {
      String value = props.getProperty(cacheName + "." + CACHE_COMPRESSION_LEVEL_KEY);
      if (value != null) {
        return Integer.parseInt(value.replaceAll(CHARS_TO_REMOVE_REGEX, ""));
      }
      return (int) getLongProperty(CACHE_COMPRESSION_LEVEL_KEY, DEFAULT_CACHE_COMPRESSION_LEVEL);
    } else {
      return -1;
    }
  }

  /**
   * Sets cache compression level
   * @param cacheName cache name
   * @param level compression level
   */
  public void setCacheCompressionLevel(String cacheName, int level) {
    props.setProperty(cacheName + "." + CACHE_COMPRESSION_LEVEL_KEY, Integer.toString(level));
  }

  /**
   * Get compression codec type
   * @param cacheName cache name
   * @return codec type name
   */
  public String getCacheCompressionCodecType(String cacheName) {
    if (isCacheCompressionEnabled(cacheName)) {
      String value = props.getProperty(cacheName + "." + CACHE_COMPRESSION_CODEC_KEY);
      if (value != null) {
        return value;
      }
      return getProperty(CACHE_COMPRESSION_CODEC_KEY, DEFAULT_CACHE_COMPRESSION_CODEC);
    } else {
      return null;
    }
  }

  /**
   * Sets cache compression codec type
   * @param cacheName cache name
   * @param type codec type
   */
  public void setCacheCompressionCodecType(String cacheName, String type) {
    props.setProperty(cacheName + "." + CACHE_COMPRESSION_CODEC_KEY, type);
  }

  /**
   * Get cache compression dictionary enabled
   * @param cacheName cache name
   * @return true if supported, false - otherwise
   */
  public boolean isCacheCompressionDictionaryEnabled(String cacheName) {
    if (isCacheCompressionEnabled(cacheName)) {
      String value = props.getProperty(cacheName + "." + CACHE_COMPRESSION_DICTIONARY_ENABLED_KEY);
      if (value == null) {
        return getBooleanProperty(CACHE_COMPRESSION_DICTIONARY_ENABLED_KEY,
          DEFAULT_CACHE_COMPRESSION_DICTIONARY_ENABLED);
      } else {
        return Boolean.parseBoolean(value);
      }
    } else {
      return false;
    }
  }

  /**
   * Set cache compression dictionary enabled
   * @param cacheName cache name
   * @param v true or false
   */
  public void setCacheCompressionDictionaryEnabled(String cacheName, boolean v) {
    props.setProperty(cacheName + "." + CACHE_COMPRESSION_DICTIONARY_ENABLED_KEY,
      Boolean.toString(v));
  }

  /**
   * Get cache compression keys enabled
   * @param cacheName cache name
   * @return true if supported, false - otherwise
   */
  public boolean isCacheCompressionKeysEnabled(String cacheName) {
    if (isCacheCompressionEnabled(cacheName)) {
      String value = props.getProperty(cacheName + "." + CACHE_COMPRESSION_KEYS_ENABLED_KEY);
      if (value == null) {
        return getBooleanProperty(CACHE_COMPRESSION_KEYS_ENABLED_KEY,
          DEFAULT_CACHE_COMPRESSION_KEYS_ENABLED);
      } else {
        return Boolean.parseBoolean(value);
      }
    } else {
      return false;
    }
  }

  /**
   * Set cache compression keys enabled
   * @param cacheName cache name
   * @param v true or false
   */
  public void setCacheCompressionKeysEnabled(String cacheName, boolean v) {
    props.setProperty(cacheName + "." + CACHE_COMPRESSION_KEYS_ENABLED_KEY, Boolean.toString(v));
  }

  /**
   * Get cache compression training async mode
   * @param cacheName cache name
   * @return true if supported, false - otherwise
   */
  public boolean isCacheCompressionDictionaryTrainingAsync(String cacheName) {
    String value =
        props.getProperty(cacheName + "." + CACHE_COMPRESSION_DICTIONARY_TRAINING_ASYNC_KEY);
    if (value == null) {
      return getBooleanProperty(CACHE_COMPRESSION_DICTIONARY_TRAINING_ASYNC_KEY,
        DEFAULT_CACHE_COMPRESSION_DICTIONARY_TRAINING_ASYNC);
    } else {
      return Boolean.parseBoolean(value);
    }
  }

  /**
   * Set cache compression dictionary training async mode
   * @param cacheName cache name
   * @param v true or false
   */
  public void setCacheCompressionDictionaryTrainingAsync(String cacheName, boolean v) {
    props.setProperty(cacheName + "." + CACHE_COMPRESSION_DICTIONARY_TRAINING_ASYNC_KEY,
      Boolean.toString(v));
  }

  /**
   * Get cache save on shutdown
   * @param cacheName cache name
   * @return true if yes, false - otherwise
   */
  public boolean isSaveOnShutdown(String cacheName) {
    String value = props.getProperty(cacheName + "." + CACHE_SAVE_ON_SHUTDOWN_KEY);
    if (value == null) {
      return getBooleanProperty(CACHE_SAVE_ON_SHUTDOWN_KEY, DEFAULT_CACHE_SAVE_ON_SHUTDOWN);
    } else {
      return Boolean.parseBoolean(value);
    }
  }

  /**
   * Set cache save on shutdown
   * @param cacheName cache name
   * @param v true or false
   */
  public void setSaveOnShutdown(String cacheName, boolean v) {
    props.setProperty(cacheName + "." + CACHE_SAVE_ON_SHUTDOWN_KEY, Boolean.toString(v));
  }

  /**
   * Gets estimated avg key value size
   * @param cacheName cache name
   * @return size
   */
  public int getEstimatedAvgKeyValueSize(String cacheName) {
    String value = props.getProperty(cacheName + "." + CACHE_ESTIMATED_AVG_KV_SIZE_KEY);
    if (value != null) {
      return (int) getLong(value, DEFAULT_ESTIMATED_AVG_KV_SIZE);
    }
    return (int) getLongProperty(CACHE_ESTIMATED_AVG_KV_SIZE_KEY, DEFAULT_ESTIMATED_AVG_KV_SIZE);
  }

  /**
   * Sets estimated avg key value size
   * @param cacheName cache name
   * @param size size
   */
  public void setEstimatedAvgKeyValueSize(String cacheName, int size) {
    props.setProperty(cacheName + "." + CACHE_ESTIMATED_AVG_KV_SIZE_KEY, Integer.toString(size));
  }


  /**
   * Gets asynchronous I/O thread pool size
   * @param cacheName cache name
   * @return pool size
   */
  public int getAsyncIOPoolSize(String cacheName) {
    String value = props.getProperty(cacheName + "." + CACHE_ASYNC_IO_POOL_SIZE_KEY);
    int defaultSize = Runtime.getRuntime().availableProcessors() * 8;
    if (value != null) {
      return (int) getLong(value, defaultSize);
    }
    return (int) getLongProperty(CACHE_ASYNC_IO_POOL_SIZE_KEY, defaultSize);
  }

  /**
   * Sets asynchronous I/O thread pool size
   * @param cacheName cache name
   * @param size size
   */
  public void setAsyncIOPoolSize(String cacheName, int size) {
    props.setProperty(cacheName + "." + CACHE_ASYNC_IO_POOL_SIZE_KEY, Integer.toString(size));
  }
  
  /**
   * Gets cache pro-active expiration factor
   * @param cacheName cache name
   * @return factor
   */
  public double getCacheProactiveExpirationFactor(String cacheName) {
    String value = props.getProperty(cacheName + "." + CACHE_PROACTIVE_EXPIRATION_FACTOR_KEY);
    if (value != null) {
      return Double.parseDouble(value);
    }
    return (double) getDoubleProperty(CACHE_PROACTIVE_EXPIRATION_FACTOR_KEY,
      DEFAULT_CACHE_PROACTIVE_EXPIRATION_FACTOR);
  }

  /**
   * Sets cache pro-active expiration factor
   * @param cacheName cache name
   * @param factor expiration check factor (probability to check on get operation)
   */
  public void setCacheProactiveExpirationFactor(String cacheName, double factor) {
    props.setProperty(cacheName + "." + CACHE_PROACTIVE_EXPIRATION_FACTOR_KEY,
      Double.toString(factor));
  }

  public void sanityCheck(String cacheName) {
    long maxSize = getCacheMaximumSize(cacheName);
    if (maxSize > 0 && maxSize < 200 * 1024 * 1024) {
      // Minimum 200MB
      maxSize = 200 * 1024 * 1024;
    }

    long segmentSize = getCacheSegmentSize(cacheName);
    // Minimum segment size is 1MB
    if (segmentSize < 1024 * 1024) {
      segmentSize = 1024 * 1024;
      setCacheSegmentSize(cacheName, segmentSize);
    }
    int num = (int) (maxSize / segmentSize);
    // Minimum number of segments is 200
    if (num < 200) {
      segmentSize = maxSize / (200 * (1024 * 1024)) * 1024 * 1024;
      setCacheSegmentSize(cacheName, segmentSize);
    }

    int avgKVSize = getEstimatedAvgKeyValueSize(cacheName);
    int maxItems = (int) (maxSize / avgKVSize);
    int l = getStartIndexNumberOfSlotsPower(cacheName);
    if (l == DEFAULT_START_INDEX_NUMBER_OF_SLOTS_POWER) {
      int val = (int) Math.pow(2, l) * 100;
      if (val > maxItems) {
        l = (int) Math.floor(Math.log(maxItems / 100) / Math.log(2));
        setStartIndexNumberOfSlotsPower(cacheName, l);
      }
    }
  }
}
