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
package com.carrotdata.cache.jmx;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.carrotdata.cache.Cache;
import com.carrotdata.cache.Scavenger;
import com.carrotdata.cache.io.IOEngine;
import com.carrotdata.cache.util.CacheConfig;
import com.carrotdata.cache.util.Epoch;

public class CacheJMXSink implements CacheJMXSinkMBean {

  private Cache cache;
  private Scavenger.Stats gcStats;

  public CacheJMXSink(Cache cache) {
    this.cache = cache;
    this.gcStats = Scavenger.getStatisticsForCache(cache.getName());
  }

  @Override
  public String getepoch_start_time() {
    long startTime = Epoch.getEpochStartTime();
    String pattern = "MM/dd/yyyy 'at' hh:mm:ss z";
    DateFormat df = new SimpleDateFormat(pattern);
    Date date = new Date(startTime);
    return df.format(date);
  }

  @Override
  public String gettype() {
    return cache.getCacheType().toString();
  }

  @Override
  public long getmax_size_bytes() {
    return cache.getMaximumCacheSize();
  }

  @Override
  public long getallocated_size_bytes() {
    return cache.getStorageAllocated();
  }

  @Override
  public long getused_size_bytes() {
    return cache.getRawDataSize();
  }

  @Override
  public double getallocated_size_ratio() {
    long max = cache.getMaximumCacheSize();
    long allocd = cache.getStorageAllocated();
    return (double) allocd / max;
  }

  @Override
  public double getused_size_ratio() {
    long max = cache.getMaximumCacheSize();
    long used = cache.getRawDataSize();
    return (double) used / max;
  }

  @Override
  public long gettotal_gets() {
    return cache.getTotalGets();
  }

  @Override
  public long gettotal_hits() {
    return cache.getTotalHits();
  }

  @Override
  public boolean getcache_hybrid() {
    return cache.getVictimCache() != null;
  }

  @Override
  public String getvictim_cache_name() {
    Cache victim = cache.getVictimCache();
    return victim == null ? null : victim.getName();
  }

  @Override
  public double getcache_hit_ratio() {
    return cache.getHitRate();
  }

  @Override
  public double getoverall_hit_ratio() {
    return cache.getOverallHitRate();
  }

  @Override
  public long getactive_dataset_size() {
    IOEngine engine = cache.getEngine();
    return engine.activeDataSize();
  }

  @Override
  public double getactive_dataset_size_ratio() {
    IOEngine engine = cache.getEngine();
    return engine.activeSizeRatio();
  }

  @Override
  public long gettotal_bytes_written() {
    long gcWritten = gcStats.getTotalBytesScanned() - gcStats.getTotalBytesFreed();
    long cacheWritten = cache.getTotalWritesSize();
    return gcWritten + cacheWritten;
  }

  @Override
  public double gettotal_avg_write_rate() {
    long currentTime = System.currentTimeMillis();
    long elapsedTimeSec = (currentTime - Epoch.getEpochStartTime()) / 1000;
    long totalBytesWritten = gettotal_bytes_written();
    if (totalBytesWritten == 0 || elapsedTimeSec == 0) {
      return 0.0;
    }
    return (double) totalBytesWritten / ((1 << 20) * elapsedTimeSec);
  }

  @Override
  public long getcache_bytes_written() {
    return cache.getTotalWritesSize();
  }

  @Override
  public double getcache_avg_write_rate() {
    long currentTime = System.currentTimeMillis();
    long elapsedTimeSec = (currentTime - Epoch.getEpochStartTime()) / 1000;
    long cacheBytesWritten = getcache_bytes_written();
    if (cacheBytesWritten == 0 || elapsedTimeSec == 0) {
      return 0.0;
    }
    return (double) cacheBytesWritten / ((1 << 20) * elapsedTimeSec);
  }

  @Override
  public long getgc_bytes_written() {
    return gcStats.getTotalBytesScanned() - gcStats.getTotalBytesFreed();
  }

  @Override
  public double getgc_avg_write_rate() {
    long currentTime = System.currentTimeMillis();
    long elapsedTimeSec = (currentTime - Epoch.getEpochStartTime()) / 1000;
    long gcBytesWritten = getgc_bytes_written();
    if (gcBytesWritten == 0 || elapsedTimeSec == 0) {
      return 0.0;
    }
    return (double) gcBytesWritten / ((1 << 20) * elapsedTimeSec);
  }

  @Override
  public long getgc_number_of_runs() {
    return gcStats.getTotalRuns();
  }

  @Override
  public long getgc_bytes_scanned() {
    return gcStats.getTotalBytesScanned();
  }

  @Override
  public long getgc_bytes_freed() {
    return gcStats.getTotalBytesFreed();
  }

  @Override
  public long gettotal_puts() {
    long totalWrites = cache.getTotalWrites();
    Cache victim = cache.getVictimCache();
    if (victim != null) {
      totalWrites += victim.getTotalWrites();
    }
    return totalWrites;
  }

  @Override
  public long gettotal_inserts() {
    long totalInserts = cache.getEngine().getTotalInserts();
    Cache victim = cache.getVictimCache();
    if (victim != null) {
      totalInserts += victim.getEngine().getTotalInserts();
    }
    return totalInserts;
  }

  @Override
  public long gettotal_updates() {
    long totalUpdates = cache.getEngine().getTotalUpdates();
    Cache victim = cache.getVictimCache();
    if (victim != null) {
      totalUpdates += victim.getEngine().getTotalUpdates();
    }
    return totalUpdates;
  }

  @Override
  public long gettotal_deletes() {
    long totalDeletes = cache.getEngine().getTotalDeletes();
    Cache victim = cache.getVictimCache();
    if (victim != null) {
      totalDeletes += victim.getEngine().getTotalDeletes();
    }
    return totalDeletes;
  }

  @Override
  public long getcache_gets() {
    return cache.getTotalGets();
  }

  @Override
  public long getcache_writes() {
    return cache.getTotalWrites();
  }

  @Override
  public long getcache_bytes_read() {
    return cache.getTotalGetsSize();
  }

  @Override
  public double getcache_avg_read_rate() {
    long totalRead = cache.getTotalGetsSize();
    long time = System.currentTimeMillis() - Epoch.getEpochStartTime();
    time /= 1000;
    if (time == 0) return 0.0;

    return (double) totalRead / ((1 << 20) * time);
  }

  @Override
  public double getoverall_avg_read_rate() {
    long overallRead = getoverall_bytes_read();
    long time = System.currentTimeMillis() - Epoch.getEpochStartTime();
    time /= 1000;
    if (time == 0) {
      return 0.0;
    }
    return (double) overallRead / ((1 << 20) * time);
  }

  @Override
  public long getoverall_bytes_read() {
    long self = getcache_bytes_read();
    Cache victim = cache.getVictimCache();
    if (victim != null) {
      self += cache.getTotalGetsSize();
    }
    return self;
  }

  @Override
  public long getio_avg_read_duration() {
    // in microseconds
    long duration = cache.getEngine().getTotalIOReadDuration() / 1000;
    return duration / gettotal_gets();
  }

  @Override
  public long getio_avg_read_size() {
    return getoverall_bytes_read() / gettotal_gets();
  }

  @Override
  public boolean getcompression_enabled() {
    String cacheName = cache.getName();
    CacheConfig config = CacheConfig.getInstance();
    return config.isCacheCompressionEnabled(cacheName);
  }

  @Override
  public String getcompression_codec() {
    String cacheName = cache.getName();
    CacheConfig config = CacheConfig.getInstance();
    String codec = config.getCacheCompressionCodecType(cacheName);
    return codec != null ? codec : "none";
  }

  @Override
  public int getcompression_level() {
    String cacheName = cache.getName();
    CacheConfig config = CacheConfig.getInstance();
    return config.getCacheCompressionLevel(cacheName);
  }

  @Override
  public boolean getcompression_dictionary_enabled() {
    String cacheName = cache.getName();
    CacheConfig config = CacheConfig.getInstance();
    return config.isCacheCompressionDictionaryEnabled(cacheName);
  }

  @Override
  public int getcompression_dictionary_size() {
    String cacheName = cache.getName();
    CacheConfig config = CacheConfig.getInstance();
    return config.getCacheCompressionDictionarySize(cacheName);
  }

  @Override
  public long getcompressed_size_bytes() {
    return cache.getStorageUsedActual();
  }

  /**
   * Compression block size
   * @return compression block size
   */
  @Override
  public int getcompression_block_size() {
    String cacheName = cache.getName();
    CacheConfig config = CacheConfig.getInstance();
    return config.getCacheCompressionBlockSize(cacheName);
  }

  /**
   * Get compression keys enabled
   * @return true or false
   */
  @Override
  public boolean getcompression_keys_enabled() {
    String cacheName = cache.getName();
    CacheConfig config = CacheConfig.getInstance();
    return config.isCacheCompressionKeysEnabled(cacheName);
  }

  @Override
  public double getcompression_ratio() {
    long allocd = getallocated_size_bytes();
    long compressed = getcompressed_size_bytes();
    return (double) allocd / compressed;
  }
}
