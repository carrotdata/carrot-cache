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
package com.carrot.cache.jmx;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.carrot.cache.Cache;
import com.carrot.cache.Scavenger;
import com.carrot.cache.io.IOEngine;
import com.carrot.cache.util.Epoch;

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
    return (double) allocd/ max;
  }

  @Override
  public double getused_size_ratio() {
    long max = cache.getMaximumCacheSize();
    long used = cache.getRawDataSize();
    return (double) used/ max;
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
    return victim == null? null: victim.getName();
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
    if(totalBytesWritten == 0 || elapsedTimeSec == 0) {
      return 0.0;
    }
    return (double)totalBytesWritten/ ((1 << 20) * elapsedTimeSec);
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
    if(cacheBytesWritten == 0 || elapsedTimeSec == 0) {
      return 0.0;
    }
    return (double)cacheBytesWritten/ ((1 << 20) * elapsedTimeSec);
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
    if(gcBytesWritten == 0 || elapsedTimeSec == 0) {
      return 0.0;
    }
    return (double)gcBytesWritten/ ((1 << 20) * elapsedTimeSec);
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
    
    return (double) totalRead/ ((1 << 20) * time);
  }

  @Override
  public double getoverall_avg_read_rate() {
    long overallRead = getoverall_bytes_read();
    long time= System.currentTimeMillis() - Epoch.getEpochStartTime();
    time /= 1000;
    if (time == 0) {
      return 0.0;
    }
    return (double) overallRead/ ((1 << 20) * time);
  }

  @Override
  public long getoverall_bytes_read() {
    long self = getcache_bytes_read();
    Cache victim = cache.getVictimCache();
    if(victim != null) {
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

}
