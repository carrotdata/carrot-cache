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

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

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
    return cache.getTotalAllocated();
  }
  
  @Override
  public double getallocated_size_ratio() {
    long max = cache.getMaximumCacheSize();
    long allocd = cache.getTotalAllocated();
    return (double) allocd / max;
  }
  
  @Override
  public long getused_size_bytes() {
    return cache.getTotalUsed();
  }
  
  @Override
  public double getused_size_ratio() {
    long max = cache.getMaximumCacheSize();
    long used = cache.getTotalUsed();
    return (double) used / max;
  }
  
  /**
   * Index size bytes
   * @return index size
   */
  public long getindex_size_bytes() {
    return this.cache.getEngine().getMemoryIndex().getAllocatedMemory();
  }
  
  @Override
  public long getraw_size_bytes() {
    return cache.getRawDataSize();
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
  public long getitems_total() {
    return cache.size();
  }

  @Override
  public long getitems_active() {
    return cache.activeSize();
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
    //FIXME: Epoch can be loaded from saved cache
    // we need server start time
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
    long gets = gettotal_gets();
    if (gets == 0) return 0;
    // in microseconds
    long duration = cache.getEngine().getTotalIOReadDuration() / 1000;
    return duration / gettotal_gets();
  }

  @Override
  public long getio_avg_read_size() {
    long gets = gettotal_gets();
    if (gets == 0) return 0;
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
    long allocd = getraw_size_bytes();
    long compressed = getallocated_size_bytes();
    return (double) allocd / compressed;
  }
  
  public List<String> asList() {
    List<String> list = new ArrayList<String>();
    String name = cache.getName();
    list.add(name + ":type");
    list.add(gettype());
    list.add(name + ":max_memory");    
    list.add("" + getmax_size_bytes());
    list.add(name + ":allocated_memory");
    list.add("" + getallocated_size_bytes());
    list.add(name + ":used_memory");
    list.add("" + getused_size_bytes());
    list.add(name + ":raw_data_size");
    list.add("" + getraw_size_bytes());
    list.add(name + ":index_size");
    list.add("" + getindex_size_bytes());
    list.add(name + ":allocated_size_ratio");
    list.add("" + getallocated_size_ratio());
    list.add(name + ":used_size_ratio");
    list.add("" + getused_size_ratio());
    list.add(name + ":active_dataset_size");
    list.add("" + getactive_dataset_size());
    list.add(name + ":active_dataset_size_ratio");
    list.add("" + getactive_dataset_size_ratio());
    list.add(name + ":total_items");
    list.add("" + getitems_total());
    
    list.add(name + ":active_items");
    list.add("" + getitems_active());  
    
    list.add(name + ":total_puts");
    list.add("" + gettotal_puts()); 
    
    list.add(name + ":total_inserts");
    list.add("" + gettotal_inserts()); 

    list.add(name + ":total_updates");
    list.add("" + gettotal_updates());
    
    list.add(name + ":total_deletes");
    list.add("" + gettotal_deletes());

    list.add(name + ":total_gets");
    list.add("" + gettotal_gets());
    list.add(name + ":total_hits");
    list.add("" + gettotal_hits());
    
    list.add(name + ":is_hybrid");
    list.add("" + getcache_hybrid());
    
    String vname = getvictim_cache_name();
    if (vname != null) {
      list.add(name + ":victim_cache_name");
      list.add("" + getvictim_cache_name());
    }
    
    list.add(name + ":gets");
    list.add("" + getcache_gets());
    
    list.add(name + ":writes");
    list.add("" + getcache_writes());
    
    list.add(name + ":hit_ratio");
    list.add("" + getcache_hit_ratio());

    list.add(name + ":overall_hit_ratio");
    list.add("" + getoverall_hit_ratio());
    
    list.add(name + ":total_bytes_written");
    list.add("" + gettotal_bytes_written());

    list.add(name + ":avg_write_rate");
    list.add("" + getcache_avg_write_rate());
    
    list.add(name + ":total_avg_write_rate");
    list.add("" + gettotal_avg_write_rate());
    
    list.add(name + ":bytes_written");
    list.add("" + getcache_bytes_written());
        
    list.add(name + ":bytes_read");
    list.add("" + getcache_bytes_read());
    
    list.add(name + ":total_bytes_read");
    list.add("" + getoverall_bytes_read());

    list.add(name + ":avg_read_rate");
    list.add("" + getcache_avg_read_rate());
    
    list.add(name + ":total_avg_read_rate");
    list.add("" + getoverall_avg_read_rate());
    
    list.add(name + ":gc_bytes_written");
    list.add("" + getgc_bytes_written());
    
    list.add(name + ":gc_avg_write_rate");
    list.add("" + getgc_avg_write_rate());
    
    list.add(name + ":gc_number_of_runs");
    list.add("" + getgc_number_of_runs());
        
    list.add(name + ":gc_bytes_scanned");
    list.add("" + getgc_bytes_scanned());
    
    list.add(name + ":gc_bytes_freed");
    list.add("" + getgc_bytes_freed());
    
    list.add(name + ":io_avg_read_duration");
    list.add("" + getio_avg_read_duration());
    
    list.add(name + ":io_avg_read_size");
    list.add("" + getio_avg_read_size());

    /**************************************
     * Compression
     *************************************/
    list.add(name + ":compression_enabled");
    list.add("" + getcompression_enabled());
    
    if (getcompression_enabled()) {
      list.add(name + ":compression_codec");
      list.add("" + getcompression_codec());
            
      list.add(name + ":compression_level");
      list.add("" + getcompression_level());
            
      list.add(name + ":compression_dictionary_enabled");
      list.add("" + getcompression_dictionary_enabled());
      
      list.add(name + ":compression_dictionary_size");
      list.add("" + getcompression_dictionary_size());
            
      list.add(name + ":compressed_size");
      list.add("" + getcompressed_size_bytes());
      
      list.add(name + ":compression_block_size");
      list.add("" + getcompression_block_size());
      
      list.add(name + ":compression_keys_enabled");
      list.add("" + getcompression_keys_enabled());
      
      list.add(name + ":compression_ratio");
      list.add("" + getcompression_ratio());
    }
    return list;
  }
}
