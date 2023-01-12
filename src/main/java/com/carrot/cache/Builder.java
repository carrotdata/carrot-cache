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
package com.carrot.cache;

import java.io.IOException;

import com.carrot.cache.io.FileIOEngine;
import com.carrot.cache.io.IOEngine;
import com.carrot.cache.io.OffheapIOEngine;
import com.carrot.cache.util.CarrotConfig;

public class Builder {
  
  /** Cache name */
  String cacheName;
  
  /* Cache configuration */
  CarrotConfig conf; 
  
  /** Cache IOEngine */
  IOEngine engine;
  
  /**
   * Public constructor
   * @param cacheName
   */
  public Builder(String cacheName) {
    this.cacheName = cacheName;
    this.conf = CarrotConfig.getInstance();
  }
  
  /**
   * Build memory cache
   * @return memory cache
   * @throws IOException
   */
  public Cache buildMemoryCache() throws IOException {
    this.engine = new OffheapIOEngine(this.cacheName);
    return build();
  }
  
  /**
   * Build disk cache
   * @return builder instance
   * @throws IOException
   */
  public Cache buildDiskCache() throws IOException {
    this.engine = new FileIOEngine(this.cacheName);
    return build();
  }
  
  /**
   * Build memory cache
   * @return memory cache
   * @throws IOException
   */
  public ObjectCache buildObjectMemoryCache() throws IOException {
    this.engine = new OffheapIOEngine(this.cacheName);
    Cache c = build();
    return new ObjectCache(c);
  }
  
  /**
   * Build object disk cache
   * @return builder instance
   * @throws IOException
   */
  public ObjectCache buildObjectDiskCache() throws IOException {
    this.engine = new FileIOEngine(this.cacheName);
    Cache c = build();
    return new ObjectCache(c);
  }
  
  /**
   *  With cache maximum size
   * @param size maximum size
   * @return builder instance 
   * @throws IOException
   */
  public Builder withCacheMaximumSize(long size) throws IOException {
    conf.setCacheMaximumSize(this.cacheName, size);
    return this;
  }
  
  /**
   * With cache data segment size
   * @param size segment size
   * @return builder instance
   */
  public Builder withCacheDataSegmentSize(long size) {
    conf.setCacheSegmentSize(this.cacheName, size);
    return this;
  }
  
  /**
   * With scavenger start memory ratio
   * @param ratio start memory ratio
   * @return builder instance
   */
  public Builder withScavengerStartMemoryRatio(double ratio) {
    conf.setScavengerStartMemoryRatio(this.cacheName, ratio);
    return this;
  }
  
  /**
   * With scavenger stop memory ratio
   * @param ratio stop memory ratio
   * @return builder instance
   */
  public Builder withScavengerStopMemoryRatio(double ratio) {
    conf.setScavengerStopMemoryRatio(this.cacheName, ratio);
    return this;
  }
  
  /**
   * With scavenger dump entry adjustment step
   * @param step adjustment step
   * @return builder instance
   */
  public Builder withScavengerDumpEntryBelowAdjStep(double step) {
    conf.setScavengerDumpEntryBelowAdjStep(this.cacheName, step);
    return this;
  }
  
  /**
   * With scavenger dump entry below start
   * @param start dump entry below start value
   * @return builder instance
   */
  public Builder withScavengerDumpEntryBelowStart(double start) {
    conf.setScavengerDumpEntryBelowStart(this.cacheName, start);
    return this;
  }
  
  /**
   * With scavenger dump entry below stop
   * @param stop dump entry below stop value
   * @return builder instance
   */
  public Builder withScavengerDumpEntryBelowStop(double stop) {
    conf.setScavengerDumpEntryBelowStop(this.cacheName, stop);
    return this;
  }
  
  /**
   * With random admission controller start ratio
   * @param ratio start ratio
   * @return builder instance
   */
  public Builder withRandomAdmissionControllerStartRatio(double ratio) {
    conf.setRandomAdmissionControllerStartRatio(this.cacheName, ratio);
    return this;
  }
  
  /**
   * With random admission controller stop ratio
   * @param ratio stop ratio
   * @return builder instance
   */
  public Builder withRandomAdmissionControllerStopRatio(double ratio) {
    conf.setRandomAdmissionControllerStopRatio(cacheName, ratio);
    return this;
  }
  
  /**
   * With number of popularity ranks
   * @param n number of ranks (bins)
   * @return builder instance
   */
  public Builder withNumberOfPopularityRanks(int n) {
    conf.setNumberOfPopularityRanks(this.cacheName, n);
    return this;
  }
  
  /**
   * With SLRU insert point
   * @param point insert point
   * @return builder instance
   */
  public Builder withSLRUInsertionPoint(int point) {
    conf.setSLRUInsertionPoint(this.cacheName, point);
    return this;
  }
  
  /**
   * With SLRU number of segments
   * @param n number of segments
   * @return builder instance
   */
  public Builder withSLRUNumberOfSegments(int n) {
    conf.setSLRUNumberOfSegments(this.cacheName, n);
    return this;
  }
  
  /**
   * With sparse file support
   * @param v true or false
   * @return builder instance
   */
  public Builder withSparseFilesSupport(boolean v) {
    conf.setSparseFilesSupport(this.cacheName, v);
    return this;
  }
  
  /**
   * With start index number of slots power
   * @param n number of slot power
   * @return builder instance
   */
  public Builder withStartIndexNumberOfSlotsPower(int n) {
    conf.setStartIndexNumberOfSlotsPower(this.cacheName, n);
    return this;
  }
  
  /**
   * With global  cache root directory
   * @param dir directory
   * @return builder instance
   */
  public Builder withGlobalCacheRootDir(String dir) {
    conf.setGlobalCacheRootDir(dir);
    return this;
  }
  
  /**
   * With cache root directory
   * @param dir directory
   * @return builder instance
   */
  public Builder withCacheRootDir(String dir) {
    conf.setCacheRootDir(this.cacheName, dir);
    return this;
  }
  
  /**
   * With admission queue start size ratio
   * @param ratio start size ratio
   * @return builder instance
   */
  public Builder withAdmissionQueueStartSizeRatio(double ratio) {
    conf.setAdmissionQueueStartSizeRatio(this.cacheName, ratio);
    return this;
  }
  
  /**
   * With admission queue minimum size ratio
   * @param ratio minimum size ratio
   * @return builder instance
   */
  public Builder withAdmissionQueueMinSizeRatio(double ratio) {
    conf.setAdmissionQueueMinSizeRatio(cacheName, ratio);
    return this;
  }
  
  /**
   * With admission queue maximum size ratio
   * @param ratio maximum size ratio
   * @return builder instance
   */
  public Builder withAdmissionQueueMaxSizeRatio(double ratio) {
    conf.setAdmissionQueueMaxSizeRatio(this.cacheName, ratio);
    return this;
  }
  
  /**
   * With throughput controller check interval
   * @param interval
   * @return builder instance
   */
  public Builder withThroughputCheckInterval(int interval) {
    conf.setThroughputCheckInterval(this.cacheName, interval);
    return this;
  }
  
  /**
   * With Scavenger run interval 
   * @param interval run interval
   * @return builder instance
   */
  public Builder withScavengerRunInterval(int interval) {
    conf.setScavengerRunInterval(this.cacheName, interval);
    return this;
  }
  
  /**
   * With throughput tolerance limit
   * @param limit tolerance limit
   * @return builder instance
   */
  public Builder withThroughputToleranceLimit(double limit) {
    conf.setThroughputToleranceLimit(this.cacheName, limit);
    return this;
  }
  
  /**
   * With cache write maximum wait time
   * @param max maximum wait time
   * @return builder instance
   */
  public Builder withCacheWritesSuspendedThreshold(long max) {
    conf.setCacheWritesMaxWaitTime(this.cacheName, max);
    return this;
  }
  
  /**
   * With cache write limit
   * @param limit write limit
   * @return builder instance
   */
  public Builder withCacheWriteLimit(long limit) {
    conf.setCacheWriteLimit(this.cacheName, limit);
    return this;
  }
  
  /**
   * With throughput controller number of adjustment steps
   * @param n number of steps
   * @return builder instance
   */
  public Builder withThrougputControllerNumberOfAdjustmentSteps(int n) {
    conf.setThrougputControllerNumberOfAdjustmentSteps(this.cacheName, n);
    return this;
  }
  
  /**
   * With index data embedding supported
   * @param v true / false
   * @return builder instance
   */
  public Builder withIndexDataEmbeddedSupported(boolean v) {
    conf.setIndexDataEmbeddedSupported(this.cacheName, v);
    return this;
  }
  
  /**
   * With index data embedded max size
   * @param maxSize max size
   * @return builder instance
   */
  public Builder withIndexDataEmbeddedMaxSize(int maxSize) {
    conf.setIndexDataEmbeddedSize(this.cacheName, maxSize);
    return this;
  }
  
  /**
   * With admission queue index format class name
   * @param className class name
   * @return builder instance
   */
  public Builder withAdmissionQueueIndexFormat(String className) {
    conf.setAdmissionQueueIndexFormat(this.cacheName, className);
    return this;
  }
  
  /**
   * With main queue index format class name
   * @param className class name
   * @return builder instance
   */
  public Builder withMainQueueIndexFormat(String className) {
    conf.setMainQueueIndexFormat(this.cacheName, className);
    return this;
  }
  
  /**
   * With cache eviction policy class name
   * @param className class name
   * @return builder instance
   */
  public Builder withCacheEvictionPolicy(String className) {
    conf.setCacheEvictionPolicy(this.cacheName, className);
    return this;
  }
  
  /**
   * With admission controller
   * @param className class name
   * @return builder instance
   */
  public Builder withAdmissionController(String className) {
    conf.setAdmissionController(this.cacheName, className);
    return this;
  }
  
  /**
   * With throughput controller
   * @param className class name
   * @return builder instance
   */
  public Builder withThroughputController(String className) {
    conf.setThroughputController(this.cacheName, className);
    return this;
  }
  
  /**
   * With recycling selector
   * @param className class name
   * @return builder instance
   */
  public Builder withRecyclingSelector(String className) {
    conf.setRecyclingSelector(this.cacheName, className);
    return this;
  }
  
  /**
   * With data writer
   * @param className class name
   * @return builder instance
   */
  public Builder withDataWriter(String className) {
    conf.setDataWriter(this.cacheName, className);
    return this;
  }
  
  /**
   * With memory data reader
   * @param className class name
   * @return builder instance
   */
  public Builder withMemoryDataReader(String className) {
    conf.setMemoryDataReader(this.cacheName, className);
    return this;
  }
  
  /**
   * With file data reader
   * @param className class name
   * @return builder instance
   */
  public Builder withFileDataReader(String className) {
    conf.setFileDataReader(this.cacheName, className);
    return this;
  }
  
  /**
   * With block writer block size
   * @param size block size
   * @return builder instance
   */
  public Builder withBlockWriterBlockSize(int size) {
    conf.setBlockWriterBlockSize(cacheName, size);
    return this;
  }
  
  /**
   * With file prefetch buffer size
   * @param size buffer size
   * @return builder instance
   */
  public Builder withFilePrefetchBufferSize(int size) {
    conf.setFilePrefetchBufferSize(cacheName, size);
    return this;
  }
  
  /**
   * With expire support class name
   * @param className class name
   * @return builder instance
   */
  public Builder withExpireSupport(String className) {
    conf.setExpireSupport(this.cacheName, className);
    return this;
  }
  
  /**
   * With expire support start bin value (in seconds)
   * @param value bin value in seconds
   * @return builder instance
   */
  public Builder withExpireStartBinValue(int value) {
    conf.setExpireStartBinValue(this.cacheName, value);
    return this;
  }
  
  /**
   * With expire support bin multiplier
   * @param multiplier bin multiplier
   * @return builder instance
   */
  public Builder withExpireBinMultiplier(double multiplier) {
    conf.setExpireBinMultiplier(this.cacheName, multiplier);
    return this;
  }
  
  /**
   * With minimum active data set ratio
   * @param ratio minimum active data set ratio 
   * @return builder instance
   */
  public Builder withMinimumActiveDatasetRatio(double ratio) {
    conf.setMinimumActiveDatasetRatio(cacheName, ratio);
    return this;
  }
  
  /**
   * With eviction disabled mode
   * @param mode 
   * @return builder instance
   */
  public Builder withEvictionDisabledMode(boolean mode) {
    conf.setEvictionDisabledMode(cacheName, mode);
    return this;
  }
  
  /**
   * With I/O storage pool size
   * @param size pool size
   * @return builder instance
   */
  public Builder withIOStoragePoolSize(int size) {
    conf.setIOStoragePoolSize(cacheName, size);
    return this;
  }
  
  /**
   * With victim cache promote on hit
   * @param v true or false
   * @return builder instance
   */
  public Builder withVictimCachePromoteOnHit(boolean v) {
    conf.setVictimCachePromotionOnHit(cacheName, v);
    return this;
  }
  
  /**
   * With scavenger maximum number of segments before stall mode is activated
   * @param n max segments
   * @return builder instance
   */
  public Builder withScavengerMaxSegmentsBeforeStall(int n) {
    conf.setScavengerMaxSegmentsBeforeStall(cacheName, n);
    return this;
  }
  
  /**
   * With victim cache promotion threshold
   * @param v threshold
   * @return builder instance
   */
  public Builder withVictimCachePromotionThreshold(double v) {
    conf.setVictimPromotionThreshold(cacheName, v);
    return this;
  }
  
  /**
   * With hybrid cache inverse mode
   * @param b mode
   * @return builder instance
   */
  public Builder withCacheHybridInverseMode(boolean b) {
    conf.setCacheHybridInverseMode(cacheName, b);
    return this;
  }
  
  /**
   * With cache spin wait time on high pressure
   * @param time wait time (in nanoseconds)
   * @return builder instance
   */
  public Builder withCacheSpinWaitTimeOnHighPressure(long time) {
    conf.setCacheSpinWaitTimeOnHighPressure(cacheName, time);
    return this;
  }
  
  /**
   * With JMX metrics domain name
   * @param name domain name
   * @return builder instance
   */
  public Builder withJMXMetricsDomainName(String name) {
    conf.setJMXMetricsDomainName(name);
    return this;
  }
  
  /**
   * With streaming buffer size
   * @param size buffer size
   * @return builder instance
   */
  public Builder withCacheStreamingSupportBufferSize(int size) {
    conf.setCacheStreamingSupportBufferSize(cacheName, size);
    return this;
  }
  
  /**
   * With Scavenger number of threads
   * @param threads number of threads
   * @return builder instances
   */
  public Builder withScavengerNumberOfThreads(int threads) {
    conf.setScavengerNumberOfThreads(cacheName, threads);
    return this;
  }
  
  /**
   * With maximum wait time on PUT
   * @param time time
   * @return builder instance
   */
  public Builder withCacheMaximumWaitTimeOnPut(long time) {
    conf.setCacheMaximumWaitTimeOnPut(cacheName, time);
    return this;
  }
  
  /**
   * Build cache
   * @return
   * @throws IOException
   */
  private Cache build() throws IOException {
    Cache cache = new Cache(conf, cacheName);
    cache.setIOEngine(this.engine);
    cache.initAll();
    return cache;
  }
  
  
}