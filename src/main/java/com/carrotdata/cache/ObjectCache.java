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
package com.carrotdata.cache;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.kryo5.Kryo;
import com.esotericsoftware.kryo.kryo5.io.Input;
import com.esotericsoftware.kryo.kryo5.io.Output;
import com.carrotdata.cache.Cache.Type;
import com.carrotdata.cache.util.CacheConfig;
import com.carrotdata.cache.util.ObjectPool;

public class ObjectCache {

  public static interface SerdeInitializationListener {

    public void initSerde(Kryo kryo);
  }

  /** Logger */
  @SuppressWarnings("unused")
  private static final Logger LOG = LoggerFactory.getLogger(ObjectCache.class);

  /** Initial buffer size in bytes for Kryo serialization */
  private final int INITIAL_BUFFER_SIZE = 4096;

  /** Native cache instance (off-heap, disk or hybrid) */
  private Cache cache;

  /** Kryo object serialization pool */
  private ObjectPool<Kryo> kryos;

  /** Key - Value class map */
  private Map<Class<?>, Class<?>> keyValueClassMap = new ConcurrentHashMap<Class<?>, Class<?>>();

  /** Kryo initialization listeners */
  private List<SerdeInitializationListener> listeners =
      Collections.synchronizedList(new ArrayList<>());

  /** Object pool for Kryo inputs */
  static ObjectPool<Input> inputs;

  /** Object pool for Kryo outputs */
  static ObjectPool<Output> outputs;

  /** To support loading cache **/
  private Map<Object, Object> waitingKeys = new ConcurrentHashMap<>();

  /* Each key class can have associated expiration time in milliseconds */
  private Map<Class<?>, Long> keyExpireMap = new ConcurrentHashMap<Class<?>, Long>();

  /* Initial output buffer size */
  private int initialOutBufferSize;

  /* Maximum output buffer size */
  private int maxOutBufferSize;

  /**
   * Default constructor
   * @param c native cache instance
   */
  ObjectCache(Cache c) {
    Objects.requireNonNull(c, "Cache is null");
    this.cache = c;
    initIOPools();
  }

  /**
   * Adds Kryo's serialization listener.
   * @param l listener
   */
  public void addSerdeInitializationListener(SerdeInitializationListener l) {
    this.listeners.add(l);
  }

  /**
   * Adds key-value classes pair
   * @param key key's class
   * @param value value's class
   */
  public void addKeyValueClasses(Class<?> key, Class<?> value) {
    this.keyValueClassMap.put(key, value);
  }

  /**
   * Sets default expiration time for key's class
   * @param keyClass key's class
   * @param expire expiration time in milliseconds (relative)
   */
  public void setKeyClassExpire(Class<?> keyClass, long expire) {
    keyExpireMap.put(keyClass, expire);
  }

  private void initIOPools() {
    CacheConfig config = cache.getCacheConfig();
    int poolSize = config.getIOStoragePoolSize(cache.getName());
    this.initialOutBufferSize = config.getObjectCacheInitialOutputBufferSize(cache.getName());
    this.maxOutBufferSize = config.getObjectCacheMaxOutputBufferSize(cache.getName());
    if (inputs == null) {
      synchronized (ObjectPool.class) {
        if (inputs == null) {
          inputs = new ObjectPool<Input>(poolSize);
        }
      }
    }
    if (outputs == null) {
      synchronized (ObjectPool.class) {
        if (outputs == null) {
          outputs = new ObjectPool<Output>(poolSize);
        }
      }
    }
    if (kryos == null) {
      synchronized (ObjectPool.class) {
        if (kryos == null) {
          kryos = new ObjectPool<Kryo>(poolSize);
        }
      }
    }
  }

  /**
   * Put key - value pair into the cache with expiration (absolute time in ms) This is operation to
   * bypass admission controller
   * @param key key object
   * @param value value object
   * @param expire expiration time (absolute in ms since 01/01/1970)
   * @return true on success, false - otherwise
   * @throws IOException
   */
  public boolean put(Object key, Object value, long expire) throws IOException {
    return put(key, value, expire, true);
  }

  /**
   * Put key - value pair into the cache with expiration (absolute time in ms)
   * @param key key object
   * @param value value object
   * @param expire expiration time (absolute)
   * @param force if true - bypass admission controller
   * @return true on success, false - otherwise
   * @throws IOException
   */
  public boolean put(Object key, Object value, long expire, boolean force) throws IOException {
    Objects.requireNonNull(key, "key is null");
    Objects.requireNonNull(value, "value is null");
    Output outKey = getOutput();
    Output outValue = getOutput();
    Kryo kryo = getKryo();
    try {
      kryo.writeObject(outKey, key);
      kryo.writeObject(outValue, value);
      // TODO: cryptographic hashing of a key
      byte[] keyBuffer = outKey.getBuffer();
      int keyLength = outKey.position();
      byte[] valueBuffer = outValue.getBuffer();
      int valueLength = outValue.position();
      boolean result =
          cache.put(keyBuffer, 0, keyLength, valueBuffer, 0, valueLength, expire, force);
      return result;
    } finally {
      release(outKey);
      release(outValue);
      release(kryo);
    }
  }

  /**
   * Get cached value
   * @param key object key
   * @return value or null
   * @throws IOException
   */
  public Object get(Object key) throws IOException {
    Objects.requireNonNull(key, "key is null");
    Output outKey = getOutput();
    Input in = getInput();
    Kryo kryo = getKryo();
    Class<?> valueClass = this.keyValueClassMap.get(key.getClass());

    if (valueClass == null) {
      throw new IOException(
          String.format("Value class is not registered for the key class %s", key.getClass()));
    }

    try {
      kryo.writeObject(outKey, key);
      byte[] keyBuffer = outKey.getBuffer();
      int keyLength = outKey.position();
      byte[] buf = in.getBuffer();
      while (true) {
        long size = cache.get(keyBuffer, 0, keyLength, buf, 0);
        if (size < 0) {
          return null;
        }
        if (size > buf.length) {
          buf = new byte[(int) size];
          in.setBuffer(buf);
          continue;
        } else {
          in.setBuffer(buf, 0, (int) size);
          break;
        }
      }
      Object value = kryo.readObject(in, valueClass);
      return value;
    } finally {
      release(outKey);
      release(in);
      release(kryo);
    }
  }

  /**
   * Get cache value with value loader
   * @param key key
   * @param valueLoader value loader
   * @return value
   * @throws IOException
   */
  public Object get(Object key, Callable<?> valueLoader) throws IOException {
    Object value = get(key);
    if (value != null) {
      return value;
    }
    Object v = waitingKeys.get(key);
    if (v != null) {
      return waitAndGet(key);
    } else {
      Object prev = waitingKeys.putIfAbsent(key, Boolean.TRUE);
      if (prev != null) {
        return waitAndGet(key);
      } else {
        try {
          value = valueLoader.call();
          if (value != null) {
            long expire = getExpireForKey(key);
            // This time is relative
            if (expire > 0) {
              put(key, value, System.currentTimeMillis() + expire);
            } else {
              put(key, value, 0L);
            }
          }
        } catch (Exception e) {
          throw new IOException(e);
        } finally {
          waitingKeys.remove(key);
        }
      }
    }
    return value;
  }

  private long getExpireForKey(Object key) {
    Long expire = keyExpireMap.get(key.getClass());
    if (expire == null) {
      return 0;
    }
    return expire;
  }

  private Object waitAndGet(Object key) throws IOException {
    // Loading in progress
    while (waitingKeys.get(key) != null) {
      try {
        Thread.sleep(1);
      } catch (InterruptedException e) {
      }
    }
    // Repeat call
    Object value = get(key);
    if (value == null) {
      // Loader failed
      throw new IOException(String.format("Failed to load value for key %s", key.toString()));
    }
    return value;
  }

  /**
   * Delete object by key
   * @param key object key
   * @return true on success, false - otherwise
   * @throws IOException
   */
  public boolean delete(Object key) throws IOException {
    Objects.requireNonNull(key, "key is null");
    Output outKey = getOutput();
    Kryo kryo = getKryo();

    try {
      kryo.writeObject(outKey, key);
      // TODO: cryptographic hashing of a key
      byte[] keyBuffer = outKey.getBuffer();
      int keyLength = outKey.position();
      boolean result = cache.delete(keyBuffer, 0, keyLength);
      return result;
    } finally {
      release(outKey);
      release(kryo);
      // Remove key from waiting set (if any, just in case)
      waitingKeys.remove(key);
    }
  }

  /**
   * Does key exist
   * @param key object key
   * @return true on success, false - otherwise
   * @throws IOException
   */
  public boolean exists(Object key) throws IOException {
    Objects.requireNonNull(key, "key is null");
    Output outKey = getOutput();
    Kryo kryo = getKryo();
    try {
      kryo.writeObject(outKey, key);
      // TODO: cryptographic hashing of a key
      byte[] keyBuffer = outKey.getBuffer();
      int keyLength = outKey.position();
      boolean result = cache.exists(keyBuffer, 0, keyLength);
      return result;
    } finally {
      release(outKey);
      release(kryo);
    }
  }

  /**
   * Touch the key
   * @param key object key
   * @return true on success, false - otherwise (key does not exists)
   * @throws IOException
   */
  public boolean touch(Object key) throws IOException {
    Objects.requireNonNull(key, "key is null");
    Output outKey = getOutput();
    Kryo kryo = getKryo();
    try {
      kryo.writeObject(outKey, key);
      // TODO: cryptographic hashing of a key
      byte[] keyBuffer = outKey.getBuffer();
      int keyLength = outKey.position();
      boolean result = cache.touch(keyBuffer, 0, keyLength);
      return result;
    } finally {
      release(outKey);
      release(kryo);
    }
  }

  /**
   * Shutdown and save cache
   * @throws IOException
   */
  public void shutdown() throws IOException {
    // Shutdown main cache
    this.cache.shutdown();
  }

  /**
   * Loads saved object cache. Make sure that CarrotConfig was already set for the cache
   * @param cacheRootDir cache root directory
   * @param cacheName cache name
   * @return object cache or null
   * @throws IOException
   */
  public static ObjectCache loadCache(String[] cacheRootDirs, String cacheName) throws IOException {

    Cache c = Cache.loadCache(cacheName);
    if (c != null) {
      return new ObjectCache(c);
    }
    // Else create new
    return null;
  }

  /**
   * Get Kryo instance from pool or new one
   * @return Kryo input
   */
  Input getInput() {
    Input in = inputs.poll();
    if (in == null) {
      in = new Input(INITIAL_BUFFER_SIZE);
    } else {
      // for reuse
      in.reset();
    }
    return in;
  }

  /**
   * Get Kryo output or new one
   * @return Kryo output
   */
  Output getOutput() {
    Output out = outputs.poll();
    if (out == null) {
      out = new Output(initialOutBufferSize, maxOutBufferSize);
    } else {
      out.reset();
    }
    return out;
  }

  /**
   * Get Kryo instance
   * @return instance
   */
  Kryo getKryo() {
    Kryo kryo = kryos.poll();
    if (kryo == null) {
      kryo = new Kryo();
      kryo.setRegistrationRequired(false);
      for (Map.Entry<Class<?>, Class<?>> entry : keyValueClassMap.entrySet()) {
        kryo.register(entry.getKey());
        kryo.register(entry.getValue());
      }
      for (SerdeInitializationListener l : listeners) {
        l.initSerde(kryo);
      }
    } else {
      kryo.reset();
    }
    return kryo;
  }

  /**
   * Release input back to the pool
   * @param in Kryo input
   */
  void release(Input in) {
    inputs.offer(in);
  }

  /**
   * Release output back to the pool
   * @param out Kryo output
   */
  void release(Output out) {
    outputs.offer(out);
  }

  /**
   * Release kryo instance
   * @param kryo
   */
  void release(Kryo kryo) {
    kryos.offer(kryo);
  }

  /**
   * Adds shutdown hook
   */
  public void addShutdownHook() {
    this.cache.addShutdownHook();
  }

  /**
   * Removes shutdown hook
   */
  public void removeShutdownHook() {
    this.cache.removeShutdownHook();
  }

  /**
   * Register JMX metrics
   */
  public void registerJMXMetricsSink() {
    this.cache.registerJMXMetricsSink();
  }

  /**
   * Register JMX metrics with a custom domain name
   * @param domainName
   */
  public void registerJMXMetricsSink(String domainName) {
    this.cache.registerJMXMetricsSink(domainName);
  }

  /**
   * Get number of objects in the cache
   * @return number of objects in the cache
   */
  public long size() {
    return this.cache.size();
  }

  /**
   * Total number of active items (accessible)
   * @return active number
   */
  public long activeSize() {
    return this.cache.activeSize();
  }

  /**
   * Gets memory limit
   * @return memory limit in bytes
   */
  public long getMaximumCacheSize() {
    return this.cache.getMaximumCacheSize();
  }

  /**
   * Get memory used as a fraction of memory limit
   * @return memory used fraction
   */
  public double getStorageAllocatedRatio() {
    return this.cache.getStorageAllocatedRatio();
  }

  /**
   * Get total used memory (storage) - before compression
   * @return used memory
   */
  public long getStorageUsed() {
    return this.cache.getRawDataSize();
  }

  /**
   * Get total used memory (storage) - after compression
   * @return used memory
   */
  public long getStorageUsedActual() {
    return this.cache.getStorageUsedActual();
  }

  /**
   * Get total allocated memory
   * @return total allocated memory
   */
  public long getStorageAllocated() {
    return this.cache.getStorageAllocated();
  }

  /**
   * Get total gets
   * @return total gets
   */
  public long getTotalGets() {
    return this.cache.getTotalGets();
  }

  /**
   * Total gets size
   * @return size
   */
  public long getTotalGetsSize() {
    return this.cache.getTotalGetsSize();
  }

  /**
   * Get total hits
   * @return total hits
   */
  public long getTotalHits() {
    return this.cache.getTotalHits();
  }

  /**
   * Get total writes
   * @return total writes
   */
  public long getTotalWrites() {
    return this.cache.getTotalWrites();
  }

  /**
   * Get total writes size
   * @return total writes size
   */
  public long getTotalWritesSize() {
    return this.cache.getTotalWritesSize();
  }

  /**
   * Get total rejected writes
   * @return total rejected writes
   */
  public long getTotalRejectedWrites() {
    return this.cache.getTotalRejectedWrites();
  }

  /**
   * Get cache hit rate
   * @return cache hit rate
   */
  public double getHitRate() {
    return this.cache.getHitRate();
  }

  /**
   * For hybrid caches
   * @return hybrid cache hit rate
   */
  public double getOverallHitRate() {
    return this.cache.getOverallHitRate();
  }

  /**
   * Get native cache
   * @return cache
   */
  public Cache getNativeCache() {
    return this.cache;
  }

  /**
   * Get cache name
   * @return cache name
   */
  public String getName() {
    return this.cache.getName();
  }

  /**
   * Get cache type
   * @return cache type
   */
  public Type getCacheType() {
    return this.cache.getCacheType();
  }

  /**
   * Get cache configuration
   * @return cache configuration
   */
  public CacheConfig getCacheConfig() {
    return this.cache.getCacheConfig();
  }
}
