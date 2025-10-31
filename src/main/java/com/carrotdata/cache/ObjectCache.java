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
package com.carrotdata.cache;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotdata.cache.Cache.Type;
import com.carrotdata.cache.util.CacheConfig;
import com.carrotdata.cache.util.ObjectPool;
import com.esotericsoftware.kryo.kryo5.Kryo;
import com.esotericsoftware.kryo.kryo5.io.Input;
import com.esotericsoftware.kryo.kryo5.io.Output;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.Expiry;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.Scheduler;


public class ObjectCache {

  public static interface SerdeInitializationListener {
    public void initSerde(Kryo kryo);
  }

  public static class ValueExpire {
    // Value
    private Object value;
    // Absolute expiration time in ms since 1970, 01/01 00:00
    private long expireAt;
    
    public ValueExpire() {
    }
    
    public ValueExpire(Object value, long expire) {
      this.value = value;
      this.expireAt = expire;
    }
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
  
  private List<Class<?>> classList = Collections.synchronizedList(new ArrayList<>());
  
  /** Kryo initialization listeners */
  private List<SerdeInitializationListener> listeners =
      Collections.synchronizedList(new ArrayList<>());

  /** Object pool for Kryo inputs */
  static ObjectPool<Input> inputs;

  /** Object pool for Kryo outputs */
  static ObjectPool<Output> outputs;

  /** To support loading cache **/
  private Map<Object, Object> waitingKeys = new ConcurrentHashMap<>();

  /* Initial output buffer size */
  private int initialOutBufferSize;

  /* Maximum output buffer size */
  private int maxOutBufferSize;

  /* On heap cache */
  private com.github.benmanes.caffeine.cache.Cache<Object, ValueExpire> heapCache;
  
  /* On heap cache maximum size in number of entries*/
  private long heapCacheMaxSize;
  
  /**
   * Default constructor
   * @param c native cache instance
   */
  ObjectCache(Cache c) {
    Objects.requireNonNull(c, "Cache is null");
    this.cache = c;
    initIOPools();
  }

  public void setHeapCacheMaxSize(long maxSize) {
    if (this.heapCacheMaxSize > 0 || maxSize == 0) {
      return; // already set or 0
    }
    if (maxSize <= 0) {
      throw new IllegalArgumentException("Maximum heap cache size must be positive number");
    }
    
    this.heapCache = Caffeine.newBuilder()
        .scheduler(Scheduler.systemScheduler())
        .expireAfter(new Expiry<Object , ValueExpire>() {
          public long expireAfterCreate(Object key, ValueExpire value, long currentTime) {
            // Its in milliseconds
            long expireAt = value.expireAt;
            return expireAt == 0? TimeUnit.SECONDS.toNanos(Long.MAX_VALUE): (expireAt - System.currentTimeMillis()) * 1_000_000;
          }

          public long expireAfterUpdate(Object key, ValueExpire value,
              long currentTime, long currentDuration) {
              return currentDuration;
          }
  
          public long expireAfterRead(Object key, ValueExpire value,
              long currentTime, long currentDuration) {
              return currentDuration;
          }
      })
      .evictionListener((Object k, ValueExpire v, RemovalCause cause) -> {
  
        if (cause == RemovalCause.SIZE) {
            long expireAt = v.expireAt;
            long current = System.currentTimeMillis();
            if (expireAt == 0 || expireAt > current) {
              try {
                putInternal(k, v.value, expireAt);
                //LOG.info(k.toString());
              } catch (IOException e) {
                LOG.error("Put operation during eviction failed", e);
              }
            }
          }
        }
      )
      .maximumSize(maxSize)
      .build();
  }
  
  /**
   * Get on heap cache maximum size 
   * @return size, 0 - no on heap cache
   */
  public long getOnHeapCacheMaxSize() {
    return this.heapCacheMaxSize;
  }
  
  /**
   * Get on heap cache
   * @return on heap cache
   */
  public com.github.benmanes.caffeine.cache.Cache<Object, ValueExpire> getOnHeapCache() {
    return this.heapCache;
  }
  
  /**
   * Adds Kryo's serialization listener.
   * @param l listener
   */
  public void addSerdeInitializationListener(SerdeInitializationListener l) {
    this.listeners.add(l);
  }

  /**
   * Pre-register classes for Kryo serialization
   * @param values
   */
  public synchronized void registerClasses(Class<?>... values) {
    for (Class<?> cls: values) {
      classList.add(cls);
    }
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
   * Put key - value pair into the cache with expiration (absolute time in ms)
   * @param key key object
   * @param value value object
   * @param expire expiration time (absolute)
   * @param force if true - bypass admission controller
   * @return true on success, false - otherwise
   * @throws IOException
   */
  public boolean put(Object key, Object value, long expire) throws IOException {
    if (this.heapCache != null) {
      // Access Variable Expiration Policy
      var varExpiration = heapCache.policy().expireVariably().orElseThrow();
      ValueExpire ve = new ValueExpire(value, expire);
      Duration d = expire == 0? Duration.ofHours(Integer.MAX_VALUE): Duration.ofMillis(expire - System.currentTimeMillis());
      varExpiration.put(key, ve, d);
      // always success
      return true;
    } else {
      return putInternal(key, value, expire);
    }
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
  public boolean putInternal(Object key, Object value, long expire) throws IOException {
    Objects.requireNonNull(key, "key is null");
    Objects.requireNonNull(value, "value is null");
    Output outKey = getOutput();
    Output outValue = getOutput();
    Kryo kryo = getKryo();
    ValueExpire ve = new ValueExpire(value, expire);
    try {
      kryo.writeObject(outKey, key);
      kryo.writeObject(outValue, ve);
      // TODO: cryptographic hashing of a key
      byte[] keyBuffer = outKey.getBuffer();
      int keyLength = outKey.position();
      byte[] valueBuffer = outValue.getBuffer();
      int valueLength = outValue.position();
      boolean result =
          cache.put(keyBuffer, 0, keyLength, valueBuffer, 0, valueLength, expire, true);
      return result;
    } finally {
      release(outKey);
      release(outValue);
      release(kryo);
    }
  }

  /**
   * Put if absent key - value pair into the cache with expiration (absolute time in ms)
   * @param key key object
   * @param value value object
   * @param expire expiration time (absolute)
   * @param force if true - bypass admission controller
   * @return true on success, false - otherwise
   * @throws IOException
   */
  public boolean putIfAbsent(Object key, Object value, long expire) throws IOException {
    if (this.heapCache != null) {
      // Access Variable Expiration Policy
      var varExpiration = heapCache.policy().expireVariably().orElseThrow();
      ValueExpire ve = new ValueExpire(value, expire);
      Duration d = expire == 0? Duration.ofHours(Integer.MAX_VALUE): Duration.ofMillis(expire - System.currentTimeMillis());
      ValueExpire old = varExpiration.putIfAbsent(key, ve, d);
      // always success
      return old == null;
    } else {
      return putIfAbsentInternal(key, value, expire);
    }
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
  public boolean putIfAbsentInternal(Object key, Object value, long expire) throws IOException {
    Objects.requireNonNull(key, "key is null");
    Objects.requireNonNull(value, "value is null");
    Output outKey = getOutput();
    Output outValue = getOutput();
    Kryo kryo = getKryo();
    ValueExpire ve = new ValueExpire(value, expire);

    try {
      kryo.writeObject(outKey, key);
      kryo.writeObject(outValue, ve);
      // TODO: cryptographic hashing of a key
      byte[] keyBuffer = outKey.getBuffer();
      int keyLength = outKey.position();
      byte[] valueBuffer = outValue.getBuffer();
      int valueLength = outValue.position();
      boolean result =
          cache.putIfAbsent(keyBuffer, 0, keyLength, valueBuffer, 0, valueLength, expire);
      return result;
    } finally {
      release(outKey);
      release(outValue);
      release(kryo);
    }
  }
  
  /**
   * Get value by key
   * @param key object key
   * @return value
   * @throws IOException
   */
  public Object get(Object key) throws IOException {
    if (this.heapCache != null) {
      ValueExpire ve = heapCache.getIfPresent(key);
      if (ve != null) {
        return ve.value;
      }
    }
    ValueExpire ve = (ValueExpire) getInternal(key);
    if (ve != null) {
      return ve.value;
    }
    return null;
  }
  
  /**
   * Get cached value
   * @param key object key
   * @return value or null
   * @throws IOException
   */
  public Object getInternal(Object key) throws IOException {
    Objects.requireNonNull(key, "key is null");
    Output outKey = getOutput();
    Input in = getInput();
    Kryo kryo = getKryo();
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
      ValueExpire value = kryo.readObject(in, ValueExpire.class);
      if (this.heapCache != null) {
        long expireAt = value.expireAt;
        if (expireAt > System.currentTimeMillis()) {
          Duration d = expireAt == 0? Duration.ofHours(Long.MAX_VALUE): Duration.ofMillis(expireAt = System.currentTimeMillis());
          // Access Variable Expiration Policy
          var varExpiration = heapCache.policy().expireVariably().orElseThrow();
          varExpiration.put(key, value, d);
          //FIXME: optimize
          //deleteInternal(key);
        }
      }
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
  public Object get(Object key, Callable<?> valueLoader, long expire) throws IOException {
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
            // This time is absolute
            if (expire > 0) {
              put(key, value, expire);
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

  public boolean delete(Object key) throws IOException {
    if (this.heapCache != null) {
      if (this.heapCache.getIfPresent(key) != null) {
        this.heapCache.invalidate(key);
        return true;
      }
    }
    return deleteInternal(key);
  }
  
  /**
   * Delete object by key
   * @param key object key
   * @return true on success, false - otherwise
   * @throws IOException
   */
  public boolean deleteInternal(Object key) throws IOException {
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
   * Exists object
   * @param key object's key
   * @return true or false
   * @throws IOException
   */
  public boolean exists(Object key) throws IOException {
    if (this.heapCache != null) {
      if (this.heapCache.getIfPresent(key) != null) {
        return true;
      }
    }
    return existsInternal(key);
  }
  
  /**
   * Does key exist
   * @param key object key
   * @return true on success, false - otherwise
   * @throws IOException
   */
  public boolean existsInternal(Object key) throws IOException {
    Objects.requireNonNull(key, "key is null");
    Output outKey = getOutput();
    Kryo kryo = getKryo();
    try {
      kryo.writeObject(outKey, key);
      // TODO: cryptographic hashing of a key
      byte[] keyBuffer = outKey.getBuffer();
      int keyLength = outKey.position();
      boolean result = cache.existsExact(keyBuffer, 0, keyLength);
      return result;
    } finally {
      release(outKey);
      release(kryo);
    }
  }

  /**
   * Touches the object
   * @param key object's key
   * @return true if success, false otherwise
   * @throws IOException
   */
  public boolean touch(Object key) throws IOException {
    if (this.heapCache != null) {
      if (this.heapCache.getIfPresent(key) != null) {
        return true;
      }
    }
    return touchInternal(key);
  }
  
  /**
   * Touch the key
   * @param key object key
   * @return true on success, false - otherwise (key does not exists)
   * @throws IOException
   */
  public boolean touchInternal(Object key) throws IOException {
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
    CacheConfig conf = this.cache.getCacheConfig();
    boolean persists = conf.isSaveOnShutdown(getName());
    if (persists && this.heapCache != null) {
      long start = System.currentTimeMillis();
      ConcurrentMap<Object, ValueExpire> map = this.heapCache.asMap();
      for(Map.Entry<Object, ValueExpire> e: map.entrySet()) {
        long expireAt = e.getValue().expireAt;
        if (expireAt == 0 || expireAt > System.currentTimeMillis()) {
          putInternal(e.getKey(), e.getValue().value, expireAt);
        }
      }
      long end = System.currentTimeMillis();
      LOG.info("Saved on-heap cache {} entries in {}ms", map.size(), (end - start));
    }
    this.cache.shutdown();
  }

  /**
   * Loads saved object cache. Make sure that CacheConfig was already set for the cache
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
      for (Class<?> cls : classList) {
        kryo.register(cls);
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
