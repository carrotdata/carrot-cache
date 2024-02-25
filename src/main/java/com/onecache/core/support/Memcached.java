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
package com.onecache.core.support;

import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.onecache.core.Cache;
import com.onecache.core.util.CarrotConfig;
import com.onecache.core.util.LockSupport;
import com.onecache.core.util.UnsafeAccess;
import com.onecache.core.util.Utils;

public class Memcached {
  /** Logger */
  private static final Logger LOG = LogManager.getLogger(Memcached.class);
  static class Record {
    byte[] value;
    int offset;
    int size;
    long cas;
    long expire;
    int flags;
    boolean error;
  }
  
  static enum OpResult {
    STORED, NOT_STORED, EXISTS, NOT_FOUND, DELETED, ERROR;
  }
  
  private static int INIT_SIZE = 1 << 16;
  
  private static ThreadLocal<byte[]> buffer = new ThreadLocal<byte[]>() {
    @Override
    protected byte[] initialValue() {
      return new byte[INIT_SIZE];
    }
  };
  
  private static ThreadLocal<Long> memory = new ThreadLocal<Long>() {
    protected Long initialValue() {
      long ptr = UnsafeAccess.mallocZeroed(INIT_SIZE);
      return ptr;
    }
  };
  
  private static ThreadLocal<Long> memorySize = new ThreadLocal<Long> (){
    @Override
    protected Long initialValue() {
      return (long) INIT_SIZE;
    }
  };
  
  private static void allocBuffer(int sizeRequired) {
    byte[] b = buffer.get();
    if (b.length < sizeRequired) {
      b = new byte[sizeRequired];
      buffer.set(b);
    }
  }
  
  private static void allocMemory(int sizeRequired) {
    if (memorySize.get() >= sizeRequired) {
      return;
    }
    UnsafeAccess.free(memory.get());
    memorySize.set((long) sizeRequired);
    long ptr = UnsafeAccess.mallocZeroed(sizeRequired);
    memory.set(ptr);
  }
  
  private static void reallocBuffer(int sizeRequired) {
    byte[] b = buffer.get();
    if (b.length < sizeRequired) {
      byte[] bb = new byte[sizeRequired];
      System.arraycopy(b, 0, bb, 0, b.length);
      buffer.set(bb);
    }
  }
  
  private static void reallocMemory(int sizeRequired) {
    long ptr = memory.get();
    long size = memorySize.get();
    if (size >= sizeRequired) {
      return;
    }
    memorySize.set((long) sizeRequired);
    long $ptr = UnsafeAccess.mallocZeroed(sizeRequired);
    UnsafeAccess.copy(ptr, $ptr, size);
    UnsafeAccess.free(memory.get());
    memory.set($ptr);
  }
  
  private Cache cache;
  
  public Memcached(Cache cache) {
    CarrotConfig config = CarrotConfig.getInstance();
    if(!config.isCacheTLSSupported(cache.getName())) {
      throw new RuntimeException("thread-local storage support must be enabled");
    }
    this.cache = cache;
  }
  
  
  /************** Storage commands ******************/
  
  /**
   * Set operation
   * @param key key buffer
   * @param keyOffset key offset
   * @param keySize key size
   * @param value value buffer
   * @param valueOffset value offset
   * @param valueSize value size
   * @param flags flags
   * @param expTime expiration time in ms since 01011970
   * @return operation result
   */
  public OpResult set(byte[] key, int keyOffset, int keySize, byte[] value, int valueOffset, 
        int valueSize, int flags, long expTime) {
    allocBuffer(valueSize + Utils.SIZEOF_INT);
    byte[] b = buffer.get();
    // Copy value
    System.arraycopy(value, valueOffset, b, 0, valueSize);
    // Add flags
    UnsafeAccess.putInt(b, valueSize, flags);
    
    try {
      boolean result = cache.put(key, keyOffset, keySize, b, 0, valueSize + Utils.SIZEOF_INT, expTime);
      return result? OpResult.STORED: OpResult.NOT_STORED;
    } catch (IOException e) {
      LOG.error(e);
      return OpResult.ERROR;
    }
  }
  
  /**
   * Set operation
   * @param keyPtr key address
   * @param keySize key size
   * @param valuePtr value address
   * @param valueSize value size
   * @param flags flags
   * @param expTime expiration time
   * @return operation result
   */
  public OpResult set(long keyPtr, int keySize, long valuePtr, int valueSize, int flags,
      long expTime) {
    allocMemory(valueSize + Utils.SIZEOF_INT);
    long ptr = memory.get();
    UnsafeAccess.copy(valuePtr, ptr, valueSize);
    // Add flags
    UnsafeAccess.putInt(ptr + valueSize, flags);

    try {
      boolean result = cache.put(keyPtr, keySize, ptr, valueSize + Utils.SIZEOF_INT, expTime);
      return result ? OpResult.STORED : OpResult.NOT_STORED;
    } catch (IOException e) {
      LOG.error(e);
      return OpResult.ERROR;
    }
  }
  
  /**
   * Set operation
   * @param keyPtr key address
   * @param keySize key size
   * @param valuePtr value address
   * @param valueSize value size
   * @param flags flags
   * @param expTime expiration time
   * @return operation result
   */
  public OpResult set(long keyPtr, int keySize, byte[] value, int valueOffset, int valueSize, int flags,
      long expTime) {
    allocMemory(valueSize + Utils.SIZEOF_INT);
    long ptr = memory.get();
    UnsafeAccess.copy(value, valueOffset, ptr, valueSize);
    // Add flags
    UnsafeAccess.putInt(ptr + valueSize, flags);

    try {
      boolean result = cache.put(keyPtr, keySize, ptr, valueSize + Utils.SIZEOF_INT, expTime);
      return result ? OpResult.STORED : OpResult.NOT_STORED;
    } catch (IOException e) {
      LOG.error(e);
      return OpResult.ERROR;
    }
  }
  
  /**
   * Add operation (atomic)
   * @param key key buffer
   * @param keyOffset key offset
   * @param keySize key size
   * @param value value buffer
   * @param valueOffset value offset
   * @param valueSize value size
   * @param flags flags
   * @param expTime expiration time
   * @return operation result
   */
  public OpResult add(byte[] key, int keyOffset, int keySize, byte[] value, int valueOffset,
      int valueSize, int flags, long expTime) {
    // This operation is atomic
    try {
      allocBuffer(valueSize + Utils.SIZEOF_INT);
      byte[] b = buffer.get();
      // Copy value
      System.arraycopy(value, valueOffset, b, 0, valueSize);
      // Add flags
      UnsafeAccess.putInt(b, valueSize, flags);

      LockSupport.lock(key, keyOffset, keySize);
      if (cache.existsExact(key, keyOffset, keySize)) {
        return OpResult.NOT_STORED;
      }
      boolean result =
          cache.put(key, keyOffset, keySize, b, 0, valueSize + Utils.SIZEOF_INT, expTime);
      return result ? OpResult.STORED : OpResult.ERROR;
    } catch (IOException e) {
      LOG.error(e);
      return OpResult.ERROR;
    } finally {
      LockSupport.unlock(key, keyOffset, keySize);
    }
  }
  
  /**
   * Add operation (atomic)
   * @param keyPtr key address
   * @param keySize key size
   * @param valuePtr value address
   * @param valueSize value size
   * @param flags flags
   * @param expTime expiration time
   * @return operation result
   */
  public OpResult add(long keyPtr, int keySize, long valuePtr, 
      int valueSize, int flags, long expTime) {
    // This operation is atomic
    try {
      allocMemory(valueSize + Utils.SIZEOF_INT);
      long ptr = memory.get();
      UnsafeAccess.copy(valuePtr, ptr, valueSize);
      // Add flags
      UnsafeAccess.putInt(ptr + valueSize, flags);

      LockSupport.lock(keyPtr, keySize);
      if (cache.existsExact(keyPtr, keySize)) {
        return OpResult.NOT_STORED;
      }
      boolean result =
          cache.put(keyPtr, keySize, ptr, valueSize + Utils.SIZEOF_INT, expTime);
      return result ? OpResult.STORED : OpResult.ERROR;
    } catch (IOException e) {
      LOG.error(e);
      return OpResult.ERROR;
    } finally {
      LockSupport.unlock(keyPtr, keySize);
    }
  }
  /**
   * Replace (atomic)
   * @param key key buffer
   * @param keyOffset key offset
   * @param keySize key size
   * @param value value buffer
   * @param valueOffset value offset
   * @param valueSize value size
   * @param flags flags
   * @param expTime expiration time
   * @return operation result
   */
  public OpResult replace(byte[] key, int keyOffset, int keySize, byte[] value, int valueOffset, 
      int valueSize, int flags, long expTime) {
    // This operation is atomic
    try {
      allocBuffer(valueSize + Utils.SIZEOF_INT);
      byte[] b = buffer.get();
      // Copy value
      System.arraycopy(value, valueOffset, b, 0, valueSize);
      // Add flags
      UnsafeAccess.putInt(b, valueSize, flags);

      LockSupport.lock(key, keyOffset, keySize);
      if (!cache.existsExact(key, keyOffset, keySize)) {
        return OpResult.NOT_STORED;
      }
      boolean result =
          cache.put(key, keyOffset, keySize, b, 0, valueSize + Utils.SIZEOF_INT, expTime);
      return result ? OpResult.STORED : OpResult.ERROR;
    } catch (IOException e) {
      LOG.error(e);
      return OpResult.ERROR;
    } finally {
      LockSupport.unlock(key, keyOffset, keySize);
    }
  }
  
  /**
   * Replace operation (atomic)
   * @param keyPtr key address
   * @param keySize key size
   * @param valuePtr value address
   * @param valueSize value size
   * @param flags flags
   * @param expTime expiration time
   * @return operation result
   */
  public OpResult replace(long keyPtr, int keySize, long valuePtr,  
      int valueSize, int flags, long expTime) {
    // This operation is atomic
    try {
      allocMemory(valueSize + Utils.SIZEOF_INT);
      long ptr = memory.get();
      UnsafeAccess.copy(valuePtr, ptr, valueSize);
      // Add flags
      UnsafeAccess.putInt(ptr + valueSize, flags);

      LockSupport.lock(keyPtr, keySize);
      if (!cache.existsExact(keyPtr, keySize)) {
        return OpResult.NOT_STORED;
      }
      boolean result =
          cache.put(keyPtr, keySize, ptr, valueSize + Utils.SIZEOF_INT, expTime);
      return result ? OpResult.STORED : OpResult.ERROR;
    } catch (IOException e) {
      LOG.error(e);
      return OpResult.ERROR;
    } finally {
      LockSupport.unlock(keyPtr, keySize);
    }
  }
  
  /**
   * Append operation (atomic)
   * @param key key buffer
   * @param keyOffset key offset
   * @param keySize key size
   * @param value value buffer
   * @param valueOffset value offset
   * @param valueSize value size
   * @param flags flags
   * @param expTime expiration time
   * @return operation result
   */
  public OpResult append(byte[] key, int keyOffset, int keySize, byte[] value, int valueOffset, 
      int valueSize, int flags, long expTime) {
    // This operation is atomic
    try {
      LockSupport.lock(key, keyOffset, keySize);
      Record r = get(key, keyOffset, keySize);
      if (r.value == null) {
        return OpResult.NOT_STORED;
      }
      int size = r.size;
      // r.offset = 0
      int requiredSize = size + valueSize + Utils.SIZEOF_INT;
      reallocBuffer(requiredSize);
      byte[] b = buffer.get();
      // Copy value
      System.arraycopy(value, valueOffset, b, size, valueSize);
      // Add flags
      UnsafeAccess.putInt(b, size + valueSize, flags);
      boolean result =
          cache.put(key, keyOffset, keySize, b, 0, requiredSize, expTime);
      return result ? OpResult.STORED : OpResult.NOT_STORED;
    } catch (IOException e) {
      LOG.error(e);
      return OpResult.ERROR;
    } finally {
      LockSupport.unlock(key, keyOffset, keySize);
    }
  }
  
  /**
   * Append operation (atomic)
   * @param keyPtr key address
   * @param keySize key size
   * @param valuePtr value address
   * @param valueSize value size
   * @param flags flags
   * @param expTime expiration time
   * @return operation result
   */
  public OpResult append(long keyPtr, int keySize, long valuePtr,  
      int valueSize, int flags, long expTime) {
    // This operation is atomic
    try {
      LockSupport.lock(keyPtr, keySize);
      Record r = get(keyPtr, keySize);
      if (r.value == null) {
        return OpResult.NOT_STORED;
      }
      int size = r.size;
      // r.offset = 0
      int requiredSize = size + valueSize + Utils.SIZEOF_INT;
      reallocMemory(requiredSize);
      long ptr = memory.get();
      // Copy value
      UnsafeAccess.copy(valuePtr, ptr + size, valueSize);
      // Add flags
      UnsafeAccess.putInt(ptr + size + valueSize, flags);
      boolean result =
          cache.put(keyPtr, keySize, ptr, requiredSize, expTime);
      return result ? OpResult.STORED : OpResult.NOT_STORED;
    } catch (IOException e) {
      LOG.error(e);
      return OpResult.ERROR;
    } finally {
      LockSupport.unlock(keyPtr, keySize);
    }
  }
  
  /**
   * Append (atomic)
   * @param key key buffer
   * @param keyOffset key offset
   * @param keySize key size
   * @param value value buffer
   * @param valueOffset value offset
   * @param valueSize value size
   * @param flags flags
   * @param expTime expiration time
   * @return operation result
   */
  public OpResult prepend(byte[] key, int keyOffset, int keySize, byte[] value, int valueOffset, 
      int valueSize, int flags, long expTime) {
    // This operation is atomic
    try {
      LockSupport.lock(key, keyOffset, keySize);
      Record r = get(key, keyOffset, keySize);
      if (r.value == null) {
        return OpResult.NOT_STORED;
      }
      int size = r.size;
      // r.offset = 0
      int requiredSize = size + valueSize + Utils.SIZEOF_INT;
      reallocBuffer(requiredSize);
      byte[] b = buffer.get();
      // Move existing value by valueSize
      System.arraycopy(b,  0,  b, valueSize, size);
      // Copy value
      System.arraycopy(value, valueOffset, b, 0, valueSize);
      // Add flags
      UnsafeAccess.putInt(b, size + valueSize, flags);
      boolean result =
          cache.put(key, keyOffset, keySize, b, 0, requiredSize, expTime);
      return result ? OpResult.STORED : OpResult.NOT_STORED;
    } catch (IOException e) {
      LOG.error(e);
      return OpResult.ERROR;
    } finally {
      LockSupport.unlock(key, keyOffset, keySize);
    }
  }
  
  /**
   * Prepend operation (atomic)
   * @param keyPtr key address
   * @param keySize key size
   * @param valuePtr value address
   * @param valueSize value size
   * @param flags flags
   * @param expTime expiration time
   * @return operation result
   */
  public OpResult prepend(long keyPtr, int keySize, long valuePtr,  
      int valueSize, int flags, long expTime) {
    // This operation is atomic
    try {
      LockSupport.lock(keyPtr, keySize);
      Record r = get(keyPtr, keySize);
      if (r.value == null) {
        return OpResult.NOT_STORED;
      }
      int size = r.size;
      // r.offset = 0
      int requiredSize = size + valueSize + Utils.SIZEOF_INT;
      reallocMemory(requiredSize);
      long ptr = memory.get();
      // Move existing value by valueSize
      UnsafeAccess.copy(ptr, ptr + valueSize, size);
      // Copy value
      UnsafeAccess.copy(valuePtr, ptr, valueSize);
      // Add flags
      UnsafeAccess.putInt(ptr + size + valueSize, flags);
      boolean result =
          cache.put(keyPtr, keySize, ptr, requiredSize, expTime);
      return result ? OpResult.STORED : OpResult.NOT_STORED;
    } catch (IOException e) {
      LOG.error(e);
      return OpResult.ERROR;
    } finally {
      LockSupport.unlock(keyPtr, keySize);
    }
  }
  
  /**
   * CAS (compare-and-swap) (atomic) can be optimized
   * @param key key buffer
   * @param keyOffset key offset
   * @param keySize key size
   * @param value value buffer
   * @param valueOffset value offset
   * @param valueSize value size
   * @param flags flags
   * @param expTime expiration time
   * @param cas CAS unique
   * @return operation result
   */
  public OpResult cas(byte[] key, int keyOffset, int keySize, byte[] value, int valueOffset, 
      int valueSize, int flags, long expTime, long cas) {
    // This operation is atomic
    try {
      LockSupport.lock(key, keyOffset, keySize);
      Record r = get(key, keyOffset, keySize);
      if (r.value == null) {
        return OpResult.NOT_FOUND;
      }
      long $cas = computeCAS(r.value, r.offset, r.size);
      if(cas != $cas) {
        return OpResult.EXISTS;
      }
      int requiredSize = valueSize + Utils.SIZEOF_INT;
      allocBuffer(requiredSize);
      byte[] b = buffer.get();
      // Copy value
      System.arraycopy(value, valueOffset, b, 0, valueSize);
      // Add flags
      UnsafeAccess.putInt(b, valueSize, flags);
      boolean result =
          cache.put(key, keyOffset, keySize, b, 0, requiredSize, expTime);
      return result ? OpResult.STORED : OpResult.NOT_STORED;
    } catch (IOException e) {
      LOG.error(e);
      return OpResult.ERROR;
    } finally {
      LockSupport.unlock(key, keyOffset, keySize);
    }
  }
  
  /**
   * CAS (compare-and-swap) operation (atomic)
   * @param keyPtr key address
   * @param keySize key size
   * @param valuePtr value address
   * @param valueSize value size
   * @param flags flags
   * @param expTime expiration time
   * @param cas CAS unique
   * @return operation result
   */
  public OpResult cas(long keyPtr, int keySize, long valuePtr,  
      int valueSize, int flags, long expTime, long cas) {
    // This operation is atomic
    try {
      LockSupport.lock(keyPtr, keySize);
      Record r = get(keyPtr, keySize);
      if (r.value == null) {
        return OpResult.NOT_FOUND;
      }
      long $cas = computeCAS(r.value, r.offset, r.size);
      if(cas != $cas) {
        return OpResult.EXISTS;
      }
      int requiredSize = valueSize + Utils.SIZEOF_INT;
      allocMemory(requiredSize);
      long ptr = memory.get();
      // Copy value
      UnsafeAccess.copy(valuePtr, ptr, valueSize);
      // Add flags
      UnsafeAccess.putInt(ptr + valueSize, flags);
      boolean result =
          cache.put(keyPtr, keySize, ptr, requiredSize, expTime);
      return result ? OpResult.STORED : OpResult.NOT_STORED;
    } catch (IOException e) {
      LOG.error(e);
      return OpResult.ERROR;
    } finally {
      LockSupport.unlock(keyPtr, keySize);
    }
  }
 
  /**************** Retrieval commands *****************/
  
  /**
   * Get value by key
   * @param key key buffer
   * @param keyOffset key offset
   * @param keySize key size
   * @return result record
   */
  public Record get(byte[] key, int keyOffset, int keySize) {
    Record result = new Record();
    
    byte[] buf = buffer.get();
    try {
      long size = cache.get(key, keyOffset, keySize, buf, 0);
      while (size > buf.length) {
        allocBuffer((int) size);
        buf = buffer.get();
        size = cache.get(key, keyOffset, keySize, buf, 0);
      }
      if (size < 0) {
        // Not found
        return result;
      }
      result.value = buf;
      result.offset = 0;
      result.size = (int)(size - Utils.SIZEOF_INT);
      result.flags = UnsafeAccess.toInt(buf, result.size);
      return result;
    } catch (IOException e) {
      LOG.error(e);
      result.error = true;
      return result;
    }
  }
  
  /**
   * Get value by key
   * @param keyPtr key address
   * @param keySize key size
   * @return result record
   */
  public Record get(long keyPtr, int keySize) {
    Record result = new Record();
    
    byte[] buf = buffer.get();
    try {
      long size = cache.get(keyPtr, keySize, true,  buf, 0);
      while (size > buf.length) {
        allocBuffer((int) size);
        buf = buffer.get();
        size = cache.get(keyPtr, keySize, true, buf, 0);
      }
      if (size < 0) {
        // Not found
        return result;
      }
      result.value = buf;
      result.offset = 0;
      result.size = (int)(size - Utils.SIZEOF_INT);
      result.flags = UnsafeAccess.toInt(buf, result.size);
      return result;
    } catch (IOException e) {
      LOG.error(e);
      result.error = true;
      return result;
    }
  }
  
  /**
   * Get and touch value by key
   * @param key key buffer
   * @param keyOffset key offset
   * @param keySize key size
   * @param newExpire new expire
   * @return result record
   */
  public Record gat(byte[] key, int keyOffset, int keySize, long newExpire) {
    try {
      LockSupport.lock(key, keyOffset, keySize);
      Record r = get(key, keyOffset, keySize);
      if (r.value != null) {
        long expire = touch(key, keyOffset, keySize, newExpire);
        r.expire = expire;
      }
      return r;
    } finally {
      LockSupport.unlock(key, keyOffset, keySize);
    }
  }
  
  /**
   * Get and touch value by key
   * @param keyPtr key address
   * @param keySize key size
   * @return
   */
  public Record gat(long keyPtr, int keySize, long newExpire) {
    try {
      LockSupport.lock(keyPtr, keySize);
      Record r = get(keyPtr, keySize);
      if (r.value != null) {
        long expire =touch(keyPtr, keySize, newExpire);
        r.expire = expire;
      }
      return r;
    } finally {
      LockSupport.unlock(keyPtr, keySize);
    }
  }
  
  /**
   * Get value by key with CAS
   * @param key key buffer
   * @param keyOffset key offset
   * @param keySize key size
   * @return result record
   */
  public Record gets(byte[] key, int keyOffset, int keySize) {
    Record result = new Record();
    
    byte[] buf = buffer.get();
    try {
      long size = cache.get(key, keyOffset, keySize, buf, 0);
      while (size > buf.length) {
        allocBuffer((int) size);
        buf = buffer.get();
        size = cache.get(key, keyOffset, keySize, buf, 0);
      }
      if (size < 0) {
        // Not found
        return result;
      }
      result.value = buf;
      result.offset = 0;
      result.size = (int)(size - Utils.SIZEOF_INT);
      result.flags = UnsafeAccess.toInt(buf, result.size);
      result.cas = computeCAS(buf, 0, result.size);
      return result;
    } catch (IOException e) {
      LOG.error(e);
      result.error = true;
      return result;
    }
  }
  
  /**
   * Get value by key with CAS
   * @param keyPtr key address
   * @param keySize key size
   * @return result record
   */
  public Record gets(long keyPtr, int keySize) {
    Record result = new Record();
    
    byte[] buf = buffer.get();
    try {
      long size = cache.get(keyPtr, keySize, true,  buf, 0);
      while (size > buf.length) {
        allocBuffer((int) size);
        buf = buffer.get();
        size = cache.get(keyPtr, keySize, true, buf, 0);
      }
      if (size < 0) {
        // Not found
        return result;
      }
      result.value = buf;
      result.offset = 0;
      result.size = (int)(size - Utils.SIZEOF_INT);
      result.flags = UnsafeAccess.toInt(buf, result.size);
      result.cas = computeCAS(buf, 0, result.size);
      return result;
    } catch (IOException e) {
      LOG.error(e);
      result.error = true;
      return result;
    }
  }
  
  /**
   * Get and touch value by key with CAS
   * @param key key buffer
   * @param keyOffset key offset
   * @param keySize key size
   * @param newExpire new expire
   * @return result record
   */
  public Record gats(byte[] key, int keyOffset, int keySize, long newExpire) {
    try {
      LockSupport.lock(key, keyOffset, keySize);
      Record r = gets(key, keyOffset, keySize);
      if (r.value != null) {
        long expire = touch(key, keyOffset, keySize, newExpire);
        r.expire = expire;
      }
      return r;
    } finally {
      LockSupport.unlock(key, keyOffset, keySize);
    }
  }
  
  /**
   * Get and touch value by key with CAS
   * @param keyPtr key address
   * @param keySize key size
   * @return
   */
  public Record gats(long keyPtr, int keySize, long newExpire) {
    try {
      LockSupport.lock(keyPtr, keySize);
      Record r = gets(keyPtr, keySize);
      if (r.value != null) {
        long expire = touch(keyPtr, keySize, newExpire);
        r.expire = expire;
      }
      return r;
    } finally {
      LockSupport.unlock(keyPtr, keySize);
    }
  }
  
  /***************** Misc commands **********************/
  
  /**
   * Touch (sets new expiration time)
   * @param key key buffer
   * @param keyOffset key offset
   * @param keySize key size
   * @param expTime new expiration time
   * @return previous expiration time or -1 (if key did exist)
   */
  public long touch(byte[] key, int keyOffset, int keySize, long expTime) {
    return cache.getAndSetExpire(key, keyOffset, keySize, expTime);
  }
  
  /**
   * Touch (sets new expiration time)
   * @param key key buffer
   * @param keyOffset key offset
   * @param keySize key size
   * @param expTime new expiration time
   * @return previous expiration time or -1 (if key did exist)
   */
  public long touch(long keyPtr, int keySize, long expTime) {
    return cache.getAndSetExpire(keyPtr, keySize, expTime);
  }
  
  /**
   * Delete by key
   * @param key key buffer
   * @param keyOffset key offset
   * @param keySize key size
   * @return operation result
   */
  public OpResult delete(byte[] key, int keyOffset, int keySize) {
    try {
      boolean result = cache.delete(key, keyOffset, keySize);
      return result? OpResult.DELETED: OpResult.NOT_FOUND;
    } catch (IOException e) {
      LOG.error(e);
      return OpResult.ERROR;
    }
  }
  
  /**
   * Increment (MUST BE OPTIMIZED)
   * @param key key buffer
   * @param keyOffset key offset
   * @param keySize key size
   * @param v - positive
   * @return -1 - error or new value after increment
   * @throws NumberFormatException
   */
  public long incr(byte[] key, int keyOffset, int keySize, long v) {
    if (v < 0) {
      throw new IllegalArgumentException("increment value must be positive");
    }
    try {
      LockSupport.lock(key, keyOffset, keySize);
      Record r = get(key, keyOffset, keySize);
      if (r.value != null) {
        byte[] b = r.value;
        int off = r.offset;
        int size = r.size;
        long val = Utils.strToLong(b, off, size);
        int numDigits = Utils.longToStr(b, off, val + v);
        long expire = cache.getExpire(key, keyOffset, keySize);
        set(key, keyOffset, keySize, b, off, numDigits, r.flags, expire);
        return val + v;
      }
      return -1;// NOT_FOUND
    } finally {
      LockSupport.unlock(key, keyOffset, keySize);
    }
  }
  
  /**
   * Increment (MUST BE OPTIMIZED)
   * @param key
   * @param keyOffset
   * @param keySize
   * @param v - positive
   * @return -1 - error or new value after increment
   * @throws NumberFormatException
   */
  public long incr(long keyPtr, int keySize, long v) {
    if (v < 0) {
      throw new IllegalArgumentException("increment value must be positive");
    }
    try {
      LockSupport.lock(keyPtr, keySize);
      Record r = get(keyPtr, keySize);
      if (r.value != null) {
        byte[] b = r.value;
        int off = r.offset;
        int size = r.size;
        long val = Utils.strToLong(b, off, size);
        int numDigits = Utils.longToStr(b, off, val + v);
        long expire = cache.getExpire(keyPtr, keySize);
        set(keyPtr, keySize, b, off, numDigits, r.flags, expire);
        return val + v;
      }
      return -1;// NOT_FOUND
    } finally {
      LockSupport.unlock(keyPtr, keySize);
    }
  }
  
  /**
   * Increment (MUST BE OPTIMIZED)
   * @param key key buffer
   * @param keyOffset key offset
   * @param keySize key size
   * @param v - positive
   * @return -1 - error or new value after increment
   * @throws NumberFormatException
   */
  public long decr(byte[] key, int keyOffset, int keySize, long v) {
    if (v < 0) {
      throw new IllegalArgumentException("decrement value must be positive");
    }
    try {
      LockSupport.lock(key, keyOffset, keySize);
      Record r = get(key, keyOffset, keySize);
      if (r.value != null) {
        byte[] b = r.value;
        int off = r.offset;
        int size = r.size;
        long val = Utils.strToLong(b, off, size);
        long newValue = val - v;
        if (newValue < 0) {
          newValue = 0;
        }
        int numDigits = Utils.longToStr(b, off, newValue);
        long expire = cache.getExpire(key, keyOffset, keySize);
        set(key, keyOffset, keySize, b, off, numDigits, r.flags, expire);
        return newValue;
      }
      return -1;// NOT_FOUND
    } finally {
      LockSupport.unlock(key, keyOffset, keySize);
    }
  }
  
  /**
   * Increment (MUST BE OPTIMIZED)
   * @param key
   * @param keyOffset
   * @param keySize
   * @param v - positive
   * @return -1 - error or new value after increment
   * @throws NumberFormatException
   */
  public long decr(long keyPtr, int keySize, long v) {
    if (v < 0) {
      throw new IllegalArgumentException("decrement value must be positive");
    }
    try {
      LockSupport.lock(keyPtr, keySize);
      Record r = get(keyPtr, keySize);
      if (r.value != null) {
        byte[] b = r.value;
        int off = r.offset;
        int size = r.size;
        long val = Utils.strToLong(b, off, size);
        long newValue = val - v;
        if (newValue < 0) {
          newValue = 0;
        }
        int numDigits = Utils.longToStr(b, off, newValue);
        long expire = cache.getExpire(keyPtr, keySize);
        set(keyPtr, keySize, b, off, numDigits, r.flags, expire);
        return newValue;
      }
      return -1;// NOT_FOUND
    } finally {
      LockSupport.unlock(keyPtr, keySize);
    }
  }
  
  
  /**
   * Delete by key
   * @param key key buffer
   * @param keyOffset key offset
   * @param keySize key size
   * @return operation result
   */
  public OpResult delete(long keyPtr, int keySize) {
    try {
      boolean result = cache.delete(keyPtr, keySize);
      return result? OpResult.DELETED: OpResult.NOT_FOUND;
    } catch (IOException e) {
      LOG.error(e);
      return OpResult.ERROR;
    }
  }
  
  
  /*************************** Utility methods ************************/
  
  long computeCAS(byte[] value, int valueOffset, int valueSize) {
    return Utils.hash64(value, valueOffset, valueSize);
  }
  
  @SuppressWarnings("unused")
  long computeCAS(long valuePtr, int valueSize) {
    return Utils.hash64(valuePtr, valueSize);
  }
  
  public void dispose() {
    this.cache.dispose();
  }
  
  public Cache getCache() {
    return this.cache;
  }
}
