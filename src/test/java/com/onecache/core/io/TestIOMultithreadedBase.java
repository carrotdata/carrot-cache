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
package com.onecache.core.io;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;

import org.junit.BeforeClass;
import org.junit.Test;

import com.onecache.core.util.TestUtils;
import com.onecache.core.util.UnsafeAccess;
import com.onecache.core.util.Utils;

public abstract class TestIOMultithreadedBase {
  
  protected static ThreadLocal<byte[][]> keysTL = new ThreadLocal<byte[][]>();
  protected static ThreadLocal<byte[][]> valuesTL = new ThreadLocal<byte[][]>();
  protected static ThreadLocal<long[]> mKeysTL = new ThreadLocal<long[]>();
  protected static ThreadLocal<long[]> mValuesTL = new ThreadLocal<long[]>();
  protected static ThreadLocal<long[]> mExpiresTL = new ThreadLocal<long[]>();
  
  protected static int maxKeySize = 32;
  protected static int maxValueSize = 5000;
    
  protected int numRecords = 10;
  protected int numThreads = 2;
  protected int blockSize = 4096;
  
  @BeforeClass
  public static void enableMallocDebug() {
    //UnsafeAccess.setMallocDebugEnabled(true);
  }
  
  protected void joinAll(Thread[] workers) {
    for (Thread t : workers) {
      try {
        t.join();
      } catch(Exception e) {
      }
    }
  }
  
  protected Thread[] startAll(Runnable r) {
    Thread[] workers = new Thread[numThreads];
    for (int i = 0; i < workers.length; i++) {
      workers[i] = new Thread(r);
      workers[i].start();
    }
    return workers;
  }
  
  protected void prepareData() {
    byte[][] keys = new byte[numRecords][];
    byte[][] values = new byte[numRecords][];
    long[] mKeys = new long[numRecords];
    long[] mValues = new long[numRecords];
    long[] expires = new long[numRecords];

    long seed = Thread.currentThread().getId() * 100000 + System.currentTimeMillis();
    Random r = new Random(seed);
    System.out.println("seed=" + seed);

    for (int i = 0; i < numRecords; i++) {
      int keySize = nextKeySize(r);
      int valueSize = nextValueSize(r);
      keys[i] = TestUtils.randomBytes(keySize, r);
      values[i] = TestUtils.randomBytes(valueSize, r);
      mKeys[i] = TestUtils.randomMemory(keySize, r);
      mValues[i] = TestUtils.randomMemory(valueSize, r);
      expires[i] = getExpire(i); // To make sure that we have distinct expiration values
    }
    keysTL.set(keys);
    valuesTL.set(values);
    mKeysTL.set(mKeys);
    mValuesTL.set(mValues);
    mExpiresTL.set(expires);
  }
  
  protected long getExpireStream(long startTime, int n) {
    return startTime + 1000000L;
  }
  
  protected long getExpire(int n) {
    return System.currentTimeMillis() + 1000000L;
  }
  
  protected int nextKeySize(Random r) {
    int size = maxKeySize / 2 + r.nextInt(maxKeySize / 2);
    return size;
  }

  protected int nextValueSize(Random r) {
    int size = 1 + r.nextInt(maxValueSize - 1);
    return size;
  }
  
  protected void clearData() {
    long[] mKeys = mKeysTL.get();
    long[] mValues = mValuesTL.get();
    Arrays.stream(mKeys).forEach(x -> UnsafeAccess.free(x));
    Arrays.stream(mValues).forEach(x -> UnsafeAccess.free(x));
  }
  
  /**
   * Put operation
   * @param key key 
   * @param value value
   * @param expire expiration time
   * @return true on success, false - otherwise
   */
  protected abstract boolean put(byte[] key, byte[] value, long expire) throws IOException;
  
  /**
   * Put operation
   * @param keyPtr key address
   * @param keySize key size
   * @param valuePtr value address
   * @param valueSize value size
   * @param expire expiration time
   * @return true on success, false - otherwise
   */
  protected abstract boolean put(long keyPtr, int keySize, long valuePtr, int valueSize, long expire) throws IOException;

  /**
   * Delete operation
   * @param key key buffer
   * @param off offset
   * @param len key length
   * @return true on success, false - otherwise
   */
  protected abstract boolean delete(byte[] key, int off, int len) throws IOException;
  
  /**
   * Delete operation
   * @param keyPtr key address
   * @param keySize key size
   * @return true on success, false - otherwise
   */
  protected abstract boolean delete(long keyPtr, int keySize) throws IOException;
  
  /**
   * Get operation
   * @param key key buffer
   * @param off key offset
   * @param len key length
   * @param buffer buffer
   * @param bufferOfset buffer offset
   * @return size of k-v pair
   */
  protected abstract long get(byte[] key, int off, int len, byte[] buffer, int bufferOfset) throws IOException;
  
  /**
   * Get operation
   * @param keyPtr key address
   * @param keySize key size
   * @param buffer byte buffer
   * @return size of k-v pair
   */
  protected abstract long get(long keyPtr, int keySize, ByteBuffer buffer) throws IOException;
  
  
  protected int loadBytes() throws IOException {
    int count = 0;
    byte[][] keys = keysTL.get();
    byte[][] values = valuesTL.get();
    long[] expires = mExpiresTL.get();
    
    while(count < this.numRecords) {
      long expire = expires[count];
      byte[] key = keys[count];
      byte[] value = values[count];      
      boolean result = put(key, value, expire);
      if (!result) {
        break;
      }
      count++;
    }    
    return count;
  }
  
  protected int deleteBytes(int num) throws IOException {
    int count = 0;
    byte[][] keys = keysTL.get();
    while(count < num) {
      byte[] key = keys[count];
      boolean result = delete(key, 0, key.length);
      if (result == false) {
        System.out.println("failed "+ count);
        count++;
        continue;
      }
      assertTrue(result);
      count++;
    }    
    return count;
  }



  protected int loadMemory() throws IOException {
    int count = 0;
    byte[][] keys = keysTL.get();
    byte[][] values = valuesTL.get();
    long[] expires = mExpiresTL.get();
    long[] mKeys = mKeysTL.get();
    long[] mValues = mValuesTL.get();
    while(count < this.numRecords) {
      long expire = expires[count];
      long keyPtr = mKeys[count];
      int keySize = keys[count].length;
      long valuePtr = mValues[count];
      int valueSize = values[count].length;
      boolean result = put(keyPtr, keySize, valuePtr, valueSize, expire);
      if (!result) {
        break;
      }
      count++;
    }    
    return count;
  }
  
  protected int deleteMemory(int num) throws IOException {
    int count = 0;
    byte[][] keys = keysTL.get();
    long[] mKeys = mKeysTL.get();

    while(count < num) {
      int keySize = keys[count].length;
      long keyPtr = mKeys[count];
      boolean result = delete(keyPtr, keySize);
      if (result == false) {
        System.out.println("failed "+ count);
        count++;
        continue;
      }
      assertTrue(result);
      count++;
    }    
    return count;
  }
  
  protected void verifyBytes(int num) throws IOException {
    int kvSize = safeBufferSize();
    byte[] buffer = new byte[kvSize];
    byte[][] keys = keysTL.get();
    byte[][] values = valuesTL.get();
    
    for (int i = 0; i < num; i++) {
      byte[] key = keys[i];
      byte[] value = values[i];
      long expSize = Utils.kvSize(key.length, value.length);
      long size = get(key, 0, key.length, buffer, 0);
      if (size != expSize) {
        System.out.println(Thread.currentThread().getName() + " i=" + i + " num=" + num);
        continue;
      }
      assertEquals(expSize, size);
      int kSize = Utils.readUVInt(buffer, 0);
      assertEquals(key.length, kSize);
      int kSizeSize = Utils.sizeUVInt(kSize);
      int vSize = Utils.readUVInt(buffer, kSizeSize);
      assertEquals(value.length, vSize);
      int vSizeSize = Utils.sizeUVInt(vSize);
      int off = kSizeSize + vSizeSize;
      assertTrue( Utils.compareTo(buffer, off, kSize, key, 0, key.length) == 0);
      off += kSize;
      assertTrue( Utils.compareTo(buffer, off, vSize, value, 0, value.length) == 0);
    }
  }
  
  protected void verifyBytesWithDeletes(int num, int deleted) throws IOException {
    int kvSize = safeBufferSize();
    byte[] buffer = new byte[kvSize];
    byte[][] keys = keysTL.get();
    byte[][] values = valuesTL.get();
    
    for (int i = 0; i < num; i++) {
      byte[] key = keys[i];
      byte[] value = values[i];
      long expSize = Utils.kvSize(key.length, value.length);
      long size = get(key, 0, key.length, buffer, 0);
      if (i < deleted) {
        assertTrue(size < 0);
        continue;
      }
      assertEquals(expSize, size);
      int kSize = Utils.readUVInt(buffer, 0);
      assertEquals(key.length, kSize);
      int kSizeSize = Utils.sizeUVInt(kSize);
      int vSize = Utils.readUVInt(buffer, kSizeSize);
      assertEquals(value.length, vSize);
      int vSizeSize = Utils.sizeUVInt(vSize);
      int off = kSizeSize + vSizeSize;
      assertTrue( Utils.compareTo(buffer, off, kSize, key, 0, key.length) == 0);
      off += kSize;
      assertTrue( Utils.compareTo(buffer, off, vSize, value, 0, value.length) == 0);
    }
  }
  
  protected void verifyMemory(int num) throws IOException {
    int kvSize = safeBufferSize();
    ByteBuffer buffer = ByteBuffer.allocate(kvSize);
    byte[][] keys = keysTL.get();
    byte[][] values = valuesTL.get();
    long[] mKeys = mKeysTL.get();
    long[] mValues = mValuesTL.get();
    
    for (int i = 0; i < num; i++) {
      int keySize = keys[i].length;
      int valueSize = values[i].length;
      long keyPtr = mKeys[i];
      long valuePtr = mValues[i];
      
      long expSize = Utils.kvSize(keySize, valueSize);
      long size = get(keyPtr, keySize, buffer);
      if (size != expSize) {
        System.out.println(Thread.currentThread().getName() + " i=" + i + " num=" + num);
        continue;
      }
      assertEquals(expSize, size);
      int kSize = Utils.readUVInt(buffer);
      assertEquals(keySize, kSize);
      int kSizeSize = Utils.sizeUVInt(kSize);
      int off = kSizeSize;
      buffer.position(off);
      int vSize = Utils.readUVInt(buffer);
      assertEquals(valueSize, vSize);
      int vSizeSize = Utils.sizeUVInt(vSize);
      off += vSizeSize;
      buffer.position(off);
      assertTrue( Utils.compareTo(buffer, kSize, keyPtr, keySize) == 0);
      off += kSize;
      buffer.position(off);
      assertTrue( Utils.compareTo(buffer, vSize, valuePtr, valueSize) == 0);
      buffer.clear();
    }    
  }
  
  protected void verifyMemoryWithDeletes(int num, int deleted) throws IOException {
    int kvSize = safeBufferSize();
    ByteBuffer buffer = ByteBuffer.allocate(kvSize);
    byte[][] keys = keysTL.get();
    byte[][] values = valuesTL.get();
    long[] mKeys = mKeysTL.get();
    long[] mValues = mValuesTL.get();
    for (int i = 0; i < num; i++) {
      int keySize = keys[i].length;
      int valueSize = values[i].length;
      long keyPtr = mKeys[i];
      long valuePtr = mValues[i];
      
      long expSize = Utils.kvSize(keySize, valueSize);
      long size = get(keyPtr, keySize, buffer);
      if (i < deleted) {
        assertTrue(size < 0);
        continue;
      }
      assertEquals(expSize, size);
      int kSize = Utils.readUVInt(buffer);
      assertEquals(keySize, kSize);
      int kSizeSize = Utils.sizeUVInt(kSize);
      int off = kSizeSize;
      buffer.position(off);
      int vSize = Utils.readUVInt(buffer);
      assertEquals(valueSize, vSize);
      int vSizeSize = Utils.sizeUVInt(vSize);
      off += vSizeSize;
      buffer.position(off);
      assertTrue( Utils.compareTo(buffer, kSize, keyPtr, keySize) == 0);
      off += kSize;
      buffer.position(off);
      assertTrue( Utils.compareTo(buffer, vSize, valuePtr, valueSize) == 0);
      buffer.clear();
    }    
  }
  
  protected int safeBufferSize() {
    int bufSize = Utils.kvSize(maxKeySize, maxValueSize);
    return (bufSize / blockSize + 1) * blockSize;
  }
  
  @Test
  public void testLoadReadBytesRun() {
    Runnable r = () -> {
      try {
        testLoadReadBytes();
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
    }};
    
    Thread[] all = startAll(r);
    joinAll(all);
  }
  
  private void testLoadReadBytes() throws IOException {
    /*DEBUG*/ System.out.println(Thread.currentThread().getName() + 
      ": testLoadReadBytes");
    prepareData();
    int loaded = loadBytes();
    /*DEBUG*/ System.out.println(Thread.currentThread().getName() + ": loaded=" + loaded);
    verifyBytes(loaded);
    /*DEBUG*/ System.out.println(Thread.currentThread().getName() + ": verified=" + loaded);
    clearData();
  }
  
  @Test
  public void testLoadReadMemoryRun() {
    Runnable r = () -> {
      try {
        testLoadReadMemory();
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
    }};
    
    Thread[] all = startAll(r);
    joinAll(all);
  }  
  
  private void testLoadReadMemory() throws IOException {
    /*DEBUG*/ System.out.println(Thread.currentThread().getName() + 
      ": testLoadReadMemory");
    prepareData();
    int loaded = loadMemory();
    /*DEBUG*/ System.out.println(Thread.currentThread().getName() + ": loaded=" + loaded);
    verifyMemory(loaded);
    clearData();
  }
  
  @Test
  public void testLoadReadBytesWithDeletesRun() {
    Runnable r = () -> {
      try {
        testLoadReadBytesWithDeletes();
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
    }};
    
    Thread[] all = startAll(r);
    joinAll(all);
  }  
  
  private void testLoadReadBytesWithDeletes() throws IOException {
    /*DEBUG*/ System.out.println(Thread.currentThread().getName() + 
      ": testLoadReadBytesWithDeletes");
    prepareData();
    int loaded = loadBytes();
    /*DEBUG*/ System.out.println(Thread.currentThread().getName() + ": loaded=" + loaded);
    deleteBytes(loaded / 2);
    verifyBytesWithDeletes(loaded, loaded / 2);
    clearData();
  }
  
  @Test
  public void testLoadReadMemoryWithDeletesRun() {
    Runnable r = () -> {
      try {
        testLoadReadMemoryWithDeletes();
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
    }};
    
    Thread[] all = startAll(r);
    joinAll(all);
  }  
  
  private void testLoadReadMemoryWithDeletes() throws IOException {
    /*DEBUG*/ System.out.println(Thread.currentThread().getName() + 
      ": testLoadReadMemoryWithDeletes");
    prepareData();
    int loaded = loadMemory();
    /*DEBUG*/ System.out.println(Thread.currentThread().getName() + ": loaded=" + loaded);
    deleteMemory(loaded / 2);
    verifyMemoryWithDeletes(loaded, loaded / 2);
    clearData();
  }
  
}
