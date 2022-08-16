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
package com.carrot.cache.io;


import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.CountDownLatch;

import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import com.carrot.cache.util.TestUtils;
import com.carrot.cache.util.UnsafeAccess;
import com.carrot.cache.util.Utils;

public abstract class TestIOEngineMultithreadedBase {
  
  static ThreadLocal<byte[][]> keysTL = new ThreadLocal<byte[][]>();
  static ThreadLocal<byte[][]> valuesTL = new ThreadLocal<byte[][]>();
  static ThreadLocal<long[]> mKeysTL = new ThreadLocal<long[]>();
  static ThreadLocal<long[]> mValuesTL = new ThreadLocal<long[]>();
  static ThreadLocal<long[]> mExpiresTL = new ThreadLocal<long[]>();
  
  static int maxKeySize = 32;
  static int maxValueSize = 5000;
  
  IOEngine engine;
  
  int numRecords = 10;
  int numThreads = 2;
  
  CountDownLatch cdl1;
  CountDownLatch cdl2;
  
  
  @BeforeClass
  public static void enableMallocDebug() {
    //UnsafeAccess.setMallocDebugEnabled(true);
  }
  
  @Before
  public void setUp() throws IOException {
    this.engine = getIOEngine();
  }
  
  @After
  public void tearDown() {
    // UnsafeAccess.mallocStats.printStats(false);
    this.engine.dispose();
  }
  
  protected abstract IOEngine getIOEngine() throws IOException;
  
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

    Random r = new Random(Thread.currentThread().getId() * 100000 + System.currentTimeMillis());

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
  
  protected long getExpire(int n) {
    return System.currentTimeMillis() + n * 100000;
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
  
  protected int loadBytesEngine() throws IOException {
    int count = 0;
    byte[][] keys = keysTL.get();
    byte[][] values = valuesTL.get();
    long[] expires = mExpiresTL.get();
    
    while(count < this.numRecords) {
      long expire = expires[count];
      byte[] key = keys[count];
      byte[] value = values[count];      
      boolean result = engine.put(key, value, expire);
      if (!result) {
        break;
      }
      count++;
    }    
    return count;
  }
  
  protected int deleteBytesEngine(int num) throws IOException {
    int count = 0;
    byte[][] keys = keysTL.get();
    while(count < num) {
      byte[] key = keys[count];
      boolean result = engine.delete(key, 0, key.length);
      assertTrue(result);
      count++;
    }    
    return count;
  }
  
  protected int loadMemoryEngine() throws IOException {
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
      boolean result = engine.put(keyPtr, keySize, valuePtr, valueSize, expire);
      if (!result) {
        break;
      }
      count++;
    }    
    return count;
  }
  
  protected int deleteMemoryEngine(int num) throws IOException {
    int count = 0;
    byte[][] keys = keysTL.get();
    long[] mKeys = mKeysTL.get();

    while(count < num) {
      int keySize = keys[count].length;
      long keyPtr = mKeys[count];
      boolean result = engine.delete(keyPtr, keySize);
      assertTrue(result);
      count++;
    }    
    return count;
  }
  
  protected void verifyBytesEngine(int num) throws IOException {
    int kvSize = Utils.kvSize(maxKeySize, maxValueSize);
    byte[] buffer = new byte[kvSize];
    byte[][] keys = keysTL.get();
    byte[][] values = valuesTL.get();
    
    for (int i = 0; i < num; i++) {
      byte[] key = keys[i];
      byte[] value = values[i];
      long expSize = Utils.kvSize(key.length, value.length);
      long size = engine.get(key, 0, key.length, buffer, 0);
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
  
  protected void verifyBytesEngineWithDeletes(int num, int deleted) throws IOException {
    int kvSize = Utils.kvSize(maxKeySize, maxValueSize);
    byte[] buffer = new byte[kvSize];
    byte[][] keys = keysTL.get();
    byte[][] values = valuesTL.get();
    
    for (int i = 0; i < num; i++) {
      byte[] key = keys[i];
      byte[] value = values[i];
      long expSize = Utils.kvSize(key.length, value.length);
      long size = engine.get(key, 0, key.length, buffer, 0);
      if (i < deleted) {
        assertTrue( size < 0);
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
  
  protected void verifyMemoryEngine(int num) throws IOException {
    int kvSize = Utils.kvSize(maxKeySize, maxValueSize);
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
      long size = engine.get(keyPtr, keySize, buffer);
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
  
  protected void verifyMemoryEngineWithDeletes(int num, int deleted) throws IOException {
    int kvSize = Utils.kvSize(maxKeySize, maxValueSize);
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
      long size = engine.get(keyPtr, keySize, buffer);
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
    int loaded = loadBytesEngine();
    /*DEBUG*/ System.out.println(Thread.currentThread().getName() + ": loaded=" + loaded);
    verifyBytesEngine(loaded);
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
    int loaded = loadMemoryEngine();
    /*DEBUG*/ System.out.println(Thread.currentThread().getName() + ": loaded=" + loaded);
    verifyMemoryEngine(loaded);
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
    int loaded = loadBytesEngine();
    /*DEBUG*/ System.out.println(Thread.currentThread().getName() + ": loaded=" + loaded);
    deleteBytesEngine(loaded / 2);
    verifyBytesEngineWithDeletes(loaded, loaded / 2);
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
    int loaded = loadMemoryEngine();
    /*DEBUG*/ System.out.println(Thread.currentThread().getName() + ": loaded=" + loaded);
    deleteMemoryEngine(loaded / 2);
    verifyMemoryEngineWithDeletes(loaded, loaded / 2);
    clearData();
  }
  
}
