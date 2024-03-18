package com.onecache.core.support;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.Random;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.onecache.core.support.Memcached.OpResult;
import com.onecache.core.support.Memcached.Record;
import com.onecache.core.util.TestUtils;
import com.onecache.core.util.UnsafeAccess;
import com.onecache.core.util.Utils;

public class TestMemcached {

  Memcached mc;

  @Before
  public void setUp() throws IOException {
    mc = new Memcached(TestUtils.createCache(400 << 20, 4 << 20, true, true));
  }

  @After
  public void tearDown() {
    mc.dispose();
  }

  @Test
  public void testSetGetDeleteBytes() {
    Random r = new Random();

    for (int i = 0; i < 1000; i++) {
      byte[] key = TestUtils.randomBytes(30);
      byte[] value = TestUtils.randomBytes(200);
      int flags = r.nextInt();
      long expire = (System.currentTimeMillis() + 10000)/ 1000;
      OpResult res = mc.set(key, 0, key.length, value, 0, value.length, flags, expire);
      assertTrue(res == OpResult.STORED);
      Record rec = mc.get(key, 0, key.length);
      assertTrue(rec.value != null);
      assertTrue(Utils.compareTo(value, 0, value.length, rec.value, 0, rec.size) == 0);
      assertEquals(flags, rec.flags);
      res = mc.delete(key, 0, key.length);
      assertTrue(res == OpResult.DELETED);
      rec = mc.get(key, 0, key.length);
      assertTrue(rec.value == null);
    }
  }

  @Test
  public void testSetGetDeleteMemory() {
    Random r = new Random();

    for (int i = 0; i < 1000; i++) {
      int keySize = 30;
      int valueSize = 200;
      long key = TestUtils.randomMemory(keySize);
      long value = TestUtils.randomMemory(200);
      int flags = r.nextInt();
      long expire = (System.currentTimeMillis() + 10000)/ 1000;
      OpResult res = mc.set(key, keySize, value, valueSize, flags, expire);
      assertTrue(res == OpResult.STORED);
      Record rec = mc.get(key, keySize);
      assertTrue(rec.value != null);
      assertTrue(Utils.compareTo(rec.value, 0, rec.size, value, valueSize) == 0);
      assertEquals(flags, rec.flags);
      res = mc.delete(key, keySize);
      assertTrue(res == OpResult.DELETED);
      rec = mc.get(key, keySize);
      assertTrue(rec.value == null);
      UnsafeAccess.free(key);
      UnsafeAccess.free(value);
    }
  }

  @Test
  public void testAddBytes() {
    Random r = new Random();

    for (int i = 0; i < 1000; i++) {
      byte[] key = TestUtils.randomBytes(30);
      byte[] value = TestUtils.randomBytes(200);
      int flags = r.nextInt();
      long expire = (System.currentTimeMillis() + 10000)/ 1000;
      OpResult res = mc.set(key, 0, key.length, value, 0, value.length, flags, expire);
      assertTrue(res == OpResult.STORED);
      int newFlags = r.nextInt();
      res = mc.add(key, 0, key.length, value, 0, value.length, newFlags, expire);
      assertTrue(res == OpResult.NOT_STORED);

      res = mc.delete(key, 0, key.length);
      assertTrue(res == OpResult.DELETED);

      res = mc.add(key, 0, key.length, value, 0, value.length, newFlags, expire);
      assertTrue(res == OpResult.STORED);
      Record rec = mc.get(key, 0, key.length);
      assertTrue(rec.value != null);
      assertTrue(Utils.compareTo(value, 0, value.length, rec.value, 0, rec.size) == 0);
      assertEquals(newFlags, rec.flags);

      res = mc.delete(key, 0, key.length);
      assertTrue(res == OpResult.DELETED);
    }
  }

  @Test
  public void testAddMemory() {
    Random r = new Random();

    for (int i = 0; i < 1000; i++) {
      int keySize = 30;
      int valueSize = 200;
      long key = TestUtils.randomMemory(keySize);
      long value = TestUtils.randomMemory(200);
      int flags = r.nextInt();
      long expire = (System.currentTimeMillis() + 10000)/ 1000;
      OpResult res = mc.set(key, keySize, value, valueSize, flags, expire);
      assertTrue(res == OpResult.STORED);

      int newFlags = r.nextInt();
      res = mc.add(key, keySize, value, valueSize, newFlags, expire);
      assertTrue(res == OpResult.NOT_STORED);

      res = mc.delete(key, keySize);
      assertTrue(res == OpResult.DELETED);

      res = mc.add(key, keySize, value, valueSize, newFlags, expire);
      assertTrue(res == OpResult.STORED);

      Record rec = mc.get(key, keySize);
      assertTrue(rec.value != null);
      assertTrue(Utils.compareTo(rec.value, 0, rec.size, value, valueSize) == 0);
      assertEquals(newFlags, rec.flags);

      res = mc.delete(key, keySize);
      assertTrue(res == OpResult.DELETED);
      UnsafeAccess.free(key);
      UnsafeAccess.free(value);
    }
  }

  @Test
  public void testReplaceBytes() {
    Random r = new Random();

    for (int i = 0; i < 1000; i++) {
      byte[] key = TestUtils.randomBytes(30);
      byte[] value = TestUtils.randomBytes(200);
      int flags = r.nextInt();
      long expire = (System.currentTimeMillis() + 10000)/ 1000;

      OpResult res = mc.replace(key, 0, key.length, value, 0, value.length, flags, expire);
      assertTrue(res == OpResult.NOT_STORED);

      res = mc.set(key, 0, key.length, value, 0, value.length, flags, expire);
      assertTrue(res == OpResult.STORED);
      int newFlags = r.nextInt();

      res = mc.replace(key, 0, key.length, value, 0, value.length, newFlags, expire);
      assertTrue(res == OpResult.STORED);
      Record rec = mc.get(key, 0, key.length);
      assertTrue(rec.value != null);
      assertTrue(Utils.compareTo(value, 0, value.length, rec.value, 0, rec.size) == 0);
      assertEquals(newFlags, rec.flags);

      res = mc.delete(key, 0, key.length);
      assertTrue(res == OpResult.DELETED);

    }
  }

  @Test
  public void testReplaceMemory() {
    Random r = new Random();

    for (int i = 0; i < 1000; i++) {
      int keySize = 30;
      int valueSize = 200;
      long key = TestUtils.randomMemory(keySize);
      long value = TestUtils.randomMemory(200);
      int flags = r.nextInt();
      long expire = (System.currentTimeMillis() + 10000)/ 1000;

      OpResult res = mc.replace(key, keySize, value, valueSize, flags, expire);
      assertTrue(res == OpResult.NOT_STORED);

      res = mc.set(key, keySize, value, valueSize, flags, expire);
      assertTrue(res == OpResult.STORED);

      int newFlags = r.nextInt();

      res = mc.replace(key, keySize, value, valueSize, newFlags, expire);
      assertTrue(res == OpResult.STORED);

      Record rec = mc.get(key, keySize);
      assertTrue(rec.value != null);
      assertTrue(Utils.compareTo(rec.value, 0, rec.size, value, valueSize) == 0);
      assertEquals(newFlags, rec.flags);

      res = mc.delete(key, keySize);
      assertTrue(res == OpResult.DELETED);
      UnsafeAccess.free(key);
      UnsafeAccess.free(value);
    }
  }

  @Test
  public void testAppendBytes() {
    Random r = new Random();

    for (int i = 0; i < 1000; i++) {
      byte[] key = TestUtils.randomBytes(30);
      byte[] value = TestUtils.randomBytes(200);
      byte[] value1 = TestUtils.randomBytes(100);
      byte[] combined = new byte[value.length + value1.length];
      System.arraycopy(value, 0, combined, 0, value.length);
      System.arraycopy(value1, 0, combined, value.length, value1.length);

      int flags = r.nextInt();
      long expire = (System.currentTimeMillis() + 10000)/ 1000;

      OpResult res = mc.append(key, 0, key.length, value1, 0, value1.length, flags, expire);
      assertTrue(res == OpResult.NOT_STORED);

      res = mc.set(key, 0, key.length, value, 0, value.length, flags, expire);
      assertTrue(res == OpResult.STORED);
      int newFlags = r.nextInt();

      res = mc.append(key, 0, key.length, value1, 0, value1.length, newFlags, expire);
      assertTrue(res == OpResult.STORED);
      Record rec = mc.get(key, 0, key.length);
      assertTrue(rec.value != null);
      assertTrue(Utils.compareTo(combined, 0, combined.length, rec.value, 0, rec.size) == 0);
      assertEquals(newFlags, rec.flags);

      res = mc.delete(key, 0, key.length);
      assertTrue(res == OpResult.DELETED);
    }
  }

  @Test
  public void testAppendMemory() {
    Random r = new Random();

    for (int i = 0; i < 1000; i++) {
      int keySize = 30;
      int valueSize = 200;
      int value1Size = 100;
      long key = TestUtils.randomMemory(keySize);
      long value = TestUtils.randomMemory(valueSize);
      long value1 = TestUtils.randomMemory(value1Size);
      long combined = UnsafeAccess.malloc(valueSize + value1Size);

      UnsafeAccess.copy(value, combined, valueSize);
      UnsafeAccess.copy(value1, combined + valueSize, value1Size);

      int flags = r.nextInt();
      long expire = (System.currentTimeMillis() + 10000)/ 1000;

      OpResult res = mc.append(key, keySize, value1, value1Size, flags, expire);
      assertTrue(res == OpResult.NOT_STORED);

      res = mc.set(key, keySize, value, valueSize, flags, expire);
      assertTrue(res == OpResult.STORED);
      int newFlags = r.nextInt();

      res = mc.append(key, keySize, value1, value1Size, newFlags, expire);
      assertTrue(res == OpResult.STORED);
      Record rec = mc.get(key, keySize);
      assertTrue(rec.value != null);
      assertTrue(Utils.compareTo(rec.value, 0, rec.size, combined, valueSize + value1Size) == 0);
      assertEquals(newFlags, rec.flags);

      res = mc.delete(key, keySize);
      assertTrue(res == OpResult.DELETED);
      UnsafeAccess.free(key);
      UnsafeAccess.free(value);
      UnsafeAccess.free(value1);
    }
  }

  @Test
  public void testPrependBytes() {
    Random r = new Random();

    for (int i = 0; i < 1000; i++) {
      byte[] key = TestUtils.randomBytes(30);
      byte[] value = TestUtils.randomBytes(200);
      byte[] value1 = TestUtils.randomBytes(100);
      byte[] combined = new byte[value.length + value1.length];
      System.arraycopy(value1, 0, combined, 0, value1.length);
      System.arraycopy(value, 0, combined, value1.length, value.length);
      int flags = r.nextInt();
      long expire = (System.currentTimeMillis() + 10000)/ 1000;

      OpResult res = mc.prepend(key, 0, key.length, value1, 0, value1.length, flags, expire);
      assertTrue(res == OpResult.NOT_STORED);

      res = mc.set(key, 0, key.length, value, 0, value.length, flags, expire);
      assertTrue(res == OpResult.STORED);
      int newFlags = r.nextInt();
      res = mc.prepend(key, 0, key.length, value1, 0, value1.length, newFlags, expire);
      assertTrue(res == OpResult.STORED);
      Record rec = mc.get(key, 0, key.length);
      assertTrue(rec.value != null);
      assertTrue(Utils.compareTo(combined, 0, combined.length, rec.value, 0, rec.size) == 0);
      assertEquals(newFlags, rec.flags);

      res = mc.delete(key, 0, key.length);
      assertTrue(res == OpResult.DELETED);
    }
  }

  @Test
  public void testPrependMemory() {
    Random r = new Random();

    for (int i = 0; i < 1000; i++) {
      int keySize = 30;
      int valueSize = 200;
      int value1Size = 100;
      long key = TestUtils.randomMemory(keySize);
      long value = TestUtils.randomMemory(valueSize);
      long value1 = TestUtils.randomMemory(value1Size);
      long combined = UnsafeAccess.malloc(valueSize + value1Size);
      UnsafeAccess.copy(value1, combined, value1Size);
      UnsafeAccess.copy(value, combined + value1Size, valueSize);

      int flags = r.nextInt();
      long expire = (System.currentTimeMillis() + 10000)/ 1000;

      OpResult res = mc.prepend(key, keySize, value1, value1Size, flags, expire);
      assertTrue(res == OpResult.NOT_STORED);

      res = mc.set(key, keySize, value, valueSize, flags, expire);
      assertTrue(res == OpResult.STORED);
      int newFlags = r.nextInt();

      res = mc.prepend(key, keySize, value1, value1Size, newFlags, expire);
      assertTrue(res == OpResult.STORED);
      Record rec = mc.get(key, keySize);
      assertTrue(rec.value != null);
      assertTrue(Utils.compareTo(rec.value, 0, rec.size, combined, valueSize + value1Size) == 0);
      assertEquals(newFlags, rec.flags);
      res = mc.delete(key, keySize);
      assertTrue(res == OpResult.DELETED);
      UnsafeAccess.free(key);
      UnsafeAccess.free(value);
      UnsafeAccess.free(value1);
    }
  }
  
  @Test
  public void testCASBytes() {
    Random r = new Random();

    for (int i = 0; i < 1000; i++) {
      byte[] key = TestUtils.randomBytes(30);
      byte[] value = TestUtils.randomBytes(200);
      byte[] value1 = TestUtils.randomBytes(100);
      int flags = r.nextInt();
      long expire = (System.currentTimeMillis() + 10000)/ 1000;

      OpResult res = mc.cas(key, 0, key.length, value1, 0, value1.length, flags, expire, 0);
      assertTrue(res == OpResult.NOT_FOUND);

      res = mc.set(key, 0, key.length, value, 0, value.length, flags, expire);
      assertTrue(res == OpResult.STORED);
      int newFlags = r.nextInt();
      res = mc.cas(key, 0, key.length, value1, 0, value1.length, newFlags, expire, 0);
      assertTrue(res == OpResult.EXISTS);
      Record rec = mc.gets(key, 0, key.length);
      assertTrue(rec.value != null);
      assertTrue(Utils.compareTo(value, 0, value.length, rec.value, 0, rec.size) == 0);
      assertEquals(flags, rec.flags);

      long cas = rec.cas;
      
      res = mc.cas(key, 0, key.length, value1, 0, value1.length, newFlags, expire, cas);
      assertTrue(res == OpResult.STORED);
      rec = mc.gets(key, 0, key.length);
      assertTrue(rec.value != null);
      assertTrue(Utils.compareTo(value1, 0, value1.length, rec.value, 0, rec.size) == 0);
      assertEquals(newFlags, rec.flags);
      
      res = mc.delete(key, 0, key.length);
      assertTrue(res == OpResult.DELETED);
    }
  }

  @Test
  public void testCASMemory() {
    Random r = new Random();

    for (int i = 0; i < 1000; i++) {
      int keySize = 30;
      int valueSize = 200;
      int value1Size = 100;
      long key = TestUtils.randomMemory(keySize);
      long value = TestUtils.randomMemory(valueSize);
      long value1 = TestUtils.randomMemory(value1Size);

      int flags = r.nextInt();
      long expire = (System.currentTimeMillis() + 10000)/ 1000;

      OpResult res = mc.cas(key, keySize, value1, value1Size, flags, expire, 0);
      assertTrue(res == OpResult.NOT_FOUND);

      res = mc.set(key, keySize, value, valueSize, flags, expire);
      assertTrue(res == OpResult.STORED);
      int newFlags = r.nextInt();

      res = mc.cas(key, keySize, value1, value1Size, newFlags, expire, 0);
      assertTrue(res == OpResult.EXISTS);
      Record rec = mc.gets(key, keySize);
      assertTrue(rec.value != null);
      assertTrue(Utils.compareTo(rec.value, 0, rec.size, value, valueSize) == 0);
      assertEquals(flags, rec.flags);
      
      long cas = rec.cas;
      res = mc.cas(key, keySize, value1, value1Size, newFlags, expire, cas);
      assertTrue(res == OpResult.STORED);
      rec = mc.get(key, keySize);
      assertTrue(rec.value != null);
      assertTrue(Utils.compareTo(rec.value, 0, rec.size, value1, value1Size) == 0);
      assertEquals(newFlags, rec.flags);
      
      res = mc.delete(key, keySize);
      assertTrue(res == OpResult.DELETED);
      UnsafeAccess.free(key);
      UnsafeAccess.free(value);
      UnsafeAccess.free(value1);
    }
  }
  
  @Test
  public void testIncrementMemory() {
    Random r = new Random();

    for (int i = 0; i < 1000; i++) {
      int keySize = 30;
      int value1Size = 100;
      long key = TestUtils.randomMemory(keySize);
      long v0 = Math.abs(r.nextLong() / 2);
      byte[] value = Long.toString(v0).getBytes();
      long vptr = UnsafeAccess.malloc(value.length);
      UnsafeAccess.copy(value, 0, vptr, value.length);
      
      long value1 = TestUtils.randomMemory(value1Size);
      int flags = r.nextInt();
      long expire = (System.currentTimeMillis() + 1000000)/ 1000;

      long v = mc.incr(key, keySize, 10);
      assertTrue(v == -1);

      OpResult res = mc.set(key, keySize, vptr, value.length, flags, expire);
      assertTrue(res == OpResult.STORED);

      try {
        v = mc.incr(key, keySize, -v0);
        fail();
      } catch(IllegalArgumentException e) {
        
      }
      
      v = mc.incr(key, keySize, 10000);
      assertEquals(v0 + 10000, v);
      
      v = mc.incr(key, keySize, 10000);
      assertEquals(v0 + 20000, v);
      
      Record rec = mc.get(key, keySize);
      assertTrue(rec.value != null);
      
      String s = new String(rec.value, rec.offset, rec.size);
      assertEquals(v, Long.parseLong(s));
      
      res = mc.set(key, keySize, value1, value1Size, flags, expire);
      assertTrue(res == OpResult.STORED);
      
      try {
        v = mc.incr(key, keySize, 10000);
        fail();
      } catch (NumberFormatException e) {     
      }
      
      res = mc.delete(key, keySize);
      assertTrue(res == OpResult.DELETED);
      UnsafeAccess.free(key);
      UnsafeAccess.free(vptr);
      UnsafeAccess.free(value1);
    }
  }

  @Test
  public void testIncrementBytes() {
    Random r = new Random();

    for (int i = 0; i < 1000; i++) {
      byte[] key = TestUtils.randomBytes(30);
      long v0 = Math.abs(r.nextLong() / 2);
      byte[] value = Long.toString(v0).getBytes();
      byte[] value1 = TestUtils.randomBytes(100);
      int flags = r.nextInt();
      long expire = (System.currentTimeMillis() + 10000)/ 1000;

      long v = mc.incr(key, 0, key.length, 10);
      assertTrue(v == -1);

      OpResult res = mc.set(key, 0, key.length, value, 0, value.length, flags, expire);
      assertTrue(res == OpResult.STORED);

      try {
        v = mc.incr(key, 0, key.length, -v0);
        fail();
      } catch(IllegalArgumentException e) {
        
      }
      
      v = mc.incr(key, 0, key.length, 10000);
      assertEquals(v0 + 10000, v);
      
      v = mc.incr(key, 0, key.length, 10000);
      assertEquals(v0 + 20000, v);
      
      Record rec = mc.get(key,  0, key.length);
      assertTrue(rec.value != null);
      
      String s = new String(rec.value, rec.offset, rec.size);
      assertEquals(v, Long.parseLong(s));
      
      res = mc.set(key, 0, key.length, value1, 0, value1.length, flags, expire);
      assertTrue(res == OpResult.STORED);
      
      try {
        v = mc.incr(key, 0, key.length, 10000);
        fail();
      } catch (NumberFormatException e) {     
      }
      
      res = mc.delete(key, 0, key.length);
      assertTrue(res == OpResult.DELETED);
    }
  }
  
  @Test
  public void testDecrementMemory() {
    Random r = new Random();

    for (int i = 0; i < 1000; i++) {
      int keySize = 30;
      int value1Size = 100;
      long key = TestUtils.randomMemory(keySize);
      long v0 = Math.abs(r.nextLong() / 2);
      byte[] value = Long.toString(v0).getBytes();
      long vptr = UnsafeAccess.malloc(value.length);
      UnsafeAccess.copy(value, 0, vptr, value.length);
      
      long value1 = TestUtils.randomMemory(value1Size);
      int flags = r.nextInt();
      long expire = (System.currentTimeMillis() + 10000)/ 1000;

      long v = mc.decr(key, keySize, 10);
      assertTrue(v == -1);

      OpResult res = mc.set(key, keySize, vptr, value.length, flags, expire);
      assertTrue(res == OpResult.STORED);

      try {
        v = mc.decr(key, keySize, -v0);
        fail();
      } catch(IllegalArgumentException e) {
        
      }
      
      v = mc.decr(key, keySize, 10000);
      assertEquals(Math.max(v0 - 10000, 0), v);
      
      v = mc.decr(key, keySize, 10000);
      assertEquals(Math.max(v0 - 20000, 0), v);
      
      v = mc.decr(key, keySize, v0);
      assertEquals(0, v);
      
      Record rec = mc.get(key, keySize);
      assertTrue(rec.value != null);
      
      String s = new String(rec.value, rec.offset, rec.size);
      assertEquals(v, Long.parseLong(s));
      
      res = mc.set(key, keySize, value1, value1Size, flags, expire);
      assertTrue(res == OpResult.STORED);
      
      try {
        v = mc.decr(key, keySize, 10000);
        fail();
      } catch (NumberFormatException e) {     
      }
      
      res = mc.delete(key, keySize);
      assertTrue(res == OpResult.DELETED);
      UnsafeAccess.free(key);
      UnsafeAccess.free(vptr);
      UnsafeAccess.free(value1);
    }
  }

  @Test
  public void testDecrementBytes() {
    Random r = new Random();

    for (int i = 0; i < 1000; i++) {
      byte[] key = TestUtils.randomBytes(30);
      long v0 = Math.abs(r.nextLong() / 2);
      byte[] value = Long.toString(v0).getBytes();
      byte[] value1 = TestUtils.randomBytes(100);
      int flags = r.nextInt();
      long expire = (System.currentTimeMillis() + 10000)/ 1000;

      long v = mc.decr(key, 0, key.length, 10);
      assertTrue(v == -1);

      OpResult res = mc.set(key, 0, key.length, value, 0, value.length, flags, expire);
      assertTrue(res == OpResult.STORED);

      try {
        v = mc.decr(key, 0, key.length, -v0);
        fail();
      } catch(IllegalArgumentException e) {
        
      }
      
      v = mc.decr(key, 0, key.length, 10000);
      assertEquals(Math.max(v0 - 10000, 0), v);
      
      v = mc.decr(key, 0, key.length, 10000);
      assertEquals(Math.max(v0 - 20000, 0), v);
      
      Record rec = mc.get(key,  0, key.length);
      assertTrue(rec.value != null);
      
      String s = new String(rec.value, rec.offset, rec.size);
      assertEquals(v, Long.parseLong(s));
      
      res = mc.set(key, 0, key.length, value1, 0, value1.length, flags, expire);
      assertTrue(res == OpResult.STORED);
      
      try {
        v = mc.decr(key, 0, key.length, 10000);
        fail();
      } catch (NumberFormatException e) {     
      }
      
      res = mc.delete(key, 0, key.length);
      assertTrue(res == OpResult.DELETED);
    }
  }
  
  @Test
  public void testGetsBytes() {
    Random r = new Random();

    for (int i = 0; i < 1000; i++) {
      byte[] key = TestUtils.randomBytes(30);
      byte[] value = TestUtils.randomBytes(200);
      int flags = r.nextInt();
      long expire = (System.currentTimeMillis() + 10000)/ 1000;
      Record rec = mc.gets(key, 0, key.length);
      assertTrue(rec.value == null);
      
      OpResult res = mc.set(key, 0, key.length, value, 0, value.length, flags, expire);
      assertTrue(res == OpResult.STORED);
      
      rec = mc.gets(key, 0, key.length);
      assertTrue(rec.value != null);
      assertTrue(Utils.compareTo(value, 0, value.length, rec.value, 0, rec.size) == 0);
      assertEquals(flags, rec.flags);
      long cas = mc.computeCAS(value, 0, value.length);
      assertEquals(cas, rec.cas);
      
      res = mc.delete(key, 0, key.length);
      assertTrue(res == OpResult.DELETED);
      
      rec = mc.get(key, 0, key.length);
      assertTrue(rec.value == null);
    }
  }

  @Test
  public void testGetsMemory() {
    Random r = new Random();

    for (int i = 0; i < 1000; i++) {
      int keySize = 30;
      int valueSize = 200;
      long key = TestUtils.randomMemory(keySize);
      long value = TestUtils.randomMemory(200);
      int flags = r.nextInt();
      long expire = (System.currentTimeMillis() + 10000)/ 1000;
      
      Record rec = mc.gets(key, keySize);
      assertTrue(rec.value == null);
      
      OpResult res = mc.set(key, keySize, value, valueSize, flags, expire);
      assertTrue(res == OpResult.STORED);
      
      rec = mc.gets(key, keySize);
      assertTrue(rec.value != null);
      assertTrue(Utils.compareTo(rec.value, 0, rec.size, value, valueSize) == 0);
      assertEquals(flags, rec.flags);
      long cas = mc.computeCAS(value, valueSize);
      assertEquals(cas, rec.cas);
      
      res = mc.delete(key, keySize);
      assertTrue(res == OpResult.DELETED);
      
      rec = mc.get(key, keySize);
      assertTrue(rec.value == null);
      UnsafeAccess.free(key);
      UnsafeAccess.free(value);
    }
  }
  
  @Test
  public void testGatsBytes() {
    Random r = new Random();

    for (int i = 0; i < 1000; i++) {
      
      byte[] key = TestUtils.randomBytes(30);
      byte[] value = TestUtils.randomBytes(200);
      int flags = r.nextInt();
      long expire = (System.currentTimeMillis() + 10000)/ 1000;
      long newExpire = expire + 10;
      
      Record rec = mc.gats(key, 0, key.length, newExpire);
      assertTrue(rec.value == null);
      
      OpResult res = mc.set(key, 0, key.length, value, 0, value.length, flags, expire);
      assertTrue(res == OpResult.STORED);
      
      rec = mc.gats(key, 0, key.length, newExpire);
      assertTrue(rec.value != null);
      assertTrue(Utils.compareTo(value, 0, value.length, rec.value, 0, rec.size) == 0);
      assertEquals(flags, rec.flags);
      long cas = mc.computeCAS(value, 0, value.length);
      assertEquals(cas, rec.cas);
      assertTrue(sameExpire(expire, rec.expire));
      
      rec = mc.gats(key, 0, key.length, newExpire);
      assertTrue(sameExpire(newExpire, rec.expire));

      res = mc.delete(key, 0, key.length);
      assertTrue(res == OpResult.DELETED);
      
      rec = mc.get(key, 0, key.length);
      assertTrue(rec.value == null);
    }
  }

  @Test
  public void testGatsMemory() {
    Random r = new Random();

    for (int i = 0; i < 1000; i++) {
      int keySize = 30;
      int valueSize = 200;
      long key = TestUtils.randomMemory(keySize);
      long value = TestUtils.randomMemory(200);
      int flags = r.nextInt();
      long expire = (System.currentTimeMillis() + 10000)/ 1000;
      long newExpire = expire + 10;
      
      Record rec = mc.gats(key, keySize, newExpire);
      assertTrue(rec.value == null);
      
      OpResult res = mc.set(key, keySize, value, valueSize, flags, expire);
      assertTrue(res == OpResult.STORED);
      
      rec = mc.gats(key, keySize, newExpire);
      assertTrue(rec.value != null);
      assertTrue(Utils.compareTo(rec.value, 0, rec.size, value, valueSize) == 0);
      assertEquals(flags, rec.flags);
      long cas = mc.computeCAS(value, valueSize);
      
      assertEquals(cas, rec.cas);
      assertTrue(sameExpire(expire, rec.expire));
      rec = mc.gats(key, keySize, newExpire);
      assertTrue(sameExpire(newExpire, rec.expire));

      res = mc.delete(key, keySize);
      assertTrue(res == OpResult.DELETED);
      
      rec = mc.get(key, keySize);
      assertTrue(rec.value == null);
      UnsafeAccess.free(key);
      UnsafeAccess.free(value);
    }
  }
  
  @Test
  public void testGatBytes() {
    Random r = new Random();

    for (int i = 0; i < 1000; i++) {
      
      byte[] key = TestUtils.randomBytes(30);
      byte[] value = TestUtils.randomBytes(200);
      int flags = r.nextInt();
      long expire = (System.currentTimeMillis() + 10000)/ 1000;
      long newExpire = expire + 10;
      OpResult res = mc.set(key, 0, key.length, value, 0, value.length, flags, expire);
      assertTrue(res == OpResult.STORED);
      
      Record rec = mc.gat(key, 0, key.length, newExpire);
      assertTrue(rec.value != null);
      assertTrue(Utils.compareTo(value, 0, value.length, rec.value, 0, rec.size) == 0);
      assertEquals(flags, rec.flags);
      assertTrue(sameExpire(expire, rec.expire));
      
      rec = mc.gat(key, 0, key.length, newExpire);
      assertTrue(sameExpire(newExpire, rec.expire));

      res = mc.delete(key, 0, key.length);
      assertTrue(res == OpResult.DELETED);
      rec = mc.get(key, 0, key.length);
      assertTrue(rec.value == null);
    }
  }

  @Test
  public void testGatMemory() {
    Random r = new Random();

    for (int i = 0; i < 1000; i++) {
      int keySize = 30;
      int valueSize = 200;
      long key = TestUtils.randomMemory(keySize);
      long value = TestUtils.randomMemory(200);
      int flags = r.nextInt();
      long expire = (System.currentTimeMillis() + 10000)/ 1000;
      long newExpire = expire + 10;
      
      Record rec = mc.gat(key, keySize, newExpire);
      assertTrue(rec.value == null);
      
      OpResult res = mc.set(key, keySize, value, valueSize, flags, expire);
      assertTrue(res == OpResult.STORED);
      rec = mc.gat(key, keySize, newExpire);
      assertTrue(rec.value != null);
      assertTrue(Utils.compareTo(rec.value, 0, rec.size, value, valueSize) == 0);
      assertEquals(flags, rec.flags);
      assertTrue(sameExpire(expire, rec.expire));
      
      rec = mc.gat(key, keySize, newExpire);
      assertTrue(sameExpire(newExpire, rec.expire));

      res = mc.delete(key, keySize);
      assertTrue(res == OpResult.DELETED);
      
      rec = mc.get(key, keySize);
      assertTrue(rec.value == null);
      UnsafeAccess.free(key);
      UnsafeAccess.free(value);
    }
  }
  
  @Test
  public void testTouchBytes() {
    Random r = new Random();

    for (int i = 0; i < 1000; i++) {
      
      byte[] key = TestUtils.randomBytes(30);
      byte[] value = TestUtils.randomBytes(200);
      int flags = r.nextInt();
      long expire = (System.currentTimeMillis() + 10000)/ 1000;
      long newExpire = expire + 10;
      
      long result = mc.touch(key, 0, key.length, newExpire);
      assertTrue(result == -1);
      
      OpResult res = mc.set(key, 0, key.length, value, 0, value.length, flags, expire);
      assertTrue(res == OpResult.STORED);
      
      result = mc.touch(key, 0, key.length, newExpire);
      assertTrue(sameExpire(expire, result));
      
      result = mc.touch(key, 0, key.length, newExpire);
      assertTrue(sameExpire(newExpire, result));
      
      res = mc.delete(key, 0, key.length);
      assertTrue(res == OpResult.DELETED);
      
      Record rec = mc.get(key, 0, key.length);
      assertTrue(rec.value == null);
    }
  }

  @Test
  public void testTouchMemory() {
    Random r = new Random();

    for (int i = 0; i < 1000; i++) {
      int keySize = 30;
      int valueSize = 200;
      long key = TestUtils.randomMemory(keySize);
      long value = TestUtils.randomMemory(200);
      int flags = r.nextInt();
      long expire = (System.currentTimeMillis() + 10000)/ 1000;
      long newExpire = expire + 10;
      
      long result = mc.touch(key, keySize, newExpire);
      assertTrue(result == -1);
      
      OpResult res = mc.set(key, keySize, value, valueSize, flags, expire);
      assertTrue(res == OpResult.STORED);
  
      result = mc.touch(key, keySize, newExpire);
      assertTrue(sameExpire(expire, result));

      res = mc.delete(key, keySize);
      assertTrue(res == OpResult.DELETED);
      
      Record rec = mc.get(key, keySize);
      assertTrue(rec.value == null);
      UnsafeAccess.free(key);
      UnsafeAccess.free(value);
    }
  }
  
  protected boolean sameExpire (long exp, long value) {
    return Math.abs(exp - value) <= 1;
  }
}
