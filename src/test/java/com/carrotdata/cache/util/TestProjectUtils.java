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

package com.carrotdata.cache.util;

import static org.junit.Assert.assertEquals;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

import org.apache.commons.math3.distribution.ZipfDistribution;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotdata.cache.expire.ExpireSupport;

public class TestProjectUtils {
  private static final Logger LOG = LoggerFactory.getLogger(TestProjectUtils.class);

  class TestClass {
    List<String> list;

    TestClass(List<String> param) {
      this.list = param;
      LOG.info("OK");
    }
  }

  @Test
  public void testReflection() {
    List<String> list = new ArrayList<>();
    list.add("test");

    try {
      Constructor<?> cstr =
          TestProjectUtils.TestClass.class.getDeclaredConstructor(this.getClass(), List.class);

      cstr.setAccessible(true);
      cstr.newInstance(this, list);
    } catch (SecurityException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (InstantiationException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (IllegalAccessException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (IllegalArgumentException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (InvocationTargetException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    } catch (NoSuchMethodException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }

    LOG.info("{}", list.getClass());
  }

  @Test
  public void testObjectArray()
      throws SecurityException, IllegalAccessException, IllegalArgumentException,
      InvocationTargetException, NoSuchFieldException, ClassNotFoundException {

    String[] arr = new String[] { "1", "2", "3" };

    Class<?> cls = arr.getClass();
    LOG.info("is array=" + cls.isArray());
    Object[] f = (Object[]) arr;
    LOG.info("arr length=" + f.length);
    String className = cls.getName();
    Class<?> fromName = Class.forName(className);

    LOG.info("class=" + cls + " from name =" + fromName);

    cls = int[].class;
    className = cls.getName();
    fromName = Class.forName(className);
    LOG.info("class=" + cls + " from name =" + fromName);

  }

  @Test
  public void testCollection() {
    List<String> l = new ArrayList<String>();
    l.add("s1");
    l.add("s2");
    l.add("s3");

    Collection<?> c = (Collection<?>) l;
    LOG.info(
      "list size =" + c.size() + "list instance of Collection=" + (l instanceof Collection<?>));
    HashMap<String, String> map = new HashMap<>();
    LOG.info("map instanceof Collection=" + (map instanceof Collection<?>));
    Iterator<?> it = c.iterator();
    LOG.info("it=" + it);

  }

  @Test
  public void testCopyBuffer() {

    byte[] buf = TestUtils.randomBytes(1000000 + 30);
    long start = System.nanoTime();
    for (int i = 0; i < 1000; i++) {
      System.arraycopy(buf, 30, buf, 0, 1000000);
    }
    long end = System.nanoTime();

    LOG.info("time={}", (end - start) / 1000);
  }

  /**
   * Compare hash64 for byte arrays and direct memory version
   */
  @Test
  public void testHash64() {
    int keySize = 33;
    int n = 1000;
    byte[][] keys = new byte[n][];
    long[] mKeys = new long[n];

    for (int i = 0; i < keys.length; i++) {
      keys[i] = TestUtils.randomBytes(keySize);
      mKeys[i] = UnsafeAccess.malloc(keySize);
      UnsafeAccess.copy(keys[i], 0, mKeys[i], keySize);
      long hash1 = Utils.hash64(keys[i], 0, keySize);
      long hash2 = Utils.hash64(mKeys[i], keySize);
      assertEquals(hash1, hash2);
    }
  }

  @Test
  public void testValue15() {
    int vi = 0xffff;
    int vs = (short) vi;
    int vvs = (short) (0x7fff & vs);
    LOG.info(" {} {} {}", vi, vs, vvs);
  }

  @Test
  public void testValue14() {
    int vi = 0xffff;
    int vs = (short) vi;
    int vvs = (short) (0x3fff & vs);
    LOG.info(" {} {} {}", vi, vs, vvs);
  }

  @Test
  public void testValue1() {
    int v1 = 0xffff;
    int v2 = 0x3fff;
    short s1 = (short) v1;
    short s2 = (short) v2;
    short ss1 = (short) ((s1 >> 15) & 1);
    short ss2 = (short) ((s2 >> 15) & 1);

    LOG.info(" {} {} {} {} {} {}", v1, v2, s1, s2, ss1, ss2);

  }

  @Test
  public void testValue2() {
    int v1 = 0xffff;
    int v2 = 0xbfff;
    short s1 = (short) v1;
    short s2 = (short) v2;
    short ss1 = (short) ((s1 >> 14) & 3);
    short ss2 = (short) ((s2 >> 14) & 3);

    LOG.info(" {} {} {} {} {} {}", v1, v2, s1, s2, ss1, ss2);

  }

  @Test
  public void testAllConversions() {
    short v1 = 10000;
    short v2 = 9999;
    short v3 = 15197;

    // Seconds
    short v = sec1(v1);
    assertEquals(v1, low15(v));
    assertEquals(ExpireSupport.TIME_IN_SECONDS, (int) high1(v1));
    v = sec1(v2);
    assertEquals(v2, low15(v));
    assertEquals(ExpireSupport.TIME_IN_SECONDS, (int) high1(v));
    v = sec1(v3);
    assertEquals(v3, low15(v));
    assertEquals(ExpireSupport.TIME_IN_SECONDS, (int) high1(v));

    v = sec2(v1);
    assertEquals(v1, low14(v));
    assertEquals(ExpireSupport.TIME_IN_SECONDS, (int) high2(v));

    v = sec2(v2);
    assertEquals(v2, low14(v));
    assertEquals(ExpireSupport.TIME_IN_SECONDS, (int) high2(v));

    v = sec2(v3);
    assertEquals(v3, low14(v));
    assertEquals(ExpireSupport.TIME_IN_SECONDS, (int) high2(v));
    // Minutes
    v = min1(v1);
    assertEquals(v1, low15(v));
    assertEquals(ExpireSupport.TIME_IN_MINUTES, (int) high1(v));

    v = min1(v2);
    assertEquals(v2, low15(v));
    assertEquals(ExpireSupport.TIME_IN_MINUTES, (int) high1(v));

    v = min1(v3);
    assertEquals(v3, low15(v));
    assertEquals(ExpireSupport.TIME_IN_MINUTES, (int) high1(v));

    v = min2(v1);
    assertEquals(v1, low14(v));
    assertEquals(ExpireSupport.TIME_IN_MINUTES, (int) high2(v));

    v = min2(v2);
    assertEquals(v2, low14(v));
    assertEquals(ExpireSupport.TIME_IN_MINUTES, (int) high2(v));

    v = min2(v3);
    assertEquals(v3, low14(v));
    assertEquals(ExpireSupport.TIME_IN_MINUTES, (int) high2(v));

    v = hours2(v1);
    assertEquals(v1, low14(v));
    assertEquals(ExpireSupport.TIME_IN_HOURS, (int) high2(v));

    v = hours2(v2);
    assertEquals(v2, low14(v));
    assertEquals(ExpireSupport.TIME_IN_HOURS, (int) high2(v));

    v = hours2(v3);
    assertEquals(v3, low14(v));
    assertEquals(ExpireSupport.TIME_IN_HOURS, (int) high2(v));

    int vv = 64000;
    short ss = (short) vv;

    int vvv = ss & 0xffff;

    assertEquals(vv, vvv);
  }

  @Test
  public void testZipfianDistribution() {
    LOG.info("Zipf distribution");
    ZipfDistribution zd = new ZipfDistribution(1000000, 0.9);

    for (int i = 0; i < 100; i++) {
      LOG.info("{}", zd.sample());
    }
  }

  @Test
  public void testStrNumberConversionsPositive() {
    Random r = new Random();

    byte[] buf = new byte[20];
    for (int i = 0; i < 1000; i++) {
      long v = r.nextLong();
      v = Math.abs(v);
      String s = Long.toString(v);
      byte[] b = s.getBytes();
      long vv = Utils.strToLong(b, 0, b.length);
      assertEquals(v, vv);
      int numDigits = Utils.longToStr(buf, 0, v);
      assertEquals(b.length, numDigits);
      String ss = new String(buf, 0, numDigits);
      long vvv = Long.parseLong(ss);
      assertEquals(v, vvv);
    }
  }

  @Test
  public void testStrNumberConversionsNegative() {
    Random r = new Random();

    byte[] buf = new byte[21];
    for (int i = 0; i < 1000; i++) {
      long v = r.nextLong();
      v = -Math.abs(v);
      String s = Long.toString(v);
      byte[] b = s.getBytes();
      long vv = Utils.strToLong(b, 0, b.length);
      assertEquals(v, vv);
      int numDigits = Utils.longToStr(buf, 0, v);
      assertEquals(b.length, numDigits);
      String ss = new String(buf, 0, numDigits);
      long vvv = Long.parseLong(ss);
      assertEquals(v, vvv);
    }
  }

  @Test
  public void testStrNumberConversionsPositiveDirect() {
    Random r = new Random();

    long buf = UnsafeAccess.mallocZeroed(20);
    for (int i = 0; i < 1000; i++) {
      long v = r.nextLong();
      v = Math.abs(v);
      String s = Long.toString(v);
      int len = s.length();
      int numDigits = Utils.longToStrDirect(buf, 20, v);
      assertEquals(len, numDigits);
      long vv = Utils.strToLongDirect(buf, len);
      assertEquals(v, vv);
    }
  }

  @Test
  public void testStrNumberConversionsNegativeDirect() {
    Random r = new Random();

    long buf = UnsafeAccess.mallocZeroed(21);
    for (int i = 0; i < 1000; i++) {
      long v = r.nextLong();
      v = -Math.abs(v);
      String s = Long.toString(v);
      int len = s.length();
      int numDigits = Utils.longToStrDirect(buf, 21, v);
      assertEquals(len, numDigits);
      long vv = Utils.strToLongDirect(buf, len);
      assertEquals(v, vv);
    }
  }

  @Test
  public void testUnsignedInt() {
    long v = 0xffffffffL;
    int vv = (int) v;
    long vvv = Integer.toUnsignedLong(vv);
    assertEquals(v, vvv);
  }

  @Test
  public void testNextPowerOf2() {
    long v = 0;
    assertEquals(0, Utils.nextPow2(v));
    Random r = new Random();
    for (int i = 2; i < 63; i++) {
      long result = 1L << i;
      for (int j = 0; j < 100; j++) {
        v = Math.abs(r.nextLong());
        v = v % (result - (result >>> 1));
        if (v == 0) v++;
        v = (result >>> 1) + v;
        assertEquals(result, Utils.nextPow2(v));
      }
    }

  }

  @Test
  public void testNumericConversion() {
    long v = -100000L;
    int vv = (int) v;
    assertEquals(-100000, vv);
  }

  @Test
  public void testEndiness() {
    long ptr = UnsafeAccess.malloc(8);
    UnsafeAccess.theUnsafe.putInt(ptr, 100);
    int v = UnsafeAccess.theUnsafe.getInt(ptr);
    assertEquals(100, v);
  }

  /**
   * Utility methods
   */
  private short low15(short v) {
    return (short) (v & 0x7fff);
  }

  private short low14(short v) {
    return (short) (v & 0x3fff);
  }

  private short high1(short v) {
    return (short) ((v >> 15) & 1);
  }

  public short high2(short v) {
    return (short) ((v >> 14) & 3);
  }

  public short sec1(short v) {
    return v;
  }

  public short sec2(short v) {
    return v;
  }

  public short min1(short v) {
    return (short) (v | 0x8000);
  }

  public short min2(short v) {
    return (short) (v | 0x4000);
  }

  public short hours2(short v) {
    return (short) (v | 0xc000);
  }

}