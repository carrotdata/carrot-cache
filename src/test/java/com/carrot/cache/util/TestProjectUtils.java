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

package com.carrot.cache.util;

import static org.junit.Assert.assertEquals;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.math3.distribution.ZipfDistribution;
import org.junit.Test;

import com.carrot.cache.expire.ExpireSupport;

public class TestProjectUtils {

  @Test
  public void testObjectArray() throws SecurityException, IllegalAccessException, IllegalArgumentException, 
  InvocationTargetException, NoSuchFieldException, ClassNotFoundException {
    
    String[] arr = new String[] {"1", "2", "3"};
    
    Class<?> cls = arr.getClass();
    System.out.println("is array=" + cls.isArray());
    Object[] f = (Object[]) arr;
    System.out.println("arr length=" + f.length);
    String className = cls.getName();
    Class<?> fromName = Class.forName(className);
    
    System.out.println("class=" +cls + " from name ="+ fromName);
    
    cls = int[].class;
    className = cls.getName();
    fromName = Class.forName(className);
    System.out.println("class=" +cls + " from name ="+ fromName);

  }
  
  @Test
  public void testCollection() {
    List<String> l = new ArrayList<String>();
    l.add("s1");
    l.add("s2");
    l.add("s3");
    
    
    Collection<?> c = (Collection<?>) l; 
    System.out.println("list size =" + c.size() + "list instance of Collection=" +
        (l instanceof Collection<?>));
    HashMap<String, String> map = new HashMap<>();
    System.out.println("map instanceof Collection=" + (map instanceof Collection<?>));
    Iterator<?> it = c.iterator();
    System.out.println("it=" + it);
    
  }
  
  @Test
  public void testCopyBuffer() {
    
    byte[] buf = TestUtils.randomBytes(1000000 + 30);
    long start = System.nanoTime();
    for(int i = 0; i < 1000; i++) {
      System.arraycopy(buf, 30, buf, 0, 1000000);
    }
    long end = System.nanoTime();
    
    System.out.printf("time=%d\n", (end - start) / 1000);
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
    System.out.printf(" %d %d %d\n", vi, vs, vvs);
  }
  
  @Test
  public void testValue14() {
    int vi = 0xffff;
    int vs = (short) vi;
    int vvs = (short) (0x3fff & vs);
    System.out.printf(" %d %d %d\n", vi, vs, vvs);
  }
  @Test
  public void testValue1() {
    int v1 = 0xffff;
    int v2 = 0x3fff;
    short s1 = (short) v1;
    short s2 = (short) v2;
    short ss1 = (short)((s1 >> 15) & 1);
    short ss2 = (short) ((s2 >> 15) & 1);
    
    System.out.printf(" %d %d %d %d %d %d\n", v1, v2, s1, s2, ss1, ss2);

  }
  @Test
  public void testValue2() {
    int v1 = 0xffff;
    int v2 = 0xbfff;
    short s1 = (short) v1;
    short s2 = (short) v2;
    short ss1 = (short)((s1 >> 14) & 3);
    short ss2 = (short) ((s2 >> 14) & 3);
    
    System.out.printf(" %d %d %d %d %d %d\n", v1, v2, s1, s2, ss1, ss2);

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
    System.out.println("Zipf distribution");
    ZipfDistribution zd = new ZipfDistribution(1000000, 0.9);
    
    for (int i = 0; i < 100; i++) {
      System.out.println(zd.sample());
    }
  }
  
  /**
   * Utility methods
   */
  private short low15 (short v) {
    return (short)(v & 0x7fff);
  }

  private short low14 (short v) {
    return (short)(v & 0x3fff);
  }

  private  short high1(short v) {
    return (short)((v >> 15) & 1);
  }
  
  public  short high2(short v) {
    return (short)((v >> 14) & 3);
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
  
  public short min2 (short v) {
    return (short) (v | 0x4000);
  }
  
  public short hours2(short v) {
    return (short) (v | 0xc000);
  }
  
  
}
