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
package com.onecache.core.expire;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.onecache.core.util.UnsafeAccess;
import com.onecache.core.util.Utils;

public class TestExpireSupport {
  
  static long MAX_SECS_DIFF = 1000;
  static long MAX_MINS_DIFF = 60 * 1000;
  static long MAX_HOURS_DIFF = 3600 * 1000;
  
  
  long ibPtr;
  int size = 4096;
  @Before
  public void setUp() {
    this.ibPtr = UnsafeAccess.mallocZeroed(size);
  }
  
  @After
  public void tearDown() {
    UnsafeAccess.free(this.ibPtr);
  }
  
  @Test
  public void testExpireSupportBase() {
    ExpireSupport support = new ExpireSupportSecondsMinutes();
    // high1 tests
    short v = -1;
    short exp = 1;
    assertEquals(exp, support.high1(v));
    v = -100;
    assertEquals(exp, support.high1(v));
    v  = Short.MIN_VALUE;
    assertEquals(exp, support.high1(v));
    exp = 0;
    v = 1;
    assertEquals(exp, support.high1(v));
    v = 1000;
    assertEquals(exp, support.high1(v));
    v = Short.MAX_VALUE;
    assertEquals(exp, support.high1(v));
    // high2
    v = -1;
    exp = 3;
    assertEquals(exp, support.high2(v));
    v = (short) 0x0fff;
    exp = 0;
    assertEquals(exp, support.high2(v));
    v = (short) 0x7fff;
    exp = 1;
    assertEquals(exp, support.high2(v));
    v = (short) 0xbfff;
    exp = 2;
    assertEquals(exp, support.high2(v)); 
    
    // low15 tests
    v = (short) 0xffff;
    exp = (short) 0x7fff;
    assertEquals(exp, support.low15(v));
    v = (short) 0x7fff;
    exp = (short) 0x7fff;
    assertEquals(exp, support.low15(v));
    v  = (short) 0x8000;
    exp = (short) 0;
    assertEquals(exp, support.low15(v));
    exp = 1;
    v = 1;
    assertEquals(exp, support.low15(v));
    v = 1000;
    exp = 1000;
    assertEquals(exp, support.low15(v));
    v = Short.MAX_VALUE;
    exp = Short.MAX_VALUE;
    assertEquals(exp, support.low15(v));
    
    // low14
    v = (short) 0xffff;
    exp = 0x3fff;
    assertEquals(exp, support.low14(v));
    v = (short) 0x0fff;
    exp = (short) 0x0fff;
    assertEquals(exp, support.low14(v));
    v = (short) 0xc000;
    exp = 0;
    assertEquals(exp, support.low14(v));
    v = (short) 0xbfff;
    exp = (short) 0x3fff;
    assertEquals(exp, support.low14(v)); 
    
    // min1
    v = (short) 0x1;
    exp = (short) 0x8001;
    
    assertEquals(exp, support.min1(v));
    // min2
    v = (short) 0x1;
    exp = (short) 0x4001;
    
    assertEquals(exp, support.min2(v));
    // hours2
    v = (short) 0x1;
    exp = (short) 0xc001;
    
    assertEquals(exp, support.hours2(v));
    
    
    // Tests for ExpireSupport API
    long time = System.currentTimeMillis();
    support.setAccessStartTime(ibPtr, time);
    long result = support.getAccessStartTime(ibPtr);
    assertEquals(time, result);
    
    int vv = 111;
    support.setSecondsEpochCounterValue(ibPtr, vv);
    int res = support.getSecondsEpochCounterValue(ibPtr);
    assertEquals(vv, res);
    
    support.setMinutesEpochCounterValue(ibPtr, vv);
    res = support.getMinutesEpochCounterValue(ibPtr);
    assertEquals(vv, res);
    
    support.setHoursEpochCounterValue(ibPtr, vv);
    res = support.getHoursEpochCounterValue(ibPtr);
    
    assertEquals(vv, res);
    
  }
  
  @Test
  public void testExpireSupportSecondsMinutes() {
    long expPtr = this.ibPtr + 64;
    
    ExpireSupportSecondsMinutes support = new ExpireSupportSecondsMinutes();
    assertEquals(Utils.SIZEOF_LONG + 2 * Utils.SIZEOF_SHORT, support.getExpireMetaSectionSize());
    
    support.begin(ibPtr, true);
    long accessStartTime = support.getAccessStartTime(ibPtr);
    
    long expire = (accessStartTime + 100 * 1000) ; // + 100 sec 
    // Test timeouts in seconds
    support.setExpire(ibPtr, expPtr, expire);
    
    long exp = support.getExpire(ibPtr, expPtr);
    
    assertTrue(Math.abs(exp - expire) < MAX_SECS_DIFF);
        
    // Test timeouts in seconds
    expire = (accessStartTime + 10000 * 1000);
    support.setExpire(ibPtr, expPtr, expire);
    
    exp = support.getExpire(ibPtr, expPtr);
    
    assertTrue(Math.abs(exp - expire) < MAX_SECS_DIFF);
    // Minutes
    expire = (accessStartTime + 100000 * 1000);
    support.setExpire(ibPtr, expPtr, expire);
    
    exp = support.getExpire(ibPtr, expPtr);
    
    assertTrue(Math.abs(exp - expire) < MAX_MINS_DIFF);
    
    support.end(ibPtr);
  }
  
  @Test
  public void testExpireSupportSecondsMinutesWithUpdates() {
    long ptr1 = this.ibPtr + 64;
    long ptr2 = this.ibPtr + 66;
    
    ExpireSupportSecondsMinutes support = new ExpireSupportSecondsMinutes();
    
    support.begin(ibPtr, true);
    long accessStartTime = support.getAccessStartTime(ibPtr);
    support.setEpochStartTime(accessStartTime);
    
    long expire = (accessStartTime + ExpireSupport.EPOCH_DURATION_SEC_SM_TYPE + 1001) ; // + 16385 sec 
    // Test timeouts in seconds
    support.setExpire(ibPtr, ptr1, expire);
    short value = UnsafeAccess.toShort(ptr1);
    short expValue = (short) (ExpireSupport.EPOCH_DURATION_SEC_SM_TYPE / 1000) + 1;
    assertEquals(expValue, value);
    long exp = support.getExpire(ibPtr, ptr1);
    assertTrue(Math.abs(exp - expire) < MAX_SECS_DIFF);
        
    // Minutes
    expire = (accessStartTime + ExpireSupport.EPOCH_DURATION_MIN_SM_TYPE + 60 * 1000);
    support.setExpire(ibPtr, ptr2, expire);
    value = (short) (UnsafeAccess.toShort(ptr2) & 0x7fff);
    expValue = (short) (ExpireSupport.EPOCH_DURATION_MIN_SM_TYPE / (60 * 1000)) + 1;
    assertEquals(expValue, value);
    exp = support.getExpire(ibPtr, ptr2);
    assertTrue(Math.abs(exp - expire) < MAX_MINS_DIFF);
    support.end(ibPtr);

    int secs = support.getSecondsEpochCounterValue(ibPtr);
    int expSecs = 0;
    
    assertEquals(expSecs, secs);
    
    int mins = support.getMinutesEpochCounterValue(ibPtr);
    int expMins = 0;
    assertEquals(expMins, mins);

    // Now move epoch backwards
    support.begin(ibPtr, true);  
    support.setAccessStartTime(ibPtr, accessStartTime);
    support.setEpochStartTime(accessStartTime - ExpireSupport.EPOCH_DURATION_SEC_SM_TYPE);
    expire = (accessStartTime + ExpireSupport.EPOCH_DURATION_SEC_SM_TYPE + 1001) ; // + 16385 sec 
    exp = support.getExpire(ibPtr, ptr1);
    exp += ExpireSupport.EPOCH_DURATION_SEC_SM_TYPE;
    assertTrue(Math.abs(exp - expire) < MAX_SECS_DIFF);
    
    value = UnsafeAccess.toShort(ptr1);
    expValue = (short) 1; // Updated seconds
    assertEquals(expValue, value);
    
    expire = (accessStartTime + ExpireSupport.EPOCH_DURATION_MIN_SM_TYPE + 60 * 1000) ; 
    
    exp = support.getExpire(ibPtr, ptr2);
    exp += ExpireSupport.EPOCH_DURATION_SEC_SM_TYPE;
    
    assertTrue(Math.abs(exp - expire) < MAX_MINS_DIFF);
    
    value = (short) (UnsafeAccess.toShort(ptr2) & 0x7fff);
    expValue = (short) (ExpireSupport.EPOCH_DURATION_MIN_SM_TYPE / (60 * 1000)) + 1;
    assertEquals(expValue, value);
    support.end(ibPtr);
    // Minutes
    support.begin(ibPtr, true);    
    support.setEpochStartTime(accessStartTime - ExpireSupport.EPOCH_DURATION_MIN_SM_TYPE);
    exp = support.getExpire(ibPtr, ptr2);
    exp += ExpireSupport.EPOCH_DURATION_MIN_SM_TYPE;
    assertTrue(Math.abs(exp - expire) < MAX_MINS_DIFF);
      
    support.end(ibPtr);
    
    secs = support.getSecondsEpochCounterValue(ibPtr);
    expSecs = 60;
    assertEquals(expSecs, secs);
    mins = support.getMinutesEpochCounterValue(ibPtr);
    expMins = 1;
    assertEquals(expMins, mins);
    
    
  }
  
  
  @Test
  public void testExpireSupportSecondsMinutesHours() {
   long expPtr = this.ibPtr + 64;
    
   ExpireSupportSecondsMinutesHours support = new ExpireSupportSecondsMinutesHours();
    assertEquals(Utils.SIZEOF_LONG + 3 * Utils.SIZEOF_SHORT, support.getExpireMetaSectionSize());
    
    support.begin(ibPtr, true);
    long accessStartTime = support.getAccessStartTime(ibPtr);
    
    long expire = (accessStartTime + 100 * 1000) ; // + 100 sec 
    // Test timeouts in seconds
    support.setExpire(ibPtr, expPtr, expire);
    
    long exp = support.getExpire(ibPtr, expPtr);
    
    assertTrue(Math.abs(exp - expire) < MAX_SECS_DIFF);
        
    // Test timeouts in seconds
    expire = (accessStartTime + 10000 * 1000);
    support.setExpire(ibPtr, expPtr, expire);
    
    exp = support.getExpire(ibPtr, expPtr);
    
    assertTrue(Math.abs(exp - expire) < MAX_SECS_DIFF);
    // Minutes
    expire = (accessStartTime + 100000 * 1000);
    support.setExpire(ibPtr, expPtr, expire);
    
    exp = support.getExpire(ibPtr, expPtr);
    
    assertTrue(Math.abs(exp - expire) < MAX_MINS_DIFF);
    
    //one more minutes
    expire = (accessStartTime + 8192L * 60 * 1000);
    support.setExpire(ibPtr, expPtr, expire);
    
    exp = support.getExpire(ibPtr, expPtr);
    
    assertTrue(Math.abs(exp - expire) < MAX_MINS_DIFF);
    
    // Hours
    expire = (accessStartTime + 20000L * 60 * 1000);
    support.setExpire(ibPtr, expPtr, expire);
    
    exp = support.getExpire(ibPtr, expPtr);
    
    assertTrue(Math.abs(exp - expire) < MAX_HOURS_DIFF);
    support.end(ibPtr);

  }
  
  @Test
  public void testExpireSupportSecondsMinutesHoursWithUpdates() {
    long ptr1 = this.ibPtr + 64;
    long ptr2 = this.ibPtr + 66;
    long ptr3 = this.ibPtr + 68;
    
    ExpireSupportSecondsMinutesHours support = new ExpireSupportSecondsMinutesHours();
    
    support.begin(ibPtr, true);
    long accessStartTime = support.getAccessStartTime(ibPtr);
    support.setEpochStartTime(accessStartTime);
    
    long expire = (accessStartTime + ExpireSupport.EPOCH_DURATION_SEC_SMH_TYPE + 1001) ; // + 16385 sec 
    // Test timeouts in seconds
    support.setExpire(ibPtr, ptr1, expire);
    short value = UnsafeAccess.toShort(ptr1);
    short expValue = (short) (ExpireSupport.EPOCH_DURATION_SEC_SMH_TYPE / 1000) + 1;
    assertEquals(expValue, value);
    long exp = support.getExpire(ibPtr, ptr1);
    assertTrue(Math.abs(exp - expire) < MAX_SECS_DIFF);
        
    // Minutes
    expire = (accessStartTime + ExpireSupport.EPOCH_DURATION_MIN_SMH_TYPE + 60 * 1000);
    support.setExpire(ibPtr, ptr2, expire);
    value = (short) (UnsafeAccess.toShort(ptr2) & 0x3fff);
    expValue = (short) (ExpireSupport.EPOCH_DURATION_MIN_SMH_TYPE / (60 * 1000)) + 1;
    assertEquals(expValue, value);
    exp = support.getExpire(ibPtr, ptr2);
    assertTrue(Math.abs(exp - expire) < MAX_MINS_DIFF);

    int secs = support.getSecondsEpochCounterValue(ibPtr);
    int expSecs = 0;
    
    assertEquals(expSecs, secs);
    
    int mins = support.getMinutesEpochCounterValue(ibPtr);
    int expMins = 0;
    assertEquals(expMins, mins);

    int hours = support.getHoursEpochCounterValue(ibPtr);
    int expHours = 0;
    assertEquals(expHours, hours);
    
    // Hours
    expire = (accessStartTime + ExpireSupport.EPOCH_DURATION_HOURS_SMH_TYPE + 3600 * 1000);
    support.setExpire(ibPtr, ptr3, expire);
    value = (short) (UnsafeAccess.toShort(ptr3) & 0x3fff); // low14
    expValue = (short) (ExpireSupport.EPOCH_DURATION_HOURS_SMH_TYPE / (3600 * 1000)) + 1;
    assertEquals(expValue, value);
    exp = support.getExpire(ibPtr, ptr3);
    assertTrue(Math.abs(exp - expire) < MAX_HOURS_DIFF);
    support.end(ibPtr);
    
    hours = support.getHoursEpochCounterValue(ibPtr);
    expHours = 0;
    assertEquals(expHours, hours);

    // Now move epoch backwards
    // Seconds
    support.begin(ibPtr, true);  
    support.setAccessStartTime(ibPtr, accessStartTime);
    support.setEpochStartTime(accessStartTime - ExpireSupport.EPOCH_DURATION_SEC_SMH_TYPE);
    expire = (accessStartTime + ExpireSupport.EPOCH_DURATION_SEC_SMH_TYPE + 1001) ; // + 16385 sec 
    exp = support.getExpire(ibPtr, ptr1);
    exp += ExpireSupport.EPOCH_DURATION_SEC_SMH_TYPE;
    assertTrue(Math.abs(exp - expire) < MAX_SECS_DIFF);
    
    value = UnsafeAccess.toShort(ptr1);
    expValue = (short) 1; // Updated seconds
    assertEquals(expValue, value);
    
    expire = (accessStartTime + ExpireSupport.EPOCH_DURATION_MIN_SMH_TYPE + 60 * 1000) ; 
    
    exp = support.getExpire(ibPtr, ptr2);
    exp += ExpireSupport.EPOCH_DURATION_SEC_SMH_TYPE;
    
    assertTrue(Math.abs(exp - expire) < MAX_MINS_DIFF);
    
    value = (short) (UnsafeAccess.toShort(ptr2) & 0x3fff);
    expValue = (short) (ExpireSupport.EPOCH_DURATION_MIN_SMH_TYPE / (60 * 1000)) + 1;
    assertEquals(expValue, value);
    
    expire = (accessStartTime + ExpireSupport.EPOCH_DURATION_HOURS_SMH_TYPE + 3600 * 1000) ; 
    
    exp = support.getExpire(ibPtr, ptr3);
    exp += ExpireSupport.EPOCH_DURATION_SEC_SMH_TYPE;
    
    assertTrue(Math.abs(exp - expire) < MAX_HOURS_DIFF);
    
    value = (short) (UnsafeAccess.toShort(ptr3) & 0x3fff);
    expValue = (short) (ExpireSupport.EPOCH_DURATION_HOURS_SMH_TYPE / (3600 * 1000)) + 1;
    assertEquals(expValue, value);
    support.end(ibPtr);
    
    // Minutes
    support.begin(ibPtr, true);    
    support.setEpochStartTime(accessStartTime - ExpireSupport.EPOCH_DURATION_MIN_SMH_TYPE);
    expire = (accessStartTime + ExpireSupport.EPOCH_DURATION_MIN_SMH_TYPE + 60 * 1000) ; 

    exp = support.getExpire(ibPtr, ptr2);
    exp += ExpireSupport.EPOCH_DURATION_MIN_SMH_TYPE;
    assertTrue(Math.abs(exp - expire) < MAX_MINS_DIFF);
      
    support.end(ibPtr);
    
    secs = support.getSecondsEpochCounterValue(ibPtr);
    expSecs = 60;
    assertEquals(expSecs, secs);
    mins = support.getMinutesEpochCounterValue(ibPtr);
    expMins = 1;
    assertEquals(expMins, mins);
    hours = support.getHoursEpochCounterValue(ibPtr);
    expHours = 0;
    
    assertEquals(expHours, hours);
    
    // Hours
    
    support.begin(ibPtr, true);    
    support.setEpochStartTime(accessStartTime - ExpireSupport.EPOCH_DURATION_HOURS_SMH_TYPE);
    expire = (accessStartTime + ExpireSupport.EPOCH_DURATION_HOURS_SMH_TYPE + 3600 * 1000) ; 

    exp = support.getExpire(ibPtr, ptr3);
    exp += ExpireSupport.EPOCH_DURATION_HOURS_SMH_TYPE;
    assertTrue(Math.abs(exp - expire) < MAX_HOURS_DIFF);
      
    support.end(ibPtr);
    
    secs = support.getSecondsEpochCounterValue(ibPtr);
    expSecs = 3600;
    assertEquals(expSecs, secs);
    mins = support.getMinutesEpochCounterValue(ibPtr);
    expMins = 60;
    assertEquals(expMins, mins);
    hours = support.getHoursEpochCounterValue(ibPtr);
    expHours = 1;
    
    assertEquals(expHours, hours);
  }
  
}
