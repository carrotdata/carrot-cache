/**
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
package com.onecache.core.expire;

import com.onecache.core.util.UnsafeAccess;
import com.onecache.core.util.Utils;

public interface ExpireSupport {
  /*Expiration field size - 2 bytes*/
  public final static int FIELD_SIZE = Utils.SIZEOF_SHORT;
  
  /* One epoch duration for seconds type SM in ms */
  public static long EPOCH_DURATION_SEC_SM_TYPE = (long)(1 << 14) * 1000;
  
  /* One epoch duration for minutes type SM in ms */
  public static long EPOCH_DURATION_MIN_SM_TYPE = (long) (1 << 14) * 1000 * 60;
  
  /* One epoch duration for seconds type SMH in ms */
  public static long EPOCH_DURATION_SEC_SMH_TYPE = (long)(1 << 13) * 1000;
  
  /* One epoch duration for minutes type SMH in ms */
  public static long EPOCH_DURATION_MIN_SMH_TYPE = (long) (1 << 13) * 1000 * 60;
  
  /* One epoch duration for minutes type SMH in ms */
  public static long EPOCH_DURATION_HOURS_SMH_TYPE = (long) (1 << 13) * 1000 * 3600;
  
  /* Expiration types */
  
  /* Expiration field contains time in seconds */
  public static int TIME_IN_SECONDS = 0;
  
  /* Expiration field contains time in minutes */
  public static int TIME_IN_MINUTES = 1;
  
  /* Expiration field contains time in hours */
  public static int TIME_IN_HOURS = 3;
  
  /** 
   * Get meta section size 
   * 
   * Format meta section:
   * 8 bytes - current start access time
   * 2 bytes - epoch seconds counter
   * 2 bytes - epoch minutes counter
   * 2 bytes - epoch hours counter (optional) 
   **/
  public default int getExpireMetaSectionSize () {
    return Utils.SIZEOF_LONG;// to keep current scan start time 
  }
  
  /**
   * Get expiration time in ms 
   * @param ibesPtr
   * @param expireFieldPtr
   * @return expiration time in ms (0 - does not expire, -1 - expired)
   */
  public long getExpire(long ibesPtr, long expireFieldPtr);
  
  /**
   * Sets expiration time for an item
   * @param ibesPtr index block expire section pointer
   * @param expireFieldPtr address of an expiration field of an index
   * @param expire expiration time in ms since 01-01-1970 12am
   */
  public void setExpire(long ibesPtr, long expireFieldPtr, long expire);
 
  /**
   * Update meta section at the end of an index block scan
   * @param ibesPtr index block expire section address
   */
  public void updateMeta (long ibesPtr);
  
  /**
   * Get seconds epoch counter value
   * @param ibesPtr index block expiration section address
   * @return seconds epoch counter value
   */
  public default int getSecondsEpochCounterValue (long ibesPtr) {
    final int off = Utils.SIZEOF_LONG;
    return UnsafeAccess.toShort(ibesPtr + off) & 0xffff;
  }
  
  /**
   * Get minutes epoch counter value
   * @param ibesPtr index block expiration section address
   * @return minutes epoch counter value
   */
  public default int getMinutesEpochCounterValue (long ibesPtr) {
    final int off = Utils.SIZEOF_LONG + Utils.SIZEOF_SHORT;
    return UnsafeAccess.toShort(ibesPtr + off) & 0xffff;
  }
  
  /**
   * Get hours epoch counter value
   * @param ibesPtr index block expiration section address
   * @return minutes epoch counter value
   */
  public default int getHoursEpochCounterValue (long ibesPtr) {
    final int off = Utils.SIZEOF_LONG + 2 * Utils.SIZEOF_SHORT;
    return UnsafeAccess.toShort(ibesPtr + off) & 0xffff;
  }
  
  /**
   * Set seconds epoch counter value
   * @param ibesPtr index block expiration section address
   * @param v value
   */
  public default void setSecondsEpochCounterValue (long ibesPtr, int v) {
    final int off = Utils.SIZEOF_LONG;
    UnsafeAccess.putShort(ibesPtr + off, (short) v);
  }
  
  /**
   * Set minutes epoch counter value
   * @param ibesPtr index block expiration section address
   * @param v value
   */
  public default void setMinutesEpochCounterValue (long ibesPtr, int v ) {
    final int off = Utils.SIZEOF_LONG + Utils.SIZEOF_SHORT;
    UnsafeAccess.putShort(ibesPtr + off, (short) v);
  }
  
  /**
   * Set hours epoch counter value
   * @param ibesPtr index block expiration section address
   * @param v value
   */
  public default void setHoursEpochCounterValue (long ibesPtr, int v) {
    final int off = Utils.SIZEOF_LONG + 2 * Utils.SIZEOF_SHORT;
    UnsafeAccess.putShort(ibesPtr + off, (short) v);
  }
  
  /**
   * Begin index block
   * @param ibesPtr index block expire section address
   * @param force forces scan block scan
   * @return true, if full block scan is required, false - otherwise
   */
  public boolean begin(long ibesPtr, boolean force);
  
  /**
   * End index block
   * @param ibesPtr index block expire section address
   */
  public default void end(long ibesPtr) {
    updateMeta(ibesPtr);
    setAccessStartTime(ibesPtr, 0);
  }
  
  /**
   * Return current scan time
   * @param ibesPtr index block expire section address
   * @return scan start time if in progress or 0
   */
  public default long getAccessStartTime(long ibesPtr) {
    return UnsafeAccess.toLong(ibesPtr);
  }
  
  /**
   * Sets scan time start
   * @param ibesPtr index block expire section address
   * @param time
   */
  public default void setAccessStartTime(long ibesPtr, long time) {
    UnsafeAccess.putLong(ibesPtr, time);
  }
  
  /**
   * Utility conversion methods
   * TODO: redo the code
   */
  
  
  /**
   * Get value of a lowest 15 bits
   * @param v value
   * @return lowest 15 bits value
   */
  public default short low15 (short v) {
    return (short)(v & 0x7fff);
  }

  /**
   * Get value of a lowest 14 bits
   * @param v value
   * @return lowest 14 bits value
   */
  public default short low14 (short v) {
    return (short)(v & 0x3fff);
  }

  /*
   * First bit value
   * @param v value
   * return highest 1 bit value (0 or 1)
   */
  public default short high1(short v) {
    return (short)((v >> 15) & 1);
  }
  
  /**
   * First 2-bits value
   * @param v value
   * @return highest 2 bits value (0 - 3)
   */
  public default short high2(short v) {
    return (short)((v >> 14) & 3);
  }
  
  /**
   * Sets highest bit to 0 (for seconds)
   * @param v original value
   * @return with highest bit set to 0
   */
  public default short sec1(short v) {
    return v;
  }
  /**
   * Sets highest 2 bits to 0 (for seconds)
   * @param v original value
   * @return with highest 2 bits set to 00
   */
  public default short sec2(short v) {
    return v;
  }
  /**
   * Sets highest bit to 1 (for minutes)
   * @param v original value
   * @return with highest bit set to 1
   */
  public default short min1(short v) {
    return (short) (v | 0x8000);
  }
  /**
   * Sets highest 2 bits to 01 (for minutes)
   * @param v original value
   * @return with highest 2 bits set to 01
   */
  public default short min2 (short v) {
    return (short) (v | 0x4000);
  }
  /**
   * Sets highest 2 bit to 11 (for hours)
   * @param v original value
   * @return with highest bit set to 11
   */
  public default short hours2(short v) {
    return (short) (v | 0xc000);
  }
}
