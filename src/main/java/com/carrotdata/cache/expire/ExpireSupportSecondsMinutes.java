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
package com.carrotdata.cache.expire;

import com.carrotdata.cache.util.Epoch;
import com.carrotdata.cache.util.UnsafeAccess;
import com.carrotdata.cache.util.Utils;
/**
 * 
 * Support for compact expiration time in Seconds-Minutes format
 * It takes only 2 bytes in the index field. There are additional 2 2 bytes fields in
 * the Meta section of each index block
 *
 */
public class ExpireSupportSecondsMinutes extends AbstractExpireSupport {
  /* Time when 0 epoch started in ms since 01-01-1970*/
  private long epochStartTime;
  
  /**
   * Default constructor
   */
  public ExpireSupportSecondsMinutes() {
    this.epochStartTime = Epoch.getEpochStartTime();
    this.metaSectionSize = super.metaSectionSize + 2 * Utils.SIZEOF_SHORT;
  }
  
  /**
   * For testing only
   * @param time
   */
  public void setEpochStartTime(long time) {
    this.epochStartTime = time;
  }
  
  /**
   * Checks and updates expiration time or expire item
   * @param ibesPtr index block expiration section address
   * @param expireFieldPtr item expire field address (2 bytes are used to keep expiration time)
   * @return true if expired, false otherwise
   */
  public final long getExpire(long ibesPtr, long expireFieldPtr) {
    
    long accessStartTime = getAccessStartTime(ibesPtr);
    
    long expTime = getExpire0(ibesPtr, expireFieldPtr);
    //accessStartTime = 0 means no update for expire field
    if (accessStartTime == 0 || accessStartTime > expTime) {
      return expTime; // expired
    }
    // else check if we need updated expiration field
    short value = UnsafeAccess.toShort(expireFieldPtr);
    boolean isSeconds = value >= 0; // 1st bit is '0' for seconds and '1'for minutes
    if (isSeconds) {
      int currentCounter = getCurrentSecondsEpochCounter(accessStartTime);
      int blockCounter = getSecondsEpochCounterValue(ibesPtr);
      if (currentCounter - blockCounter > 1) {
        return expTime; // expired b/c at least two epochs behind
      } else if (currentCounter - blockCounter == 1) {
        // currentCounter = blockCounter + 1
        // Update expiration field
        value -= EPOCH_DURATION_SEC_SM_TYPE / 1000; 
        // Save it back
        UnsafeAccess.putShort(expireFieldPtr, value);
      }
      return expTime;
    } else { // minutes
      value = low15(value); // set first bit to 0
      int currentCounter = getCurrentMinutesEpochCounter(accessStartTime);
      int blockCounter = getMinutesEpochCounterValue(ibesPtr);
      if (currentCounter - blockCounter > 1) {
        return expTime; // expired b/c at least two epochs behind
      } else if (currentCounter - blockCounter == 1) {
        // currentCounter = blockCounter + 1
        // Update expiration field
        value -= EPOCH_DURATION_MIN_SM_TYPE / 60 * 1000; 
        value = min1(value);
        //FIXME: what if its < 0?

        // Save it back
        UnsafeAccess.putShort(expireFieldPtr, value);
      }
      return expTime;
    }
  }
  
  private final long getExpire0(long ibesPtr, long expireFieldPtr) {
    // check first bit
    // 1 - minutes
    // 0 - seconds
    short value = UnsafeAccess.toShort(expireFieldPtr);
    boolean isSeconds = value >= 0; // 1st bit is '0'
    if (isSeconds) {
      long start = this.epochStartTime + 
          getSecondsEpochCounterValue(ibesPtr) * EPOCH_DURATION_SEC_SM_TYPE;
      
      return start + value * 1000;
    } else { // minutes
      value = low15(value);
      long start = this.epochStartTime + 
          getMinutesEpochCounterValue(ibesPtr) * EPOCH_DURATION_MIN_SM_TYPE;
      return start + value * 60 * (long)1000; 
    }
  }
  
  private long getEpochTimeSeconds(long ibesPtr) {
    int counter = getSecondsEpochCounterValue(ibesPtr);
    return counter * EPOCH_DURATION_SEC_SM_TYPE;
  }
  
  private long getEpochTimeMinutes(long ibesPtr) {
    int counter = getMinutesEpochCounterValue(ibesPtr);
    return counter * EPOCH_DURATION_MIN_SM_TYPE;
  }
  
  private short toSeconds(short v) {
    return v;
  }
  
  private short toMinutes(short v) {
    return (short) (v | 0x8000);
  }
  
  @Override
  public void setExpire(long ibesPtr, long expireFieldPtr, long expire) {
    long epochSecTime = getEpochTimeSeconds(ibesPtr);
    // Maximum expiration time in seconds we can set
    long maxSeconds = epochSecTime + 2 * EPOCH_DURATION_SEC_SM_TYPE;
    maxSeconds += epochStartTime;
    epochSecTime += epochStartTime;
    if (expire  < maxSeconds) {
      short secs = toSeconds((short)((expire - epochSecTime) / 1000));
      UnsafeAccess.putShort(expireFieldPtr, secs);
      return;
    }
    long epochMinTime = getEpochTimeMinutes(ibesPtr);
    long maxMinutes = epochMinTime + 2 * EPOCH_DURATION_MIN_SM_TYPE;
    maxMinutes += epochStartTime;
    epochMinTime += epochStartTime;
    if (expire > maxMinutes) {
      expire = maxMinutes;
    }
    short mins = toMinutes((short)((expire - epochMinTime) / (60 * 1000)));
    UnsafeAccess.putShort(expireFieldPtr, mins);    
  }
  
  @Override
  public void updateMeta(long ibesPtr) {
    long accessStartTime = getAccessStartTime(ibesPtr);
    int c = getCurrentSecondsEpochCounter(accessStartTime);
    setSecondsEpochCounterValue(ibesPtr, c);
    c = getCurrentMinutesEpochCounter(accessStartTime);
    setMinutesEpochCounterValue(ibesPtr, c);
  }
  
  /**
   * Get current counter for minutes epoch
   * @param accessStartTime access start time
   * @return counter
   */
  final int getCurrentMinutesEpochCounter(long accessStartTime) {
    return (int)((accessStartTime - this.epochStartTime) / EPOCH_DURATION_MIN_SM_TYPE);
  }
  
  /**
   * Get current counter for seconds epoch
   * @param accessStartTime access start time
   * @return counter
   */
  public int getCurrentSecondsEpochCounter(long accessStartTime) {
    return (int)((accessStartTime - epochStartTime) / EPOCH_DURATION_SEC_SM_TYPE);
  }

  @Override
  public boolean begin(long ibesPtr, boolean force) {
    long time = System.currentTimeMillis();
    if (force) {
      setAccessStartTime(ibesPtr, time);
      return true;
    }
    int secCounter = getSecondsEpochCounterValue(ibesPtr);
    int minCounter = getMinutesEpochCounterValue(ibesPtr);
    
    if (getCurrentSecondsEpochCounter(time) > secCounter || 
        getCurrentMinutesEpochCounter(time) > minCounter) {
      setAccessStartTime(ibesPtr, time);
      return true;
    }
    return false;
  }
}
