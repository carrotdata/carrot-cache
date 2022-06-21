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
package com.carrot.cache.expire;

import com.carrot.cache.util.UnsafeAccess;
import com.carrot.cache.util.Utils;
/**
 * 
 * Support for compact expiration time in Seconds-Minutes format
 * It takes only 2 bytes in the index field. There are additional 2 2 bytes fields in
 * a the Meta section of each index block
 *
 */
public class ExpireSupportSecondsMinutesHours implements ExpireSupport {
  /* Time when 0 epoch started in ms since 01-01-1970*/
  private long epochStartTime;
  
  /**
   * Default constructor
   * @param type
   * @param epochStartTime
   */
  public ExpireSupportSecondsMinutesHours(long epochStartTime) {
    this.epochStartTime = epochStartTime;
  }

  /**
   * Get expiration meta section size for index data block. Expire adds additional 
   * meta section to each index block meta.
   * 
   * Format meta
   * 
   * Type.SM
   * 
   * 2 bytes - seconds epoch counter
   * 2 bytes - minutes 
   * 
   * Type.SMH
   * 2 bytes - seconds epoch counter
   * 2 bytes - minutes 
   * 2 bytes  - hours
   * @param type expire type
   * @return meta section size in bytes
   */
  public int getExpireMetaSectionSize () {
    return Utils.SIZEOF_LONG +  3 * Utils.SIZEOF_SHORT;
  }
  
  /**
   * Checks and updates expiration time or expire item
   * @param ibesPtr index block expiration section address
   * @param expireFieldPtr item expire field address (2 bytes are used to keep expiration time)
   * @return true if expired, false otherwise
   */
  public final boolean updateOrExpire(long ibesPtr, long expireFieldPtr) {
    
    long accessStartTime = getAccessStartTime(ibesPtr);
    
    long expTime = absoluteTime(ibesPtr, expireFieldPtr);
    if (accessStartTime > expTime) {
      return true; // expired
    }
    // else check if we need updated expiration field
    short value = UnsafeAccess.toShort(expireFieldPtr);
    int type  = high2(value) ;
    value = low14(value);
    switch (type) {
      case TIME_IN_SECONDS:
        {
          
          int currentCounter = getCurrentSecondsEpochCounter(accessStartTime);
          int blockCounter = getSecondsEpochCounterValue(ibesPtr);
          if (currentCounter - blockCounter > 1) {
            return true; // expired b/c at least two epochs behind
          } else if (currentCounter - blockCounter == 1) {
            // currentCounter = blockCounter + 1
            // Update expiration field
            value -= EPOCH_DURATION_SEC_SMH_TYPE / 1000;
            // Save it back
            UnsafeAccess.putShort(expireFieldPtr, sec2(value));
          }
        }
      case TIME_IN_MINUTES:
        { // minutes
          int currentCounter = getCurrentMinutesEpochCounter(accessStartTime);
          int blockCounter = getMinutesEpochCounterValue(ibesPtr);
          if (currentCounter - blockCounter > 1) {
            return true; // expired b/c at least two epochs behind
          } else if (currentCounter - blockCounter == 1) {
            // currentCounter = blockCounter + 1
            // Update expiration field
            value -= EPOCH_DURATION_MIN_SMH_TYPE / 60 * 1000;
            // Save it back
            UnsafeAccess.putShort(expireFieldPtr, min2(value));
          }
        }
      case TIME_IN_HOURS:
        int currentCounter = getCurrentHoursEpochCounter(accessStartTime);
        int blockCounter = getHoursEpochCounterValue(ibesPtr);
        if (currentCounter - blockCounter > 1) {
          return true; // expired b/c at least two epochs behind
        } else if (currentCounter - blockCounter == 1) {
          // currentCounter = blockCounter + 1
          // Update expiration field
          value -= EPOCH_DURATION_HOURS_SMH_TYPE / 3600 * 1000;
          // Save it back
          UnsafeAccess.putShort(expireFieldPtr, hours2(value));
        }
        break;
      default:
    }
    return false;
  }
  
  public final long absoluteTime(long ibesPtr, long expireFieldPtr) {
    // check first bit
    // 1 - minutes
    // 0 - seconds
    short value = UnsafeAccess.toShort(expireFieldPtr);
    int type  = high2(value) ; 
    value = low14(value);

    switch(type) {
      case TIME_IN_SECONDS:
        long start = this.epochStartTime + 
        getSecondsEpochCounterValue(ibesPtr) * EPOCH_DURATION_SEC_SMH_TYPE;
        return start + value * 1000;
      case TIME_IN_MINUTES:
        start = this.epochStartTime + 
        getMinutesEpochCounterValue(ibesPtr) * EPOCH_DURATION_MIN_SMH_TYPE;
        return start + value * 60 * (long)1000; 
      case TIME_IN_HOURS:
        start = this.epochStartTime + 
        getHoursEpochCounterValue(ibesPtr) * EPOCH_DURATION_HOURS_SMH_TYPE;
        return start + value * 3600 * (long)1000; 
      default:
        return -1;
    }
  }

  @Override
  public void updateMeta(long ibesPtr) {
    long accessStartTime = getAccessStartTime(ibesPtr);
    int c = getCurrentSecondsEpochCounter(accessStartTime);
    setSecondsEpochCounterValue(ibesPtr, c);
    c = getCurrentMinutesEpochCounter(accessStartTime);
    setMinutesEpochCounterValue(ibesPtr, c);
    c = getCurrentHoursEpochCounter(accessStartTime);
    setHoursEpochCounterValue(ibesPtr, c);
  }
  
  /**
   * Get current counter for minutes epoch
   * @param accessStartTime access start time
   * @return counter
   */
  final int getCurrentMinutesEpochCounter(long accessStartTime) {
    return (int)((accessStartTime - this.epochStartTime) / EPOCH_DURATION_MIN_SMH_TYPE);
  }
  
  /**
   * Get current counter for seconds epoch
   * @param accessStartTime access start time
   * @return counter
   */
  public int getCurrentSecondsEpochCounter(long accessStartTime) {
    return (int)((accessStartTime - epochStartTime) / EPOCH_DURATION_SEC_SMH_TYPE);
  }
  
  /**
   * Get current counter for hours epoch
   * @param accessStartTime access start time
   * @return counter
   */
  public int getCurrentHoursEpochCounter(long accessStartTime) {
    return (int)((accessStartTime - epochStartTime) / EPOCH_DURATION_HOURS_SMH_TYPE);
  }
}
