/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable
 * law or agreed to in writing, software distributed under the License is distributed on an "AS IS"
 * BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License
 * for the specific language governing permissions and limitations under the License.
 */
package com.carrotdata.cache.expire;

import com.carrotdata.cache.util.Epoch;
import com.carrotdata.cache.util.UnsafeAccess;
import com.carrotdata.cache.util.Utils;

/**
 * Support for compact expiration time in Seconds-Minutes format It takes only 2 bytes in the index
 * field. There are additional 2 2 bytes fields in a the Meta section of each index block
 */
public class ExpireSupportSecondsMinutesHours extends AbstractExpireSupport {
  /* Time when 0 epoch started in ms since 01-01-1970 */
  private long epochStartTime;

  /**
   * Default constructor
   */
  public ExpireSupportSecondsMinutesHours() {
    this.epochStartTime = Epoch.getEpochStartTime();
    this.metaSectionSize = super.metaSectionSize + 3 * Utils.SIZEOF_SHORT;
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
  @Override
  public final long getExpire(long ibesPtr, long expireFieldPtr) {

    long accessStartTime = getAccessStartTime(ibesPtr);

    long expTime = getExpire0(ibesPtr, expireFieldPtr);
    if (accessStartTime == 0 || accessStartTime > expTime) {
      return expTime; // expired
    }
    // else check if we need updated expiration field
    short value = UnsafeAccess.toShort(expireFieldPtr);
    int type = high2(value);
    value = low14(value);
    switch (type) {
      case TIME_IN_SECONDS: {

        int currentCounter = getCurrentSecondsEpochCounter(accessStartTime);
        int blockCounter = getSecondsEpochCounterValue(ibesPtr);
        if (currentCounter - blockCounter > 1) {
          return expTime; // expired b/c at least two epochs behind
        } else if (currentCounter - blockCounter == 1) {
          // currentCounter = blockCounter + 1
          // Update expiration field
          value -= EPOCH_DURATION_SEC_SMH_TYPE / 1000;
          // Save it back
          UnsafeAccess.putShort(expireFieldPtr, sec2(value));
        }
        break;
      }
      case TIME_IN_MINUTES: { // minutes
        int currentCounter = getCurrentMinutesEpochCounter(accessStartTime);
        int blockCounter = getMinutesEpochCounterValue(ibesPtr);
        if (currentCounter - blockCounter > 1) {
          return expTime; // expired b/c at least two epochs behind
        } else if (currentCounter - blockCounter == 1) {
          // currentCounter = blockCounter + 1
          // Update expiration field
          value -= EPOCH_DURATION_MIN_SMH_TYPE / 60 * 1000;
          // Save it back
          UnsafeAccess.putShort(expireFieldPtr, min2(value));
        }
        break;
      }
      case TIME_IN_HOURS:
        int currentCounter = getCurrentHoursEpochCounter(accessStartTime);
        int blockCounter = getHoursEpochCounterValue(ibesPtr);
        if (currentCounter - blockCounter > 1) {
          return expTime; // expired b/c at least two epochs behind
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
    return expTime;
  }

  private long getExpire0(long ibesPtr, long expireFieldPtr) {
    // check first bit
    // 1 - minutes
    // 0 - seconds
    short value = UnsafeAccess.toShort(expireFieldPtr);
    int type = high2(value);
    value = low14(value);

    switch (type) {
      case TIME_IN_SECONDS:
        long start = this.epochStartTime
            + getSecondsEpochCounterValue(ibesPtr) * EPOCH_DURATION_SEC_SMH_TYPE;
        return start + value * 1000;
      case TIME_IN_MINUTES:
        start = this.epochStartTime
            + getMinutesEpochCounterValue(ibesPtr) * EPOCH_DURATION_MIN_SMH_TYPE;
        return start + value * 60 * (long) 1000;
      case TIME_IN_HOURS:
        start = this.epochStartTime
            + getHoursEpochCounterValue(ibesPtr) * EPOCH_DURATION_HOURS_SMH_TYPE;
        return start + value * 3600 * (long) 1000;
      default:
        return -1;
    }
  }

  private long getEpochTimeSeconds(long ibesPtr) {
    int counter = getSecondsEpochCounterValue(ibesPtr);
    return counter * EPOCH_DURATION_SEC_SMH_TYPE;
  }

  private long getEpochTimeMinutes(long ibesPtr) {
    int counter = getMinutesEpochCounterValue(ibesPtr);
    return counter * EPOCH_DURATION_MIN_SMH_TYPE;
  }

  private long getEpochTimeHours(long ibesPtr) {
    int counter = getHoursEpochCounterValue(ibesPtr);
    return counter * EPOCH_DURATION_HOURS_SMH_TYPE;
  }

  private short toSeconds(short v) {
    return v;
  }

  private short toMinutes(short v) {
    return (short) (v | 0x4000);
  }

  private short toHours(short v) {
    return (short) (v | 0xc000);
  }

  @Override
  public void setExpire(long ibesPtr, long expireFieldPtr, long expire) {
    long epochSecTime = getEpochTimeSeconds(ibesPtr);
    // Maximum expiration time in seconds we can set
    long maxSeconds = epochSecTime + 2 * EPOCH_DURATION_SEC_SMH_TYPE;
    maxSeconds += epochStartTime;
    epochSecTime += epochStartTime;
    if (expire < maxSeconds) {
      short secs = toSeconds((short) ((expire - epochSecTime) / 1000));
      UnsafeAccess.putShort(expireFieldPtr, secs);
      return;
    }
    long epochMinTime = getEpochTimeMinutes(ibesPtr);
    long maxMinutes = epochMinTime + 2 * EPOCH_DURATION_MIN_SMH_TYPE;
    maxMinutes += epochStartTime;
    epochMinTime += epochStartTime;
    if (expire < maxMinutes) {
      short mins = toMinutes((short) ((expire - epochMinTime) / (60 * 1000)));
      UnsafeAccess.putShort(expireFieldPtr, mins);
      return;
    }
    long epochHoursTime = getEpochTimeHours(ibesPtr);
    long maxHours = epochHoursTime + 2 * EPOCH_DURATION_HOURS_SMH_TYPE;
    maxHours += epochStartTime;
    epochHoursTime += epochStartTime;
    if (expire > maxHours) {
      expire = maxHours;
    }
    short hours = toHours((short) ((expire - epochHoursTime) / (3600 * 1000)));
    UnsafeAccess.putShort(expireFieldPtr, hours);
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
    return (int) ((accessStartTime - this.epochStartTime) / EPOCH_DURATION_MIN_SMH_TYPE);
  }

  /**
   * Get current counter for seconds epoch
   * @param accessStartTime access start time
   * @return counter
   */
  public int getCurrentSecondsEpochCounter(long accessStartTime) {
    return (int) ((accessStartTime - epochStartTime) / EPOCH_DURATION_SEC_SMH_TYPE);
  }

  /**
   * Get current counter for hours epoch
   * @param accessStartTime access start time
   * @return counter
   */
  public int getCurrentHoursEpochCounter(long accessStartTime) {
    return (int) ((accessStartTime - epochStartTime) / EPOCH_DURATION_HOURS_SMH_TYPE);
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
    int hoursCounter = getHoursEpochCounterValue(ibesPtr);

    if (getCurrentSecondsEpochCounter(time) > secCounter
        || getCurrentMinutesEpochCounter(time) > minCounter
        || getCurrentHoursEpochCounter(time) > hoursCounter) {
      setAccessStartTime(ibesPtr, time);
      return true;
    }
    return false;
  }

}
