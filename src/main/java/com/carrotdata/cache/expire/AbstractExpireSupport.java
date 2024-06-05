package com.carrotdata.cache.expire;

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

import com.carrotdata.cache.util.UnsafeAccess;
import com.carrotdata.cache.util.Utils;

public abstract class AbstractExpireSupport implements ExpireSupport {

  public int metaSectionSize;

  public int fieldSize;

  public AbstractExpireSupport() {
    this.metaSectionSize = Utils.SIZEOF_LONG;
    this.fieldSize = Utils.SIZEOF_INT;
  }

  public final int getExpireMetaSectionSize() {
    return this.metaSectionSize;
  }

  public final int getFieldSize() {
    return this.fieldSize;
  }

  /**
   * End index block
   * @param ibesPtr index block expire section address
   */
  public void end(long ibesPtr) {
    updateMeta(ibesPtr);
    setAccessStartTime(ibesPtr, 0);
  }

  /**
   * Return current scan time
   * @param ibesPtr index block expire section address
   * @return scan start time if in progress or 0
   */
  public long getAccessStartTime(long ibesPtr) {
    return UnsafeAccess.toLong(ibesPtr);
  }

  /**
   * Sets scan time start
   * @param ibesPtr index block expire section address
   * @param time
   */
  public void setAccessStartTime(long ibesPtr, long time) {
    UnsafeAccess.putLong(ibesPtr, time);
  }

  /**
   * Get seconds epoch counter value
   * @param ibesPtr index block expiration section address
   * @return seconds epoch counter value
   */
  public int getSecondsEpochCounterValue(long ibesPtr) {
    final int off = Utils.SIZEOF_LONG;
    return UnsafeAccess.toShort(ibesPtr + off) & 0xffff;
  }

  /**
   * Get minutes epoch counter value
   * @param ibesPtr index block expiration section address
   * @return minutes epoch counter value
   */
  public int getMinutesEpochCounterValue(long ibesPtr) {
    final int off = Utils.SIZEOF_LONG + Utils.SIZEOF_SHORT;
    return UnsafeAccess.toShort(ibesPtr + off) & 0xffff;
  }

  /**
   * Get hours epoch counter value
   * @param ibesPtr index block expiration section address
   * @return minutes epoch counter value
   */
  public int getHoursEpochCounterValue(long ibesPtr) {
    final int off = Utils.SIZEOF_LONG + 2 * Utils.SIZEOF_SHORT;
    return UnsafeAccess.toShort(ibesPtr + off) & 0xffff;
  }

  /**
   * Set seconds epoch counter value
   * @param ibesPtr index block expiration section address
   * @param v value
   */
  public void setSecondsEpochCounterValue(long ibesPtr, int v) {
    final int off = Utils.SIZEOF_LONG;
    UnsafeAccess.putShort(ibesPtr + off, (short) v);
  }

  /**
   * Set minutes epoch counter value
   * @param ibesPtr index block expiration section address
   * @param v value
   */
  public void setMinutesEpochCounterValue(long ibesPtr, int v) {
    final int off = Utils.SIZEOF_LONG + Utils.SIZEOF_SHORT;
    UnsafeAccess.putShort(ibesPtr + off, (short) v);
  }

  /**
   * Set hours epoch counter value
   * @param ibesPtr index block expiration section address
   * @param v value
   */
  public void setHoursEpochCounterValue(long ibesPtr, int v) {
    final int off = Utils.SIZEOF_LONG + 2 * Utils.SIZEOF_SHORT;
    UnsafeAccess.putShort(ibesPtr + off, (short) v);
  }

  /**
   * Get value of a lowest 15 bits
   * @param v value
   * @return lowest 15 bits value
   */
  public short low15(short v) {
    return (short) (v & 0x7fff);
  }

  /**
   * Get value of a lowest 14 bits
   * @param v value
   * @return lowest 14 bits value
   */
  public short low14(short v) {
    return (short) (v & 0x3fff);
  }

  /*
   * First bit value
   * @param v value return highest 1 bit value (0 or 1)
   */
  public short high1(short v) {
    return (short) ((v >> 15) & 1);
  }

  /**
   * First 2-bits value
   * @param v value
   * @return highest 2 bits value (0 - 3)
   */
  public short high2(short v) {
    return (short) ((v >> 14) & 3);
  }

  /**
   * Sets highest bit to 0 (for seconds)
   * @param v original value
   * @return with highest bit set to 0
   */
  public short sec1(short v) {
    return v;
  }

  /**
   * Sets highest 2 bits to 0 (for seconds)
   * @param v original value
   * @return with highest 2 bits set to 00
   */
  public short sec2(short v) {
    return v;
  }

  /**
   * Sets highest bit to 1 (for minutes)
   * @param v original value
   * @return with highest bit set to 1
   */
  public short min1(short v) {
    return (short) (v | 0x8000);
  }

  /**
   * Sets highest 2 bits to 01 (for minutes)
   * @param v original value
   * @return with highest 2 bits set to 01
   */
  public short min2(short v) {
    return (short) (v | 0x4000);
  }

  /**
   * Sets highest 2 bit to 11 (for hours)
   * @param v original value
   * @return with highest bit set to 11
   */
  public short hours2(short v) {
    return (short) (v | 0xc000);
  }
}
