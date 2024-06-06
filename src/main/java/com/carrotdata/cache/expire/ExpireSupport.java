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
package com.carrotdata.cache.expire;

import com.carrotdata.cache.util.Utils;

public interface ExpireSupport {
  /* Expiration field size - 2 bytes, by default */
  public final static int FIELD_SIZE = Utils.SIZEOF_SHORT;

  /* One epoch duration for seconds type SM in ms */
  public static long EPOCH_DURATION_SEC_SM_TYPE = (long) (1 << 14) * 1000;

  /* One epoch duration for minutes type SM in ms */
  public static long EPOCH_DURATION_MIN_SM_TYPE = (long) (1 << 14) * 1000 * 60;

  /* One epoch duration for seconds type SMH in ms */
  public static long EPOCH_DURATION_SEC_SMH_TYPE = (long) (1 << 13) * 1000;

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
   * Get meta section size Format meta section: 8 bytes - current start access time 2 bytes - epoch
   * seconds counter 2 bytes - epoch minutes counter 2 bytes - epoch hours counter (optional)
   **/
  public int getExpireMetaSectionSize();

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
  public void updateMeta(long ibesPtr);

  /**
   * Begin index block
   * @param ibesPtr index block expire section address
   * @param force forces scan block scan
   * @return true, if full block scan is required, false - otherwise
   */
  public boolean begin(long ibesPtr, boolean force);

  /**
   * Get expire field size in bytes
   * @return field size
   */
  public default int getFieldSize() {
    return FIELD_SIZE;
  }

  /**
   * End index block
   * @param ibesPtr index block expire section address
   */
  public void end(long ibesPtr);

  /**
   * Return current scan time
   * @param ibesPtr index block expire section address
   * @return scan start time if in progress or 0
   */
  public long getAccessStartTime(long ibesPtr);

  /**
   * Sets scan time start
   * @param ibesPtr index block expire section address
   * @param time
   */
  public void setAccessStartTime(long ibesPtr, long time);

}
