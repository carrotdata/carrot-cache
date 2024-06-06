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

import com.carrotdata.cache.util.UnsafeAccess;
import com.carrotdata.cache.util.Utils;

/**
 * This is 4 byte field, which keeps seconds since 01/01/1970
 */
public final class ExpireSupportUnixTime extends AbstractExpireSupport {

  public ExpireSupportUnixTime() {
    this.fieldSize = Utils.SIZEOF_INT;
    this.metaSectionSize = 0;
  }

  @Override
  public void end(long ibesPtr) {
    // Do nothing
  }

  @Override
  public long getExpire(long ibesPtr, long expireFieldPtr) {
    int t = UnsafeAccess.toInt(expireFieldPtr);
    return (((long) t) & 0xffffffffL) * 1000;
  }

  @Override
  public void setExpire(long ibesPtr, long expireFieldPtr, long expire) {
    expire = expire / 1000;
    UnsafeAccess.putInt(expireFieldPtr, (int) expire);
  }

  @Override
  public void updateMeta(long ibesPtr) {
    // do nothing
  }

  @Override
  public boolean begin(long ibesPtr, boolean force) {
    // TODO Auto-generated method stub
    return false;
  }

}
