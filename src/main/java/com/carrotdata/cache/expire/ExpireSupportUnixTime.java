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

import com.carrotdata.cache.util.UnsafeAccess;
import com.carrotdata.cache.util.Utils;
/**
 * 
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
    UnsafeAccess.putInt( expireFieldPtr, (int) expire);
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
