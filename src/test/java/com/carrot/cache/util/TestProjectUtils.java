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

import org.junit.Test;

public class TestProjectUtils {
  
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
}
