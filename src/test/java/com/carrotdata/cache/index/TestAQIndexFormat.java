/*
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
package com.carrotdata.cache.index;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.carrotdata.cache.util.TestUtils;
import com.carrotdata.cache.util.UnsafeAccess;
import com.carrotdata.cache.util.Utils;

/**
 * Admission Queue index format tests
 */
public class TestAQIndexFormat {

  @Test
  public void testIndexWriteByteArrays() {

    IndexFormat ifrmt = new AQIndexFormat();
    long buf = UnsafeAccess.malloc(ifrmt.indexEntrySize());
    byte[] key = TestUtils.randomBytes(16);
    byte[] value = TestUtils.randomBytes(16);
    long hash = Utils.hash64(key, 0, key.length);

    ifrmt.writeIndex(0L, buf, key, 0, key.length, value, 0, value.length, (short) 0 /* sid */,
      0 /* data offset */, 0 /* data size */, 0 /* expire */);
    assertTrue(ifrmt.equals(buf, hash));
    UnsafeAccess.free(buf);

  }

  @Test
  public void testIndexWriteMemory() {

    IndexFormat ifrmt = new AQIndexFormat();
    long buf = UnsafeAccess.malloc(ifrmt.indexEntrySize());
    long key = TestUtils.randomMemory(16);
    long value = TestUtils.randomMemory(16);
    long hash = Utils.hash64(key, 16);

    ifrmt.writeIndex(0L, buf, key, 16, value, 16, (short) 0 /* sid */, 0 /* data offset */,
      0 /* data size */, 0 /* expire */);
    assertTrue(ifrmt.equals(buf, hash));
    UnsafeAccess.free(buf);
    UnsafeAccess.free(key);
    UnsafeAccess.free(value);
  }

}
