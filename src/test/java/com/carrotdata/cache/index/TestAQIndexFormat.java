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
