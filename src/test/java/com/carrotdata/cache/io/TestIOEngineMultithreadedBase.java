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

package com.carrotdata.cache.io;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.After;
import org.junit.Before;

public abstract class TestIOEngineMultithreadedBase extends TestIOMultithreadedBase {

  protected IOEngine engine;

  @Before
  public void setUp() throws IOException {
    this.engine = getIOEngine();
  }

  @After
  public void tearDown() {
    // UnsafeAccess.mallocStats.printStats(false);
    this.engine.dispose();
  }

  protected abstract IOEngine getIOEngine() throws IOException;

  @Override
  protected final boolean put(byte[] key, byte[] value, long expire) throws IOException {
    return engine.put(key, value, expire);
  }

  @Override
  protected boolean put(long keyPtr, int keySize, long valuePtr, int valueSize, long expire)
      throws IOException {
    return engine.put(keyPtr, keySize, valuePtr, valueSize, expire);
  }

  @Override
  protected boolean delete(byte[] key, int off, int len) throws IOException {
    return engine.delete(key, off, len);
  }

  @Override
  protected boolean delete(long keyPtr, int keySize) throws IOException {
    return engine.delete(keyPtr, keySize);
  }

  @Override
  protected long get(byte[] key, int off, int len, byte[] buffer, int bufferOffset)
      throws IOException {
    return engine.get(key, off, len, true, buffer, bufferOffset);
  }

  @Override
  protected long get(long keyPtr, int keySize, ByteBuffer buffer) throws IOException {
    return engine.get(keyPtr, keySize, true, buffer);
  }
}
