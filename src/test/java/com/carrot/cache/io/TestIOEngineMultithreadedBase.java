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

package com.carrot.cache.io;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.junit.After;
import org.junit.Before;

public abstract class TestIOEngineMultithreadedBase extends TestIOMultithreadedBase{

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
    throws IOException
  {
    return engine.put(keyPtr, keySize, valuePtr, valueSize, expire);
  }

  @Override
  protected boolean delete(byte[] key, int off, int len) throws IOException{
    return engine.delete(key, off, len);
  }

  @Override
  protected boolean delete(long keyPtr, int keySize) throws IOException{
    return engine.delete(keyPtr, keySize);
  }

  @Override
  protected long get(byte[] key, int off, int len, byte[] buffer, int bufferOffset) throws IOException {
    return engine.get(key, off, len, true, buffer, bufferOffset);
  }

  @Override
  protected long get(long keyPtr, int keySize, ByteBuffer buffer) throws IOException {
    return engine.get(keyPtr, keySize, true, buffer);
  }
}
