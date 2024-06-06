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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotdata.cache.index.MemoryIndex.Type;

public abstract class StressTestMemoryIndexMQ extends TestMemoryIndexFormatBase {
  private static final Logger LOG = LoggerFactory.getLogger(StressTestMemoryIndexMQ.class);

  @Before
  public void setUp() {
  }

  @After
  public void tearDown() {
    //
  }

  private void setup() {
    super.setUp();
  }

  private void teardown() {
    super.tearDown();
  }

  @Test
  public void stressTest() {
    for (int i = 0; i < 100; i++) {
      LOG.info("\nRun #{}", i);
      setup();
      testLoadReadNoRehashBytes();
      teardown();

      setup();
      testLoadReadNoRehashMemory();
      teardown();

      setup();
      testLoadReadNoRehashBytesWithHit();
      teardown();

      setup();
      testLoadReadNoRehashMemoryWithHit();
      teardown();

      setup();
      testLoadReadDeleteNoRehashBytes();
      teardown();

      setup();
      testLoadReadDeleteNoRehashMemory();
      teardown();

      setup();
      testLoadReadWithRehashBytes();
      teardown();

      setup();
      testLoadReadWithRehashMemory();
      teardown();

      setup();
      testLoadReadWithRehashBytesWithHit();
      teardown();

      setup();
      testLoadReadWithRehashMemoryWithHit();
      teardown();

      setup();
      testLoadReadDeleteWithRehashBytes();
      teardown();

      setup();
      testLoadReadDeleteWithRehashMemory();
      teardown();

      // setup();
      // testEvictionBytes();
      // teardown();
      //
      // setup();
      // testEvictionMemory();
      // teardown();

    }
  }

  @Override
  protected MemoryIndex getMemoryIndex() {
    return new MemoryIndex("default", Type.MQ);
  }
}
