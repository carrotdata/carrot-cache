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

import static org.junit.Assert.assertEquals;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotdata.cache.index.MemoryIndex.Type;

public class TestMemoryIndexMQ extends TestMemoryIndexFormatBase {
  private static final Logger LOG = LoggerFactory.getLogger(TestMemoryIndexMQ.class);

  @Override
  protected MemoryIndex getMemoryIndex() {
    return new MemoryIndex("default", Type.MQ);
  }

  public static void main(String[] args) {
    
    TestMemoryIndexMQ test = new TestMemoryIndexMQ();
    int n = 1_000_000;
    boolean prepared = false;
    LOG.info("Read access with hit = false");
    for(int i = 0; i < 200; i++) {
      test.setUp();
      if (!prepared) {
        test.prepareData(n);
        prepared = true;
      }
      long t1 = System.currentTimeMillis();
      int loaded = test.loadIndexMemory();
      long t2 = System.currentTimeMillis();
      long size = test.memoryIndex.size();
      assertEquals(loaded, (int) size);
      test.verifyIndexMemory(false, loaded);
      long t3 = System.currentTimeMillis();
      test.verifyIndexMemoryFound(false, loaded);
      long t4 = System.currentTimeMillis();
      test.verifyIndexMemory(true, loaded);
      long t5 = System.currentTimeMillis();
      test.verifyIndexMemoryFound(true, loaded);
      long t6 = System.currentTimeMillis();
      
      LOG.info("load time={}, hit=false: time={} fast time={}, hit=true: time={} fast time={}", t2 - t1, t3 - t2, t4 - t3, t5 - t4, t6 - t5);
      test.memoryIndex.dispose();;
    }
    
  }
  
}
