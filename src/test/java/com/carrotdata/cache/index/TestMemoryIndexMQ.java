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
