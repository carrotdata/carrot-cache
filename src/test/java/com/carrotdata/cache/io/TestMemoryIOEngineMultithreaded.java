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

package com.carrotdata.cache.io;

import java.io.IOException;

import com.carrotdata.cache.util.CacheConfig;

public class TestMemoryIOEngineMultithreaded extends TestIOEngineMultithreadedBase {

  @Override
  public void setUp() throws IOException {
    super.setUp();
    this.numRecords = 1000000;
    this.numThreads = 4;
  }

  @Override
  protected IOEngine getIOEngine() throws IOException {
    int segmentSize = 16 * 1024 * 1024;
    long cacheSize = 200L * segmentSize;
    CacheConfig conf = CacheConfig.getInstance();
    conf.setCacheSegmentSize("default", segmentSize);
    conf.setCacheMaximumSize("default", cacheSize);
    conf.setCacheTLSSupported("default", true);
    this.engine = new MemoryIOEngine(conf);
    return this.engine;
  }

}
