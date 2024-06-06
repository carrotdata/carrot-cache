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

import com.carrotdata.cache.util.CacheConfig;

public class TestFileIOEngineMultithreaded extends TestIOEngineMultithreadedBase {

  @Override
  public void setUp() throws IOException {
    super.setUp();
    this.numRecords = 100000;
    this.numThreads = 4;
  }

  @Override
  protected IOEngine getIOEngine() throws IOException {
    int segmentSize = 4 * 1024 * 1024;
    long cacheSize = 100 * segmentSize;
    CacheConfig conf = CacheConfig.getInstance();
    conf.setCacheSegmentSize("default", segmentSize);
    conf.setCacheMaximumSize("default", cacheSize);
    this.engine = new FileIOEngine(conf);
    return this.engine;
  }

}
