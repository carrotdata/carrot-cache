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

package com.carrotdata.cache;

import java.io.IOException;

import org.junit.Before;

public class TestMemoryCacheMultithreadedStream extends TestCacheMultithreadedStreamBase {

  @Before
  public void setUp() throws IOException {
    this.numRecords = 1000000;
    this.numThreads = 4;
    this.scavDumpBelowRatio = 0.1;
    this.segmentSize = 4 * 1024 * 1024;
    // 4GB GB
    this.maxCacheSize = 1000L * this.segmentSize;
    this.memory = true;
  }

}