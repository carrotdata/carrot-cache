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
import java.util.Random;

import org.junit.Before;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestFileCacheGetRangeAPI extends TestMemoryCacheGetRangeAPI {
  private static final Logger LOG = LoggerFactory.getLogger(TestFileCacheGetRangeAPI.class);

  @Before
  public void setUp() throws IOException {
    this.memory = false;
    cache = createCache();
    this.numRecords = 100000;
    this.r = new Random();
    long seed = System.currentTimeMillis();
    r.setSeed(seed);
    LOG.info("r.seed={}", seed);
  }

}
