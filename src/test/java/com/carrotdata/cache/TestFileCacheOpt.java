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

import com.carrotdata.cache.io.BaseBatchDataWriterOpt;
import com.carrotdata.cache.io.BaseFileDataReaderOpt;
import com.carrotdata.cache.io.BaseMemoryDataReaderOpt;

public class TestFileCacheOpt extends TestFileCache {

  @Before
  public void setUp() throws IOException {
    super.setUp();
    this.dataWriter = BaseBatchDataWriterOpt.class.getName();
    this.dataReaderMemory = BaseMemoryDataReaderOpt.class.getName();
    this.dataReaderFile = BaseFileDataReaderOpt.class.getName();

  }
}
