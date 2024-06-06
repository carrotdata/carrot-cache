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
import java.net.URISyntaxException;
import java.util.List;

import com.carrotdata.cache.util.CacheConfig;
import com.carrotdata.cache.util.TestUtils;

public class TestOffheapIOEngineWithBatchCompression extends TestOffheapIOEngineWithCompression {

  @Override
  public void setUp() {
    super.setUp();
    this.maxValueSize = 500;
  }

  @Override
  protected IOEngine getEngine(CacheConfig conf) {
    conf.setDataWriter(cacheName, CompressedBlockBatchDataWriter.class.getName());
    conf.setMemoryDataReader(cacheName, CompressedBlockMemoryDataReader.class.getName());
    return new OffheapIOEngine(conf);
  }

  protected void prepareGithubData(int numRecords) throws URISyntaxException, IOException {
    this.numRecords = numRecords;
    keys = new byte[numRecords][];
    values = new byte[numRecords][];
    mKeys = new long[numRecords];
    mValues = new long[numRecords];
    expires = new long[numRecords];
    String key = "testkey";
    List<String> dataList = loadGithubData();
    String[] sdata = new String[dataList.size()];
    dataList.toArray(sdata);
    // Random r = new Random();
    for (int i = 0; i < numRecords; i++) {
      keys[i] = (key + i).getBytes();
      int off = i % sdata.length;

      values[i] = sdata[off].getBytes();
      mKeys[i] = TestUtils.copyToMemory(keys[i]);
      mValues[i] = TestUtils.copyToMemory(values[i]);
      expires[i] = getExpire(i); // To make sure that we have distinct expiration values
    }
  }
}
