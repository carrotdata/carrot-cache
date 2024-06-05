/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
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
