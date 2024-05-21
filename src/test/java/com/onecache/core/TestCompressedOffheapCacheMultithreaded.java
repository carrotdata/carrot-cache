/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.onecache.core;

import java.io.IOException;
import java.net.URISyntaxException;

import org.junit.Before;

public class TestCompressedOffheapCacheMultithreaded extends TestCompressedCacheMultithreadedBase {

  @Before
  public void setUp() throws IOException, URISyntaxException{
    super.setUp();
    this.numRecords = 1_000_000;
    this.numThreads = 8;
    this.offheap = true;
    this.segmentSize = 64_000_000;
    this.maxCacheSize = 1000L * this.segmentSize;
    this.cache = createCache();
  }
}
