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

package com.carrotdata.cache;

import java.io.IOException;

import org.junit.Before;

public class TestFileCacheMultithreadedStream extends TestCacheMultithreadedStreamBase {

  @Before
  public void setUp() throws IOException {
    this.numRecords = 1000000;
    this.numThreads = 4;
    this.scavDumpBelowRatio = 0.1;
    this.segmentSize = 4 * 1024 * 1024;
    this.maxCacheSize = 1000L * this.segmentSize;
    this.memory = false;
  }

}
