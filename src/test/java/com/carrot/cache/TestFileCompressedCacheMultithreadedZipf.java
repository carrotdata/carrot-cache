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
package com.carrot.cache;

import java.io.IOException;
import java.net.URISyntaxException;

public class TestFileCompressedCacheMultithreadedZipf extends TestOffheapCompressedCacheMultithreadedZipf {
  
  @Override
  public void setUp() throws IOException, URISyntaxException {
    super.setUp();
    this.offheap = false;
    this.numRecords = 1000000;
    this.numThreads = 4;
    this.minActiveRatio = 0.1;
    this.maxCacheSize = 100 * this.segmentSize;
  }
}
