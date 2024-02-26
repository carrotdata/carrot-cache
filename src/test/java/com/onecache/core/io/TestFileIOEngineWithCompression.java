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
package com.onecache.core.io;

import java.io.IOException;
import java.net.URISyntaxException;

import com.onecache.core.util.CacheConfig;

public class TestFileIOEngineWithCompression extends TestOffheapIOEngineWithCompression {

  @Override
  protected IOEngine getEngine(CacheConfig conf) {
    conf.setDataWriter(cacheName, CompressedBlockDataWriter.class.getName());
    conf.setMemoryDataReader(cacheName, CompressedBlockMemoryDataReader.class.getName());
    conf.setFileDataReader(cacheName, CompressedBlockFileDataReader.class.getName());
    return new FileIOEngine(conf);
  }
  
  @Override 
  public void testLoadSave() throws IOException, URISyntaxException {
    super.testLoadSave();
  } 
}
