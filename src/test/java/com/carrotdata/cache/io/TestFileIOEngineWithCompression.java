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

import com.carrotdata.cache.util.CacheConfig;

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
