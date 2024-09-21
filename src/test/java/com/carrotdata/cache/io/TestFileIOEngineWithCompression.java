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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.URISyntaxException;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotdata.cache.util.CacheConfig;

public class TestFileIOEngineWithCompression extends TestMemoryIOEngineWithCompression {

  private static final Logger LOG =
      LoggerFactory.getLogger(TestFileIOEngineWithCompression.class);
  
  @Override
  protected IOEngine getEngine(CacheConfig conf) {
    conf.setDataWriter(cacheName, CompressedBlockDataWriter.class.getName());
    conf.setMemoryDataReader(cacheName, CompressedBlockMemoryDataReader.class.getName());
    conf.setFileDataReader(cacheName, CompressedBlockFileDataReader.class.getName());
    return new FileIOEngine(conf);
  }

  @Override
  @Test
  public void testLoadSave() throws IOException, URISyntaxException {
    /* DEBUG */ LOG.info("testLoadSave");
    createEngine(4 * 1024 * 1024, 20 * 4 * 1024 * 1024, false, true, 100000);
    int loaded = loadMemoryEngine(engine);
    /* DEBUG */ LOG.info("loaded=" + loaded);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    this.engine.save(dos);
    this.engine.getMemoryIndex().save(dos);
    // Get current config, we will reuse it later because it keeps location of a dictionary folder
    CacheConfig config = CacheConfig.getInstance();
    // Dispose engine
    //engine.dispose();
    // Re-create new one (creates new root directory for cache)
    engine = getEngine(config);

    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    DataInputStream dis = new DataInputStream(bais);
    engine.load(dis);
    engine.getMemoryIndex().load(dis);
    verifyMemoryEngine(engine, loaded);
  }
}
