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
