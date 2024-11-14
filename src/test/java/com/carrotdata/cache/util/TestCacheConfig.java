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
package com.carrotdata.cache.util;

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Properties;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestCacheConfig {
  private static final Logger LOG = LoggerFactory.getLogger(TestCacheConfig.class);

  @Test
  public void testCacheConfig() throws URISyntaxException, IOException {

    CacheConfig conf = CacheConfig.getInstance("./src/test/resources/conf/test.conf");
    Properties props = conf.getProperties();
    assertEquals(72, props.size());
    assertEquals(1073741824L, conf.getCacheMaximumSize("cache"));
    assertEquals(1073741999L, conf.getCacheMaximumSize("cache1"));
    assertEquals(4194304L, conf.getCacheSegmentSize("cache1"));
    assertEquals(24, conf.getStartIndexNumberOfSlotsPower("cache"));
    props.forEach((x, y) -> LOG.info(x + "=" + y));
  }
}
