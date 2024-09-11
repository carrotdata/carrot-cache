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
    props.forEach((x, y) -> LOG.info(x + "=" + y));
  }
}
