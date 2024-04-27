package com.onecache.core.util;

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
    assertEquals(68, props.size());
    props.forEach((x,y) -> LOG.info(x + "=" + y));
  }
}
