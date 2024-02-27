package com.onecache.core.util;

import static org.junit.Assert.*;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Properties;

import org.junit.Test;

public class TestCacheConfig {

  
  @Test
  public void testCacheConfig() throws URISyntaxException, IOException {
    
    CacheConfig conf = CacheConfig.getInstance("./src/test/resources/conf/test.conf");
    Properties props = conf.getProperties();
    assertEquals(68, props.size());
    props.forEach((x,y) -> System.out.println(x + "=" + y));
  }
}
