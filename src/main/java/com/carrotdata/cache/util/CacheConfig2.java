package com.carrotdata.cache.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.LinkedHashMap;

import static com.carrotdata.cache.util.CacheConfigKey.*;

public class CacheConfig2 {

  public static final String DEFAULT_PROPERTY = "default_cache.cfg";

  public static void main(String[] args) {
    var cacheConfig = loadDefaultCache();
    for (var entry : cacheConfig.entrySet()) {
      System.out.println(entry.getKey() + "=" + entry.getValue());
    }
    System.out.println("\nSample get properties:");
    System.out.printf("CACHES_NAME_LIST_KEY: %s\n", cacheConfig.get(CACHES_NAME_LIST_KEY));
    System.out.printf("CACHES_TYPES_LIST_KEY: %s\n", cacheConfig.get(CACHES_TYPES_LIST_KEY));
    System.out.println("Done");
  }

  public static LinkedHashMap<CacheConfigKey, Object> loadDefaultCache() {
    var linkedProperties = new LinkedHashMap<CacheConfigKey, Object>();

    try (var input = CacheConfig2.class.getClassLoader().getResourceAsStream(DEFAULT_PROPERTY)) {
      assert input != null;
      try (var reader = new BufferedReader(new InputStreamReader(input))) {
        String line;
        while ((line = reader.readLine()) != null) {
          line = line.trim();
          if (!line.isEmpty() && !line.startsWith("#")) {
            int delimiterPos = line.indexOf('=');
            if (delimiterPos != -1) {
              var key = line.substring(0, delimiterPos).trim();
              var value = line.substring(delimiterPos + 1).trim();
              var configKey = getConfigKeyByKey(key);
              if (configKey != null) {
                linkedProperties.put(configKey, parseValue(value));
              }
            }
          }
        }
      }
    } catch (IOException ex) {
      ex.printStackTrace();
    }

    return linkedProperties;
  }

  private static CacheConfigKey getConfigKeyByKey(String key) {
    CacheConfigKey result = null;
    for (CacheConfigKey configKey : CacheConfigKey.values()) {
      if (configKey.getKey().equals(key)) {
        result = configKey;
        break;
      }
    }

    if (result != null) {
      return result;
    }

    System.err.printf("key: %s doesn't defined in the cache: %s%n", key, DEFAULT_PROPERTY);
    return null;
  }

  private static Object parseValue(String value) {
    // Try parsing the value as different types
    if (value == null || value.isEmpty()) {
      return value; // Return as string if it's empty or null
    }

    if ("true".equalsIgnoreCase(value) || "false".equalsIgnoreCase(value)) {
      return Boolean.parseBoolean(value);
    }

    try {
      return Double.parseDouble(value);
    } catch (NumberFormatException empty) {
      // Not a long
    }

    try {
      return Integer.parseInt(value);
    } catch (NumberFormatException empty) {
      // Not an integer
    }

    try {
      return Long.parseLong(value);
    } catch (NumberFormatException empty) {
      // Not a long
    }

    return value; // Return as string if no other type matches
  }
}
