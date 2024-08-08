package com.carrotdata.cache.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.LinkedHashMap;
import java.util.regex.Pattern;

import static com.carrotdata.cache.util.CacheConfigKey.*;

public class CacheConfig2 {

  public static final String DEFAULT_PROPERTY = "default_cache.cfg";
  private static final Pattern LONG_PATTERN = Pattern.compile("^-?\\d+[Ll]$");
  private static final Pattern NUMERIC_PATTERN = Pattern.compile("^\\d+$");
  private static final Pattern DOUBLE_PATTERN = Pattern.compile("^-?\\d+(\\.\\d+)?$");

  public static void main(String[] args) {
    var cacheConfig = loadDefaultCache();
    for (var entry : cacheConfig.entrySet()) {
      System.out.println(
          entry.getKey().getKey() + "=" + entry.getValue() + " (" + entry.getValue().getClass()
              .getSimpleName() + ")");
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

    if (isValidLong(value)) {
      return Long.parseLong(value.substring(0, value.length() - 1));
    }

    if (isValidInteger(value)) {
      return Integer.parseInt(value);
    }

    if (isValidDouble(value)) {
      return Double.parseDouble(value);
    }

    return value; // Return as string if no other type matches
  }

  private static boolean isValidInteger(String value) {
    if (!NUMERIC_PATTERN.matcher(value).matches()) {
      return false;
    }
    try {
      var longValue = Long.parseLong(value);
      return longValue >= Integer.MIN_VALUE && longValue <= Integer.MAX_VALUE;
    } catch (NumberFormatException e) {
      return false;
    }
  }

  private static boolean isValidDouble(String value) {
    return DOUBLE_PATTERN.matcher(value).matches();
  }

  private static boolean isValidLong(String value) {
    if (!LONG_PATTERN.matcher(value).matches()) {
      return false;
    }

    try {
      Long.parseLong(value.substring(0, value.length() - 1));
      return true;
    } catch (NumberFormatException e) {
      return false;
    }
  }
}
