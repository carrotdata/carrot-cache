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
package com.carrotdata.cache.compression;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotdata.cache.compression.CompressionCodec.Type;
import com.carrotdata.cache.util.CacheConfig;

public class CodecFactory {
  /* Logger */
  private static final Logger LOG = LoggerFactory.getLogger(CodecFactory.class);

  /* Factory instance */
  private static CodecFactory factory = new CodecFactory();

  /* Maps cache name to codec */
  private ConcurrentHashMap<String, CompressionCodec> codecs = new ConcurrentHashMap<>();

  private CodecFactory() {
  }

  public static CodecFactory getInstance() {
    return factory;
  }

  public void clear() {
    codecs.clear();
  }

  /**
   * Get compression codec for cache
   * @param cacheName cache name
   * @return codec
   */
  public CompressionCodec getCompressionCodecForCache(String cacheName) {
    CompressionCodec codec = codecs.get(cacheName);
    return codec;
  }

  /**
   * Initializes codec for cache (called by Cache instance)
   * @param cacheName cache name
   * @param is input stream to read codec data from
   * @throws IOException
   */
  public boolean initCompressionCodecForCache(String cacheName, InputStream is) throws IOException {
    // This method is called during cache instance initialization
    CacheConfig config = CacheConfig.getInstance();
    boolean enabled = config.isCacheCompressionEnabled(cacheName);
    if (!enabled) {
      return false;
    }
    boolean tls = config.isCacheTLSSupported(cacheName);
    if (!tls) {
      String msg = "Data compression requires thread-local-storage support enabled";
      LOG.error(msg);
      throw new IllegalArgumentException(msg);
    }
    String sType = config.getCacheCompressionCodecType(cacheName);
    Type type = getCodecType(sType);
    if (type == null) {
      String msg = String.format("Compression codec type '%s' is not supported", sType);
      LOG.error(msg);
      throw new IllegalArgumentException(msg);
    }
    CompressionCodec codec = type.newCodec();
    codec.init(cacheName);
    if (is != null) {
      codec.load(is);
    }
    codecs.put(cacheName, codec);
    return true;
  }

  public boolean saveCodecForCache(String cacheName, OutputStream os) throws IOException {
    CompressionCodec codec = codecs.get(cacheName);
    if (codec == null) {
      return false;
    }
    codec.save(os);
    return true;
  }

  private Type getCodecType(String sType) {
    if (sType.equalsIgnoreCase(Type.ZSTD.name())) {
      return Type.ZSTD;
    }
    return null;
  }
}
