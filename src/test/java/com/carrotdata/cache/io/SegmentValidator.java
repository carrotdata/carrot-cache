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

import java.io.FileInputStream;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotdata.cache.compression.CompressionCodec;
import com.carrotdata.cache.compression.zstd.ZstdCompressionCodec;
import com.carrotdata.cache.util.Utils;

public class SegmentValidator {
  private static final Logger LOG = LoggerFactory.getLogger(SegmentValidator.class);

  public static void main(String[] args) throws IOException {
    String fileName = "fault_segment.data";
    FileInputStream fis = new FileInputStream(fileName);
    Segment s = new Segment();
    s.load(fis);
    ZstdCompressionCodec codec = (ZstdCompressionCodec) CompressionCodec.Type.ZSTD.newCodec();
    codec.init("default");

    CompressedBlockMemorySegmentScanner scanner = new CompressedBlockMemorySegmentScanner(s, codec);
    int count = 0;
    while (scanner.hasNext()) {
      long keyPtr = scanner.keyAddress();
      int keySize = scanner.keyLength();
      int valSize = scanner.valueLength();

      String key = new String(Utils.toBytes(keyPtr, keySize));
      LOG.info(key + " ptr=" + keyPtr + " keySize=" + keySize + " valSize=" + valSize
          + " buffer offset key=" + (keyPtr - scanner.getBufferAddress()));
      count++;
      scanner.next();
    }
    scanner.close();
    LOG.info("seg num items={} count={}", s.getTotalItems(), count);

  }
}
