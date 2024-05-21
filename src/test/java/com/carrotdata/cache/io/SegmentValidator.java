/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
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
    ZstdCompressionCodec codec = (ZstdCompressionCodec)CompressionCodec.Type.ZSTD.newCodec();
    codec.init("default");
    
    CompressedBlockMemorySegmentScanner scanner = new CompressedBlockMemorySegmentScanner(s, codec);
    int count = 0;
    while (scanner.hasNext()) {
      long keyPtr = scanner.keyAddress();
      int keySize = scanner.keyLength();
      int valSize = scanner.valueLength();
      
      String key = new String(Utils.toBytes(keyPtr, keySize));
      LOG.info(key + " ptr=" + keyPtr + " keySize=" + keySize + " valSize=" + valSize + 
        " buffer offset key=" + (keyPtr - scanner.getBufferAddress()));
      count++;
      scanner.next();
    }
    scanner.close();
    LOG.info("seg num items={} count={}", s.getTotalItems(), count);
    
    
  }
}
