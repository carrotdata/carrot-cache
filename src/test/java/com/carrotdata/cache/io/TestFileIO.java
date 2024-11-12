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

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.concurrent.ThreadLocalRandom;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotdata.cache.util.UnsafeAccess;


public class TestFileIO {

  private static final Logger LOG = LoggerFactory.getLogger(TestFileIO.class);

  public static void main(String[] args) throws InterruptedException, IOException {
    
    int numThreads = 8;
    final String path = "/Users/vrodionov/Development/carrotdata/data/temp_250g_file";
    final int blockSize = 4096;
    final int numIterations = 100000;
    
    prepareFile(path, 250L * 1024 * 1024 * 1024);
    
    Runnable r = () -> {
      RandomAccessFile f = null;
      try {
        
        f = new RandomAccessFile(path, "rw");
        long length = f.length();
        int count = 0;
        byte[] buffer = new byte[blockSize];
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        while(count++ < numIterations) {
          
          long offset = rnd.nextLong(length / blockSize) * blockSize;
          f.seek(offset);
          f.readFully(buffer);
          
          if ((count % 10000) == 0) {
            LOG.info("count={}", count);
          }
        }
      } catch (IOException e) {
        e.printStackTrace();
      } finally {
        try {
          f.close();
        } catch (IOException e) {
          // TODO Auto-generated catch block
          e.printStackTrace();
        }
      }
    };
    long start = System.currentTimeMillis();
    Thread[] workers = new Thread[numThreads];
    
    for (int i = 0; i < numThreads; i++) {
      workers[i] = new Thread(r);
      workers[i].start();
    }
    
    for (int i = 0; i < numThreads; i++) {
      workers[i].join();
    }
    
    long end = System.currentTimeMillis();
    LOG.info("Finished {} reads, block size={}, threads={} in {} ms, RPS={}", numThreads, blockSize, 
      numIterations, end - start, (double) numThreads * numIterations * 1000/(end - start));
    
    long ptr = UnsafeAccess.mallocZeroed(30L << 30);
    LOG.info("Created mega memory user");
    System.in.read();

  }

  private static void prepareFile(String path, long l) throws IOException {
    RandomAccessFile f = new RandomAccessFile(path, "rw");
    byte[] buffer = new byte[1 << 20];
    ThreadLocalRandom r = ThreadLocalRandom.current();
    r.nextBytes(buffer);
    
    int count = (int) (l / buffer.length);
    long totalWritten = 0;
    for (int i = 0; i < count; i++) {
      f.write(buffer);
      totalWritten += buffer.length;
      if ((totalWritten % (1 << 30)) == 0) {
        LOG.info("written={} pos={}", totalWritten, f.getFilePointer());
        
      }
    }
    f.close();
  }
  
  
}
