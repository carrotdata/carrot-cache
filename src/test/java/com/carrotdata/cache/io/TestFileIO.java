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
