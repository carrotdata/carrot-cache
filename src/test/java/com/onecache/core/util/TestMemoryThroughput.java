package com.onecache.core.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import org.junit.Test;
import static org.junit.Assert.assertTrue;

public abstract class TestMemoryThroughput {
    
  static class MemoryBuffer {
    long ptr;
    long size;
    AtomicLong writeOffset = new AtomicLong(0);
    volatile long safeReadLimit;
  }
  
  static ThreadLocal<long[]> dataList = new ThreadLocal<long[]>();
  
  long[] data;
  
  final int cacheLine = 64;
  
  final int kvSize = 16 * cacheLine;
  
  final int kvTotal = 1_000_000;
  
  final int numIterations = 10_000_000;
  
  final int numThreads = 8;
  
  volatile long memory;

  AtomicLong offset = new AtomicLong(0);
  
  volatile long safeReadLimit = 0;
  
  AtomicInteger currentIndex = new AtomicInteger(0);  
 
  // For async memory buffer initialization test
  ConcurrentLinkedQueue<MemoryBuffer> memoryBuffers = new ConcurrentLinkedQueue<MemoryBuffer>();
  
  int maxQueueSize = 2;
    
  long bufferSize = 10 << 30; // 6 GB
  
  volatile MemoryBuffer memoryBuffer;
  
  int maxBuffers = 1;
  
  Thread producerThread;
  
  void produceBuffers() {
    int currentBuffer = 1;
    long t1 = System.currentTimeMillis();
    while(currentBuffer <= maxBuffers) {
      long ptr = UnsafeAccess.mallocZeroed(bufferSize);
      MemoryBuffer buffer = new MemoryBuffer();
      buffer.ptr = ptr;
      buffer.size = bufferSize;
      memoryBuffers.add(buffer);
      currentBuffer++;
      while (memoryBuffers.size() >= maxQueueSize) {
        Thread.onSpinWait();
      }
    }
    long t2 = System.currentTimeMillis();
    System.out.printf("Producer finished in %dms\n", (t2 - t1));
  }
  
  void writeBuffers() {
    
    long ptr = 0;
    MemoryBuffer localBuffer = memoryBuffer;
    ArrayList<MemoryBuffer> consumed = new ArrayList<MemoryBuffer>();
    long localPtr = 0;
    ThreadLocalRandom r = ThreadLocalRandom.current();
    
    for (;;) {
      localBuffer = memoryBuffer;
      if (localBuffer == null) {
        break;
      }
      localPtr = localBuffer.ptr;
      long off = localBuffer.writeOffset.getAndAdd(kvSize);
      // Check end of buffer condition
      if (off >= bufferSize) {
        // Finished current buffer but must wait until next one
        // wait till memory pointer changes
        while(localBuffer == memoryBuffer) {
          LockSupport.parkNanos(10000);
          //Thread.onSpinWait();
        }
        if (memoryBuffer == null) {
          break;
        }
        
        localBuffer = memoryBuffer;
        localPtr = localBuffer.ptr;
        // TODO check if we off the limit
        off = localBuffer.writeOffset.getAndAdd(kvSize);
        if (off >= bufferSize || off + kvSize >= bufferSize) {
          localBuffer.writeOffset.getAndAdd(-kvSize);
          continue;
        }
      } else if (off < bufferSize && off + kvSize >= bufferSize) {
        // This thread must update memory pointer
        // Finished current buffer but must wait until next one
        // Wait for read offset
        while(localBuffer.safeReadLimit != off) {
          Thread.onSpinWait();
        };
        
        // Get next buffer
        MemoryBuffer nextMemory = null;
        while ((nextMemory = memoryBuffers.poll()) == null && producerThread.isAlive()) {
          LockSupport.parkNanos(10000);          
          //Thread.onSpinWait();
        };
        if (nextMemory == null) {
          memoryBuffer = null;
          break;
        }
        // Do not free - add memory to consumed list to avoid reuse
        consumed.add(localBuffer);
        // Set memory pointer
        memoryBuffer = nextMemory;
        // Update local pointer and write offset
        localBuffer = nextMemory;
        localPtr = localBuffer.ptr;
        off = localBuffer.writeOffset.getAndAdd(kvSize);
        if (off >= bufferSize || off + kvSize >= bufferSize) {
          localBuffer.writeOffset.getAndAdd(-kvSize);
          continue;
        }
      }
      ptr = data[r.nextInt(kvTotal)];
      UnsafeAccess.copy(ptr, localPtr + off, kvSize);
      // Flash SOB - notify cache
      // This is probably not necessary
      UnsafeAccess.storeFence();
      // Advance safe read limit  
      while(localBuffer.safeReadLimit != off) {
        Thread.onSpinWait();
      };
      localBuffer.safeReadLimit += kvSize;
    }
    System.out.printf("Writer %d finished last memory=%d\n", Thread.currentThread().getId(), localPtr);
  }
  
  @Test
  public void testWritersProducer() throws InterruptedException, IOException {
    prepareData();
    long tt1 = System.nanoTime();
    memory = UnsafeAccess.mallocZeroed(bufferSize);
    long tt3 = System.nanoTime();
    System.out.println("Malloc pre-touch main buffer time=" + (tt3 - tt1));
    memoryBuffer =new MemoryBuffer();
    memoryBuffer.ptr = memory;
    memoryBuffer.size = bufferSize;
    
    //System.out.printf("Press any button ...");
    //System.in.read();
    
    // Start buffer producer
    Runnable producer = () -> produceBuffers();
    producerThread = new Thread(producer);
    producerThread.setPriority(Thread.NORM_PRIORITY + 1);
    producerThread.start();
    
    // Start writers
    Runnable writer = () -> writeBuffers();
    Thread[] workers = new Thread[numThreads];
    for(int i = 0; i < numThreads; i++) {
      workers[i] = new Thread(writer);
      workers[i].start();
    }
    // Wait till finish
    long t1 = System.currentTimeMillis();
    for(int i = 0; i < numThreads; i++) {
      workers[i].join();
    }
    long t2 = System.currentTimeMillis();
    System.out.printf("Treads=%d, copied %d bytes in %dms throughput=%d MB/s\n", numThreads, (long) maxBuffers * bufferSize,
      t2 - t1, (long) maxBuffers * bufferSize/ (1000L * (t2 - t1)));
  }
  
  void prepareDataPerThread(){
    long[] dataList = new long[kvTotal];
    for (int i = 0; i < kvTotal; i++) {
      long ptr = TestUtils.randomMemory((int)kvSize);
      dataList[i] = ptr;
    }
    TestMemoryThroughput.dataList.set(dataList);
  }
  
  void prepareData(){
    long t1 = System.currentTimeMillis();
    System.out.printf("Preparing data ...");
    if (data == null) {
      data = new long[kvTotal];
      for (int i = 0; i < kvTotal; i++) {
        long ptr = TestUtils.randomMemory((int)kvSize);
        data[i] = ptr;
      }
    } else {
      shuffleArray(data);
    }
    long t2 = System.currentTimeMillis();
    System.out.printf("Done in %d\n", t2 - t1);
  }
  
  private void testCopy() {
    prepareDataPerThread();

    long[] list = dataList.get();
    long off = 0;
    final long dst = memory;
    long t1 = System.currentTimeMillis();

    for (int i = 0; i < kvTotal; i++) {
      long ptr = list[i];
      off = offset.getAndAdd(kvSize);
      UnsafeAccess.copy(ptr, dst + off, kvSize);
      UnsafeAccess.storeFence();
      off += kvSize;
    }
    long t2 = System.currentTimeMillis();
    
    System.out.printf("Copied %d datas in %dms throughput=%d MB/s\n", kvTotal, t2 - t1, ((long) numIterations * kvSize)/ (1000L * (t2-t1)));
  }
  
  private void testCopyAndRead() {

    final long dst = memory;
    long t1 = System.currentTimeMillis();
    ThreadLocalRandom r = ThreadLocalRandom.current();
    for (;;) {
      int index = currentIndex.getAndIncrement();
      if (index >= numIterations) {
        break;
      }
      long ptr = data[index % kvTotal];
      long off = (long) index * kvSize;
      UnsafeAccess.copy(ptr, dst + off, kvSize);
      // Flash SOB - notify cache
      // This is probably not necessary
      //UnsafeAccess.storeFence();
      // Advance safe read limit      
      while(safeReadLimit != off);
      safeReadLimit += kvSize;
//      // Read back previous and compare
//      if (index < 1) {
//        continue;
//      }
//      // Get random index in [0. index - 1]
//      index = r.nextInt(index);
//      off = (long) kvSize * index;
//      // Flush LOB, process all invalidation requests
//      //UnsafeAccess.loadFence();
//      ptr = data[index % kvTotal];
//      assertTrue(Utils.compareTo(ptr, kvSize, dst + off, kvSize) == 0);
    }
    long t2 = System.currentTimeMillis();
    
    System.out.printf("Copied  and read %d datas in %dms throughput=%d MB/s\n", 
      kvTotal, t2 - t1, (kvTotal * kvSize)/ (1000L * (t2-t1)));
  }

  @Test
  public void testCopyMultithreaded() throws InterruptedException {
    
    long tt1 = System.nanoTime();
    memory = UnsafeAccess.malloc((long) numThreads * kvTotal * kvSize);
    //memory = memory / cacheLine * cacheLine + cacheLine;
    long tt3 = System.nanoTime();
    System.out.println("Malloc pre-touch main buffer time=" + (tt3 - tt1));

    Runnable r = () -> testCopy();
    Thread[] workers = new Thread[numThreads];
    for(int i = 0; i < numThreads; i++) {
      workers[i] = new Thread(r);
      workers[i].start();
    }
    
    for(int i = 0; i < numThreads; i++) {
      workers[i].join();
    }
  }
  
  @Test
  public void testCopyAndReadMultithreaded() throws InterruptedException {
    
    long tt1 = System.nanoTime();
    memory = UnsafeAccess.mallocZeroed((long) numIterations * kvSize);
    //memory = memory / cacheLine * cacheLine + cacheLine;
    long tt3 = System.nanoTime();
    System.out.println("Malloc pre-touch main buffer time=" + (tt3 - tt1));

    Runnable r = () -> testCopyAndRead();
    Thread[] workers = new Thread[numThreads];
    int numIterations = 100;
    int iteration = 0;
    while (iteration++ < numIterations) { 
      System.out.println("Preparing data");
      prepareData();
      System.out.println("Complete");

      System.out.println("\n*************** ITERATION=" + iteration + " ***************\n");
      for(int i = 0; i < numThreads; i++) {
        workers[i] = new Thread(r);
        workers[i].start();
      }
    
      for(int i = 0; i < numThreads; i++) {
        workers[i].join();
      }
      safeReadLimit = 0;
      currentIndex.set(0);
    }
  }
  
  // Implementing Fisher–Yates shuffle
  void shuffleArray(long[] arr)
  {
    ThreadLocalRandom rnd = ThreadLocalRandom.current();
    for (int i = arr.length - 1; i > 0; i--)
    {
      int index = rnd.nextInt(i + 1);
      // Simple swap
      long a = arr[index];
      arr[index] = arr[i];
      arr[i] = a;
    }
  }
}
