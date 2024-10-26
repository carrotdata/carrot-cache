package com.carrotdata.cache.io;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.AsynchronousFileChannel;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TestFileIOAsync {

  private static final Logger LOG = LoggerFactory.getLogger(TestFileIOAsync.class);

  private static class FileReadTask implements Callable<Future<Integer>>
  {
    
    AsynchronousFileChannel channel;
    
    long offset;
    
    ByteBuffer buffer;
    
    Future<Integer> future;
    
    private FileReadTask(AsynchronousFileChannel channel, long offset, ByteBuffer buf) {
      this.channel = channel;
      this.offset = offset;
      this.buffer = buf;
    }
    
    @Override
    public Future<Integer> call() throws Exception {
      future = channel.read(buffer, offset);
      return future;
    }
    
    private ByteBuffer getBuffer() {
      this.buffer.clear();
      return this.buffer;
    }
    
    private boolean isDone() throws InterruptedException, ExecutionException {
      return future.isDone();
    }
  }
  
  private static class BusyWaitLoopTask implements Callable<Future<Integer>> {

    private long micros;
    
    private BusyWaitLoopTask(long microseconds) {
      this.micros = microseconds;
    }
    
    @Override
    public Future<Integer> call() throws Exception {
      final long wait = micros * 1000;
      long start = System.nanoTime();
      while(System.nanoTime() - start < wait) {
        Thread.onSpinWait();
      }
//      Future<Integer> f = new Future<Integer>() {
//
//        @Override
//        public boolean cancel(boolean mayInterruptIfRunning) {
//          return false;
//        }
//
//        @Override
//        public boolean isCancelled() {
//          return false;
//        }
//
//        @Override
//        public boolean isDone() {
//          return true;
//        }
//
//        @Override
//        public Integer get() throws InterruptedException, ExecutionException {
//          return 1;
//        }
//
//        @Override
//        public Integer get(long timeout, TimeUnit unit)
//            throws InterruptedException, ExecutionException, TimeoutException {
//          return 1;
//        }
//      };
      return null;
    }
    
  }
  
  public static void main(String[] args) throws InterruptedException, IOException {
    
    int numThreads = 8;
    final String path = "/Users/vrodionov/Development/carrotdata/data/temp_250g_file";
    final int blockSize = 1024 * 1024;
    final int numIterations = 10000;
    final int ioQueueSize = 8;
    final double ratio = 1.d;
    final long waitTime = 2;
    //prepareFile(path, 250L * 1024 * 1024 * 1024);
    
    Runnable r = () -> {
      AsynchronousFileChannel f = null;
      try {
        
        f = AsynchronousFileChannel.open(Path.of(path));
        
        long length = f.size();
        int count = 0;
        int fired = 0;
        long max = ioQueueSize * numIterations;
        List<FileReadTask> pendingQueue = new ArrayList<FileReadTask>(ioQueueSize);
        List<ByteBuffer> buffers = new ArrayList<ByteBuffer>(ioQueueSize);
        
        for (int i = 0; i < ioQueueSize; i++) {
          buffers.add(ByteBuffer.allocateDirect(blockSize));
        }
        
        ThreadLocalRandom rnd = ThreadLocalRandom.current();
        
        List<FileReadTask> finished = new ArrayList<FileReadTask>();
        BusyWaitLoopTask btask = new BusyWaitLoopTask(waitTime);

        while(count < max) {
          
          long offset = rnd.nextLong(length / blockSize) * blockSize;
          int val = fired == max? 0: ioQueueSize - 1;
          while(pendingQueue.size() > val) {
            for (FileReadTask task: pendingQueue) {
              if (task.isDone()) {
                count++;
                if ((count % 100000) == 0) {
                  LOG.info("count={}, pending={} fired={}", count, pendingQueue.size(), fired);
                }
                buffers.add(task.getBuffer());
                finished.add(task);
              }
            }
            pendingQueue.removeAll(finished);
            finished.clear();
          }
          
          if (fired < max) {
            double d = rnd.nextDouble();
            if (d < ratio) {
              FileReadTask task = new FileReadTask(f, offset, buffers.remove(0));
              Future<Integer> fut = task.call();
              pendingQueue.add(task);
              
            } else {
              btask.call();
              count++;
              if ((count % 100000) == 0) {
                LOG.info("count={}, pending={} fired={}", count, pendingQueue.size(), fired);
              }
            }
            fired++;
          }        
        }
      } catch (IOException e) {
        e.printStackTrace();
      } catch (Exception e) {
        // TODO Auto-generated catch block
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
    LOG.info("Finished {} reads, block size={}, threads={}  queue size={} in {} ms, RPS={}", numThreads * ioQueueSize * numIterations, blockSize, 
      numThreads, ioQueueSize, end - start, (double) numThreads * ioQueueSize * numIterations * 1000/(end - start));

  }

  @SuppressWarnings("unused")
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
