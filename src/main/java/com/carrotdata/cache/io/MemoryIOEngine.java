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

import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotdata.cache.util.CacheConfig;
import com.carrotdata.cache.util.UnsafeAccess;

public class MemoryIOEngine extends IOEngine {

  /** Logger */
  private static final Logger LOG = LoggerFactory.getLogger(MemoryIOEngine.class);

  private AtomicInteger activeSaveTasks = new AtomicInteger(0);

  private int ioStoragePoolSize = 32;

  private BlockingQueue<Runnable> taskQueue;

  private ExecutorService unboundedThreadPool;

  private boolean isPersistent;
  
  public MemoryIOEngine(String cacheName) {
    super(cacheName);
    this.isPersistent = this.config.isSaveOnShutdown(cacheName);
  }

  public MemoryIOEngine(CacheConfig conf) {
    super(conf);
    this.isPersistent = this.config.isSaveOnShutdown(cacheName);
  }

  private void initIOPool() {
      this.ioStoragePoolSize = this.config.getIOStoragePoolSize(this.cacheName);
      int keepAliveTime = 60; // hard-coded
      // This is actually unbounded queue (LinkedBlockingQueue w/o parameters)
      // and bounded thread pool - only coreThreads is maximum, maximum number of threads is ignored
      taskQueue = new LinkedBlockingQueue<>(ioStoragePoolSize);
      unboundedThreadPool = new ThreadPoolExecutor(ioStoragePoolSize, Integer.MAX_VALUE,
          keepAliveTime, TimeUnit.SECONDS, taskQueue); 
  }
  
  private void shutdownIOPool() {
    unboundedThreadPool.shutdown();
    try {
      // Wait for all tasks to finish, timeout after 300 seconds
      // TODO: 300 sec
      if (!unboundedThreadPool.awaitTermination(300, TimeUnit.SECONDS)) {
        LOG.error("Timeout occurred! Forcing shutdown...");
        unboundedThreadPool.shutdownNow(); // Force shutdown if tasks are not finished
      }
    } catch (InterruptedException e) {
      unboundedThreadPool.shutdownNow();
    }
  }
  
  @Override
  protected int getInternal(int sid, long offset, int size, byte[] key, int keyOffset, int keySize,
      byte[] buffer, int bufOffset) {
    try {
      return this.memoryDataReader.read(this, key, keyOffset, keySize, sid, offset, size, buffer,
        bufOffset);
    } catch (IOException e) {
      // never happens
    }
    return NOT_FOUND;
  }

  @Override
  protected int getInternal(int sid, long offset, int size, byte[] key, int keyOffset, int keySize,
      ByteBuffer buffer) throws IOException {
    try {
      return this.memoryDataReader.read(this, key, keyOffset, keySize, sid, offset, size, buffer);
    } catch (IOException e) {
      // never happens
    }
    return NOT_FOUND;
  }

  @Override
  protected int getInternal(int sid, long offset, int size, long keyPtr, int keySize, byte[] buffer,
      int bufOffset) {
    try {
      return this.memoryDataReader.read(this, keyPtr, keySize, sid, offset, size, buffer,
        bufOffset);
    } catch (IOException e) {
      // never happens
    }
    return NOT_FOUND;
  }

  @Override
  protected int getInternal(int sid, long offset, int size, long keyPtr, int keySize,
      ByteBuffer buffer) throws IOException {
    try {
      return this.memoryDataReader.read(this, keyPtr, keySize, sid, offset, size, buffer);
    } catch (IOException e) {
      // never happens
    }
    return NOT_FOUND;
  }

  @Override
  public SegmentScanner getScanner(Segment s) throws IOException {
    return this.memoryDataReader.getSegmentScanner(this, s);
  }

  @Override
//  protected void saveInternal(Segment data) throws IOException {
//    data.seal();
//  }

  /**
   * IOEngine subclass can override this method
   * @param data data segment
   * @throws FileNotFoundException
   */
  protected void saveInternal(Segment data) throws IOException {
    if (!this.isPersistent) {
      data.seal();
      return;
    }
    // FIXME: async write
    int id = data.getId();
    RandomAccessFile file = null;
    try {
      activeSaveTasks.incrementAndGet();
      // WRITE_LOCK
      data.writeLock();
      if (data.isSealed()) {
        return;
      }
      file = getFileFor(id);
      if (file == null) {
        return;
      }
      data.writeUnlock();
      // WRITE_UNLOCK
      // Save to file without locking
      data.save(file);
      // LOCK AGAIN
      //FIXME: do weed lock?
      data.writeLock();
      data.seal();
    } catch (IOException e) {
      LOG.error("saveInternal segmentId=" + data.getId() + " s=" + data, e);
    } finally {
      data.writeUnlock();
      activeSaveTasks.decrementAndGet();
      if (file != null) {
        file.close();
      }
    }
  }

  /**
   * TODO: move to parent class
   * @param id
   * @return
   * @throws FileNotFoundException
   */
  RandomAccessFile getFileFor(int id) throws FileNotFoundException {
    // open
    Path p = getPathForDataSegment(id);
    if (Files.exists(p)) {
      return null;
    }
    RandomAccessFile file = new RandomAccessFile(p.toFile(), "rw");
    return file;
  }
  
  
  @Override
  public void disposeDataSegment(Segment data) {
    // TODO: is it a good idea to lock on file I/O?
    // TODO: make sure that we remove file before save to the same ID
    // That is the race condition
    // close and delete file
    try {
      data.writeLock();
      if (this.isPersistent) {
        Files.deleteIfExists(getPathForDataSegment(data.getId()));
      }
      super.disposeDataSegment(data);
    } catch (IOException e) {
      LOG.error("Error:", e);
    } finally {
      data.writeUnlock();
    }
  }
  
  @Override
  public void dispose() {
    // TDOD: some test use this method in a wrong way
    // find and fix them
    waitForIoStoragePool();
    super.dispose();
    if (this.isPersistent) {
      int count = 0;
      for (int i = 0; i < dataSegments.length; i++) {
        Segment s = dataSegments[i];
        if (s == null) continue;
        disposeDataSegment(s);
      }
      LOG.debug("Closed {} files", count);
    }
  }
  
  private void waitForIoStoragePool() {
    while (this.activeSaveTasks.get() > 0) {
      try {
        Thread.sleep(10);
      } catch (InterruptedException e) {
        // TODO Auto-generated catch block
      }
    }
  }
  
  @Override
  public void shutdown() {
    super.shutdown();
    waitForIoStoragePool();
    // should we close files? They are read only
  }
  
  @Override
  public void save(OutputStream os) throws IOException {
    waitForIoStoragePool();
    // Save in memory segments
    saveRAMSegments();
    super.save(os);
  }

  private void saveRAMSegments() throws IOException {
    for (int i = 0; i < ramBuffers.length; i++) {
      Segment s = ramBuffers[i];
      if (s == null) {
        continue;
      }
      Path p = getPathForDataSegment(s.getId());
      RandomAccessFile file = new RandomAccessFile(p.toFile(), "rw");
      s.save(file);
      s.seal();
      file.close();
    }
  }

  @Override
  public void load(InputStream is) throws IOException {
    super.load(is);
    loadSegments();
  }

  private void loadSegments() throws IOException {

    if (!this.isPersistent) {
      return;
    }
    initIOPool();

    //try (Stream<Path> list = Files.list(Paths.get(dataDir));) {
      File dir = new File(this.dataDir);
      String[] sfiles = dir.list();
      
      //Iterator<Path> it = list.iterator();
      //while (it.hasNext()) {
        for (int i =0; i < sfiles.length; i++) {
        //Path p = it.next();
        final File f = new File(dataDir, sfiles[i]);
        Runnable r = () -> {
          String fileName = f.getName();
          try {
            int sid = getSegmentIdFromFileName(fileName);
            Segment s = this.dataSegments[sid];
            FileInputStream fis = new FileInputStream(f);
            DataInputStream dis = new DataInputStream(fis);

            long size = dis.readLong();
            // We here do not have IOEngine reference yet
            // therefore we allocate memory directly
            // TODO: real data size
            long ptr = UnsafeAccess.mallocZeroed(s.size());
            int bufSize = (int) Math.min(16 << 20, size);
            byte[] buffer = new byte[bufSize];
            int read = 0;

            while (read < size) {
              int toRead = (int) Math.min(size - read, bufSize);
              dis.readFully(buffer, 0, toRead);
              UnsafeAccess.copy(buffer, 0, ptr + read, toRead);
              read += toRead;
            }
            dis.close();
            s.setAddress(ptr);
          } catch (IOException e) {
            LOG.error("File=" + fileName, e);
            // TODO: how to catch failure during load?
          }
        };
        unboundedThreadPool.submit(r);
      }
    //}
    shutdownIOPool();
  }
  
  private int getSegmentIdFromFileName(String name) {
    String s = name.substring(FILE_NAME.length());
    return Integer.parseInt(s);
  }
  
  /**
   * TODO: move to parent
   * Get file path for a data segment
   * @param id data segment id
   * @return path to a file
   */
  private Path getPathForDataSegment(int id) {
    return Paths.get(dataDir, getSegmentFileName(id));
  }
  
  @Override
  protected int getRangeInternal(int sid, long offset, int size, byte[] key, int keyOffset,
      int keySize, int rangeStart, int rangeSize, byte[] buffer, int bufOffset) throws IOException {
    try {
      return this.memoryDataReader.readValueRange(this, key, keyOffset, keySize, sid, offset, size,
        buffer, bufOffset, rangeStart, rangeSize);
    } catch (IOException e) {
      // never happens
    }
    return NOT_FOUND;
  }

  @Override
  protected int getRangeInternal(int sid, long offset, int size, long keyPtr, int keySize,
      int rangeStart, int rangeSize, byte[] buffer, int bufOffset) throws IOException {
    try {
      return this.memoryDataReader.readValueRange(this, keyPtr, keySize, sid, offset, size, buffer,
        bufOffset, rangeStart, rangeSize);
    } catch (IOException e) {
      // never happens
    }
    return NOT_FOUND;
  }

  @Override
  protected int getRangeInternal(int sid, long offset, int size, byte[] key, int keyOffset,
      int keySize, int rangeStart, int rangeSize, ByteBuffer buffer) throws IOException {
    try {
      return this.memoryDataReader.readValueRange(this, key, keyOffset, keySize, sid, offset, size,
        buffer, rangeStart, rangeSize);
    } catch (IOException e) {
      // never happens
    }
    return NOT_FOUND;
  }

  @Override
  protected int getRangeInternal(int sid, long offset, int size, long keyPtr, int keySize,
      int rangeStart, int rangeSize, ByteBuffer buffer) throws IOException {
    try {
      return this.memoryDataReader.readValueRange(this, keyPtr, keySize, sid, offset, size, buffer,
        rangeStart, rangeSize);
    } catch (IOException e) {
      // never happens
    }
    return NOT_FOUND;
  }

  @Override
  protected boolean isMemory() {
    return true;
  }
  
  
}
