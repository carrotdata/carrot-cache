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

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
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

public class FileIOEngine extends IOEngine {
  /** Logger */
  private static final Logger LOG = LoggerFactory.getLogger(FileIOEngine.class);
  /**
   * Maps data segment Id to a disk file Attention: System MUST provide support for reasonably large
   * number of open files Max = 64K
   */
  Map<Integer, RandomAccessFile> dataFiles = new ConcurrentHashMap<Integer, RandomAccessFile>();

  protected DataReader fileDataReader;

  private AtomicInteger activeSaveTasks = new AtomicInteger(0);

  private int ioStoragePoolSize = 32;

  private BlockingQueue<Runnable> taskQueue;

  private ExecutorService unboundedThreadPool;

  /**
   * Constructor
   * @param cacheName cache name
   */
  public FileIOEngine(String cacheName) {
    super(cacheName);
    initEngine();
  }

  /**
   * Constructor
   * @param conf test configuration
   */
  public FileIOEngine(CacheConfig conf) {
    super(conf);
    initEngine();
  }

  private void initEngine() {
    try {
      this.fileDataReader = this.config.getFileDataReader(this.cacheName);
      this.ioStoragePoolSize = this.config.getIOStoragePoolSize(this.cacheName);
      int keepAliveTime = 60; // hard-coded
      // This is actually unbounded queue (LinkedBlockingQueue w/o parameters)
      // and bounded thread pool - only coreThreads is maximum, maximum number of threads is ignored
      taskQueue = new LinkedBlockingQueue<>(ioStoragePoolSize);
      unboundedThreadPool = new ThreadPoolExecutor(ioStoragePoolSize, Integer.MAX_VALUE,
          keepAliveTime, TimeUnit.SECONDS, taskQueue);
    } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
      LOG.error("FATAL", e);
      throw new RuntimeException(e);
    }
  }

  /**
   * IOEngine subclass can override this method
   * @param data data segment
   * @throws FileNotFoundException
   */
  protected void saveInternal(Segment data) throws IOException {
    //FIXME: async write
    //Runnable r = () -> {
      int id = data.getId();
      try {
        // WRITE_LOCK
        data.writeLock();
        if (data.isSealed()) {
          return;
        }
        RandomAccessFile file = getFileFor(id);
        if (file != null) {
          return;
        }
        file = getOrCreateFileFor(id);
        data.writeUnlock();
        // WRITE_UNLOCK

        // Save to file without locking
        data.save(file);
        
        // LOCK AGAIN
        data.writeLock();
        // Release segment
        data.setMemory(false);
        // release memory buffer
        long ptr = data.getAddress();
        data.setAddress(0);
        data.seal();
        UnsafeAccess.free(ptr);
        
      } catch (IOException e) {
        LOG.error("saveInternal segmentId=" + data.getId() + " s=" + data, e);
      } finally {
        data.writeUnlock();
        activeSaveTasks.decrementAndGet();
      }
    //};
    //submitTask(r);
  }

  private void submitTask(Runnable r) {
    activeSaveTasks.incrementAndGet();
    unboundedThreadPool.submit(r);
  }

  @Override
  protected int getInternal(int sid, long offset, int size, byte[] key, int keyOffset, int keySize,
      byte[] buffer, int bufOffset) throws IOException {

    Segment s = getSegmentById(sid);
    if (s == null) {
      return NOT_FOUND;
    }

    if (s.isMemory()) {
      return this.memoryDataReader.read(this, key, keyOffset, keySize, sid, offset, size, buffer,
        bufOffset);
    } else {
      long start = System.nanoTime();
      int result = this.fileDataReader.read(this, key, keyOffset, keySize, sid, offset, size,
        buffer, bufOffset);
      long end = System.nanoTime();
      this.totalIOReadDuration.addAndGet(end - start);
      return result;
    }
  }

  @Override
  protected int getRangeInternal(int sid, long offset, int size, byte[] key, int keyOffset,
      int keySize, int rangeStart, int rangeSize, byte[] buffer, int bufOffset) throws IOException {

    Segment s = getSegmentById(sid);
    if (s == null) {
      return NOT_FOUND;
    }

    if (s.isMemory()) {
      return this.memoryDataReader.readValueRange(this, key, keyOffset, keySize, sid, offset, size,
        buffer, bufOffset, rangeStart, rangeSize);
    } else {
      long start = System.nanoTime();
      int result = this.fileDataReader.readValueRange(this, key, keyOffset, keySize, sid, offset,
        size, buffer, bufOffset, rangeStart, rangeSize);
      long end = System.nanoTime();
      this.totalIOReadDuration.addAndGet(end - start);
      return result;
    }
  }

  @Override
  protected int getInternal(int sid, long offset, int size, byte[] key, int keyOffset, int keySize,
      ByteBuffer buffer) throws IOException {
    Segment s = getSegmentById(sid);
    if (s == null) {
      return NOT_FOUND;
    }

    if (s.isMemory()) {
      return this.memoryDataReader.read(this, key, keyOffset, keySize, sid, offset, size, buffer);
    } else {
      long start = System.nanoTime();
      int result =
          this.fileDataReader.read(this, key, keyOffset, keySize, sid, offset, size, buffer);
      long end = System.nanoTime();
      this.totalIOReadDuration.addAndGet(end - start);
      return result;
    }
  }

  @Override
  protected int getRangeInternal(int sid, long offset, int size, byte[] key, int keyOffset,
      int keySize, int rangeStart, int rangeSize, ByteBuffer buffer) throws IOException {
    Segment s = getSegmentById(sid);
    if (s == null) {
      return NOT_FOUND;
    }

    if (s.isMemory()) {
      return this.memoryDataReader.readValueRange(this, key, keyOffset, keySize, sid, offset, size,
        buffer, rangeStart, rangeSize);
    } else {
      long start = System.nanoTime();
      int result = this.fileDataReader.readValueRange(this, key, keyOffset, keySize, sid, offset,
        size, buffer, rangeStart, rangeSize);
      long end = System.nanoTime();
      this.totalIOReadDuration.addAndGet(end - start);
      return result;
    }
  }

  @Override
  protected int getInternal(int sid, long offset, int size, long keyPtr, int keySize, byte[] buffer,
      int bufOffset) throws IOException {
    Segment s = getSegmentById(sid);
    if (s == null) {
      return NOT_FOUND;
    }

    if (s.isMemory()) {
      return this.memoryDataReader.read(this, keyPtr, keySize, sid, offset, size, buffer,
        bufOffset);
    } else {
      long start = System.nanoTime();
      int result =
          this.fileDataReader.read(this, keyPtr, keySize, sid, offset, size, buffer, bufOffset);
      long end = System.nanoTime();
      this.totalIOReadDuration.addAndGet(end - start);
      return result;
    }
  }

  @Override
  protected int getRangeInternal(int sid, long offset, int size, long keyPtr, int keySize,
      int rangeStart, int rangeSize, byte[] buffer, int bufOffset) throws IOException {
    Segment s = getSegmentById(sid);
    if (s == null) {
      return NOT_FOUND;
    }

    if (s.isMemory()) {
      return this.memoryDataReader.readValueRange(this, keyPtr, keySize, sid, offset, size, buffer,
        bufOffset, rangeStart, rangeSize);
    } else {
      long start = System.nanoTime();
      int result = this.fileDataReader.readValueRange(this, keyPtr, keySize, sid, offset, size,
        buffer, bufOffset, rangeStart, rangeSize);
      long end = System.nanoTime();
      this.totalIOReadDuration.addAndGet(end - start);
      return result;
    }
  }

  @Override
  protected int getInternal(int sid, long offset, int size, long keyPtr, int keySize,
      ByteBuffer buffer) throws IOException {
    Segment s = getSegmentById(sid);
    if (s == null) {
      return NOT_FOUND;
    }

    if (s.isMemory()) {
      return this.memoryDataReader.read(this, keyPtr, keySize, sid, offset, size, buffer);
    } else {
      long start = System.nanoTime();
      int result = this.fileDataReader.read(this, keyPtr, keySize, sid, offset, size, buffer);
      long end = System.nanoTime();
      this.totalIOReadDuration.addAndGet(end - start);
      return result;
    }
  }

  @Override
  protected int getRangeInternal(int sid, long offset, int size, long keyPtr, int keySize,
      int rangeStart, int rangeSize, ByteBuffer buffer) throws IOException {
    Segment s = getSegmentById(sid);
    if (s == null) {
      return NOT_FOUND;
    }

    if (s.isMemory()) {
      return this.memoryDataReader.readValueRange(this, keyPtr, keySize, sid, offset, size, buffer,
        rangeStart, rangeSize);
    } else {
      long start = System.nanoTime();
      int result = this.fileDataReader.readValueRange(this, keyPtr, keySize, sid, offset, size,
        buffer, rangeStart, rangeSize);
      long end = System.nanoTime();
      this.totalIOReadDuration.addAndGet(end - start);
      return result;
    }
  }

  OutputStream getOSFor(int id) throws FileNotFoundException {
    Path p = getPathForDataSegment(id);
    FileOutputStream fos = new FileOutputStream(p.toFile());
    return fos;
  }

  RandomAccessFile getOrCreateFileFor(int id) throws FileNotFoundException {
    RandomAccessFile file = dataFiles.get(id);
    if (file == null) {
      // open
      Path p = getPathForDataSegment(id);
      file = new RandomAccessFile(p.toFile(), "rw");
      dataFiles.put(id, file);
    }
    return file;
  }

  /**
   * Get file for by segment id
   * @param id segment id
   * @return file
   */
  public RandomAccessFile getFileFor(int id) {
    RandomAccessFile file = dataFiles.get(id);
    return file;
  }

  @Override
  public void disposeDataSegment(Segment data) {
    // TODO: is it a good idea to lock on file I/O?
    // TODO: make sure that we remove file before save to the same ID
    // That is the race condition
    // close and delete file
    RandomAccessFile f = dataFiles.get(data.getId());
    if (f != null) {
      try {
        data.writeLock();
        f.close();
        Files.deleteIfExists(getPathForDataSegment(data.getId()));
        dataFiles.remove(data.getId());
        super.disposeDataSegment(data);
      } catch (IOException e) {
        LOG.error("Error:", e);
      } finally {
        data.writeUnlock();
      }
    }
  }

  /**
   * Get file prefetch buffer size
   * @return prefetch buffer size
   */
  public int getFilePrefetchBufferSize() {
    return this.config.getFilePrefetchBufferSize(this.cacheName);
  }

  /**
   * Get file path for a data segment
   * @param id data segment id
   * @return path to a file
   */
  private Path getPathForDataSegment(int id) {
    return Paths.get(dataDir, getSegmentFileName(id));
  }

  private int getSegmentIdFromFileName(String name) {
    String s = name.substring(FILE_NAME.length());
    return Integer.parseInt(s);
  }

  @Override
  public SegmentScanner getScanner(Segment s) throws IOException {
    return this.fileDataReader.getSegmentScanner(this, s);
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
      s.setMemory(false);

    }
  }

  @Override
  public void load(InputStream is) throws IOException {
    super.load(is);
    loadSegments();
  }

  private void loadSegments() throws IOException {
    try (Stream<Path> list = Files.list(Paths.get(dataDir));) {
      Iterator<Path> it = list.iterator();
      while (it.hasNext()) {
        Path p = it.next();
        File f = p.toFile();
        String fileName = f.getName();
        int sid = getSegmentIdFromFileName(fileName);
        RandomAccessFile raf = new RandomAccessFile(f, "r");
        this.dataFiles.put(sid, raf);
      }
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
  public void dispose() {
    waitForIoStoragePool();
    long startTime = System.currentTimeMillis();
    super.dispose();
    int count = 0;
    int deleted = 0;
    for (Map.Entry<Integer, RandomAccessFile> entry : this.dataFiles.entrySet()) {
      try {
        int id = entry.getKey();
        Path p = getPathForDataSegment(id);
        if (Files.exists(p)) {
          long lastModifiedTime = Files.getLastModifiedTime(p).toMillis();
          entry.getValue().close();
          if (lastModifiedTime < startTime) {
            Files.delete(p);
            deleted++;
          }
          count++;
        }
      } catch (IOException e) {
        // swallow
        LOG.error("Error:", e);
      }
    }
    LOG.debug("Closed {} files, deleted {}", count, deleted);
  }

  @Override
  public void shutdown() {
    super.shutdown();
    waitForIoStoragePool();
    // should we close files? They are read only
  }

  @Override
  protected boolean isMemory() {
    return false;
  }
}
