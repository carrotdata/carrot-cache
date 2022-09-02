/**
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
package com.carrot.cache.io;

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
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.carrot.cache.util.CacheConfig;
import com.carrot.cache.util.UnsafeAccess;

public class FileIOEngine extends IOEngine {
  /** Logger */
  private static final Logger LOG = LogManager.getLogger(FileIOEngine.class);
  /**
   * Maps data segment Id to a disk file Attention: System MUST provide support for reasonably large
   * number of open files Max = 64K
   */
  Map<Integer, RandomAccessFile> dataFiles = new HashMap<Integer, RandomAccessFile>();

  protected DataReader fileDataReader;

  /** We use this buffer pool to avoid unnecessary large memory allocations */
  protected ConcurrentLinkedQueue<Long> bufferPool = new ConcurrentLinkedQueue<Long>();

  /**
   * Constructor
   *
   * @param cacheName cache name
   */
  public FileIOEngine(String cacheName) {
    super(cacheName);
    try {
      this.fileDataReader = this.config.getFileDataReader(this.cacheName);
      this.fileDataReader.init(this.cacheName);
    } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
      LOG.fatal(e);
      throw new RuntimeException(e);
    }
  }

  /**
   * Constructor
   *
   * @param conf test configuration
   */
  public FileIOEngine(CacheConfig conf) {
    super(conf);
    try {
      this.fileDataReader = this.config.getFileDataReader(this.cacheName);
      this.fileDataReader.init(this.cacheName);
    } catch (ClassNotFoundException | InstantiationException | IllegalAccessException e) {
      LOG.fatal(e);
      throw new RuntimeException(e);
    }
  }

  /**
   * IOEngine subclass can override this method
   *
   * @param data data segment
   * @throws FileNotFoundException
   */
  protected void saveInternal(Segment data) throws IOException {
    Runnable r =
        () -> {
          int id = data.getId();
          try {
            // First save segment data to file
            RandomAccessFile file = getOrCreateFileFor(id);
            // Save to file
            data.save(file);
            // Release segment
            data.writeLock();
            data.setOffheap(false);
            // release memory buffer
            long ptr = data.getAddress();
            clearMemory(ptr);
            this.bufferPool.offer(ptr);
            data.setAddress(0);
            data.seal();

          } catch (IOException e) {
            LOG.error("saveInternal segmentId=" + data.getId() + " s=" + data, e);
          } finally {
            data.writeUnlock();
          }
        };
    new Thread(r).start();
  }

  private void clearMemory(long ptr) {
    UnsafeAccess.setMemory(ptr, this.segmentSize, (byte) 0);
  }

  @Override
  protected Segment getRAMSegmentByRank(int rank) {
    Segment s = this.ramBuffers[rank];
    if (s == null) {
      synchronized (this.ramBuffers) {
        s = this.ramBuffers[rank];
        if (s != null) {
          return s;
        }
        int id = getAvailableId();
        if (id < 0) {
          return null;
        }
        if (this.dataSegments[id] == null) {
          Long ptr = this.bufferPool.poll();
          if (ptr == null) {
            ptr = UnsafeAccess.mallocZeroed(this.segmentSize);
          }
          s = Segment.newSegment(ptr, (int) this.segmentSize, id, rank);
          s.init(this.cacheName);
          // Set data appender

          s.setDataWriter(this.dataWriter);
          this.dataSegments[id] = s;
          reportAllocation(this.segmentSize);
          // LOG.error("created "+ id + " s=" + s);
        } else {
          // TODO: is it normal path of an execution?
          // FIXME: check Scavenger
          s = this.dataSegments[id];
          s.reuse(id, rank, System.currentTimeMillis());
        }
        this.ramBuffers[rank] = s;
      }
    }
    return s;
  }

  @Override
  protected int getInternal(
      int sid,
      long offset,
      int size,
      byte[] key,
      int keyOffset,
      int keySize,
      byte[] buffer,
      int bufOffset)
      throws IOException {
    return this.fileDataReader.read(
        this, key, keyOffset, keySize, sid, offset, size, buffer, bufOffset);
  }

  @Override
  protected int getInternal(
      int sid, long offset, int size, byte[] key, int keyOffset, int keySize, ByteBuffer buffer)
      throws IOException {
    return this.fileDataReader.read(this, key, keyOffset, keySize, sid, offset, size, buffer);
  }

  @Override
  protected int getInternal(
      int sid, long offset, int size, long keyPtr, int keySize, byte[] buffer, int bufOffset)
      throws IOException {
    return this.fileDataReader.read(this, keyPtr, keySize, sid, offset, size, buffer, bufOffset);
  }

  @Override
  protected int getInternal(
      int sid, long offset, int size, long keyPtr, int keySize, ByteBuffer buffer)
      throws IOException {
    return this.fileDataReader.read(this, keyPtr, keySize, sid, offset, size, buffer);
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
   *
   * @param id segment id
   * @return file
   * @throws FileNotFoundException
   */
  public RandomAccessFile getFileFor(int id) {
    RandomAccessFile file = dataFiles.get(id);
    return file;
  }

  @Override
  public synchronized void disposeDataSegment(Segment data) {
    // TODO: is it a good idea to lock on file I/O?
    // TODO: make sure that we remove file before save to the same ID
    // That is the race condition
    // close and delete file
    RandomAccessFile f = dataFiles.get(data.getId());
    if (f != null) {
      try {
        f.close();
        Files.deleteIfExists(getPathForDataSegment(data.getId()));
        // *DEBUG*/LOG.error("delete id=" + data.getId() + " s=" + data);
        dataFiles.remove(data.getId());
        super.disposeDataSegment(data);
      } catch (IOException e) {
        // TODO
        e.printStackTrace();
      }
    }
  }

  /**
   * Get file prefetch buffer size
   *
   * @return prefetch buffer size
   */
  public int getFilePrefetchBufferSize() {
    return this.config.getFilePrefetchBufferSize(this.cacheName);
  }
  /**
   * Get file path for a data segment
   *
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
    super.save(os);
  }

  @Override
  public void load(InputStream is) throws IOException {
    super.load(is);
    loadSegments();
  }

  private void loadSegments() throws IOException {
    try (Stream<Path> list = Files.list(Paths.get(dataDir)); ) {
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
}
