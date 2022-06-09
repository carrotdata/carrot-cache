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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Objects;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.carrot.cache.Cache;
import com.carrot.cache.index.IndexFormat;
import com.carrot.cache.index.MemoryIndex;
import com.carrot.cache.util.CacheConfig;
import com.carrot.cache.util.Persistent;
import com.carrot.cache.util.UnsafeAccess;
import com.carrot.cache.util.Utils;

/**
 * IOEngine base class
 *
 * <p>This class is responsible for storing and retrieving cached data, and keeping statistics
 * information for all data segments. All data is stored in segments Each segment has a size which
 * is configurable Total number of segments is defined by a maximum cache size and segment size.
 * Data can be stored in RAM (off-heap) and on disk (SSD)
 */
public abstract class IOEngine implements Persistent {

  /** Logger */
  private static final Logger LOG = LogManager.getLogger(IOEngine.class);
  
  protected final static String FILE_NAME = "cache_";

  protected static int NOT_FOUND = -1;
  
  public static enum IOEngineEvent {
    DATA_SIZE_CHANGED; // size of a data changed
  }

  public static interface Listener {
    public void onEvent(IOEngine e, IOEngineEvent evt);
  }

  /* Parent cache */
  protected final Cache parent;

  /* Parent cache name */
  protected final String cacheName;

  /* Segment size (bytes)*/
  protected final long segmentSize;

  /* Number of segments */
  protected final int numSegments;

  /* Cache configuration */
  protected final CacheConfig config;

  /* IOEngine listener */
  protected Listener aListener;

  /* maximum allowed storage size */
  protected long maxStorageSize;

  /*
   * RAM buffers accumulates incoming PUT's before submitting them to an IOEngine
   * There are 'slru.cache.segments' of segments in RAM buffers
   */

  protected Segment[] ramBuffers;

  /* RAM buffers locks */
  protected ReentrantReadWriteLock[] rbLocks;

  /* Keeps tracks of all segments*/
  protected Segment[] dataSegments;

  /* Memory index */
  protected MemoryIndex index;

  /* Cached data directory name */
  protected String dataDir;
  
  /**
   * Initialize engine for a given cache
   * @param cacheName cache name
   * @param config cache configuration
   * @return new engine
   */
  public static IOEngine getEngineForCache(Cache cache) {
    //TODO: Check NULL on return
    String cacheName = cache.getName();
    CacheConfig config = cache.getCacheConfig();
    String[] caches = config.getCacheNames();
    String[] types = config.getCacheTypes();
    if (caches == null || types == null || caches.length != types.length) {
      throw new RuntimeException("Cache misconfiguration");
    }
    for (int i = 0; i < caches.length; i++) {
      if (caches[i].equals(cacheName)) {
        return engineFor(types[i], cache);
      }
    }
    return null;
  }
  
  /**
   * Get engine for cache
   * @param type cache type
   * @param cache cache itself
   * @return engine
   */
  protected static IOEngine engineFor(String type, Cache cache) {
    if (type.equals("offheap")) {
      return new OffheapIOEngine(cache);
    } else if (type.equals("file")) {
      return new FileIOEngine(cache);
    }
    return null;
  }

  /**
   * Constructor
   *
   * @param numSegments number of segments
   * @param segmentSize segment size
   */
  public IOEngine(Cache parent) {
    this.parent = parent;
    this.cacheName = this.parent.getName();
    this.config = this.parent.getCacheConfig();
    this.segmentSize = this.config.getCacheSegmentSize(this.cacheName);
    this.maxStorageSize = this.config.getCacheMaximumSize(this.cacheName);
    this.numSegments = (int) (this.maxStorageSize / this.segmentSize + 1);
    int num = this.config.getNumberOfRanks(this.cacheName);
    ramBuffers = new Segment[num];
    this.dataSegments = new Segment[this.numSegments];
    this.index = new MemoryIndex(this.parent, MemoryIndex.Type.MQ);
    this.dataDir = this.config.getDataDir(this.cacheName);

    initLocks();
  }

  private void initLocks() {
    this.rbLocks = new ReentrantReadWriteLock[ramBuffers.length];
    for (int i = 0; i < this.rbLocks.length; i++) {
      rbLocks[i] = new ReentrantReadWriteLock();
    }
  }

  /**
   * Enables - disables eviction
   *
   * @param b
   */
  public void setEvictionEnabled(boolean b) {
    this.index.setEvictionEnabled(b);
  }

  /**
   * Is eviction enabled
   *
   * @return true or false
   */
  public boolean isEvictionEnabled() {
    return this.index.isEvictionEnabled();
  }
  /**
   * Sets engine listener
   *
   * @param al listener
   */
  public void setListener(Listener al) {
    this.aListener = al;
  }

  /**
   * Get engine listener
   *
   * @return engine listener
   */
  public Listener getListener() {
    return this.aListener;
  }

  /**
   * Get memory index
   *
   * @return memory index
   */
  public MemoryIndex getMemoryIndex() {
    return this.index;
  }

  /**
   * Get segment default size
   *
   * @return segment default size
   */
  public long getSegmentSize() {
    return this.segmentSize;
  }

  /**
   * Get number of segments
   *
   * @return number of segments
   */
  public int getNumberOfSegments() {
    return this.numSegments;
  }

  /**
   * Get number of ranks
   * @return number of ranks
   */
  public int getNumberOfRanks() {
    return ramBuffers.length;
  }
  
  /**
   * Get segment by segment id
   * @param sid segment id
   * @return segment
   */
  public Segment getSegmentById(int sid) {
    //TODO: checkId(sid);
    return this.dataSegments[sid];
  }
  
  /**
   * TODO: test
   * Converts popularity value to a the rank
   *
   * @param p popularity (0, 1.0)
   * @return rank
   */
  public int popularityToRank(double p) {
    //TODO: what is MAX : 0 or ?
    p = p * ramBuffers.length;
    return (int) Math.floor(p);
  }

  /**
   * Get key-value into a given byte buffer
   *
   * @param keyPtr key address
   * @param keySize size of a key
   * @param buffer buffer
   * @param bufOffset buffer offset
   * @return length of an item or -1
   * @throws IOException
   */
  public long get(long keyPtr, int keySize, byte[] buffer, int bufOffset) throws IOException {
    // TODO: locking
    IndexFormat format = this.index.getIndexFormat();
    // TODO: embedded entry case
    int bSize = format.indexEntrySize();
    long buf = UnsafeAccess.mallocZeroed(bSize);
    boolean dataEmbedded = this.config.isIndexDataEmbeddedSupported();
    int slot = 0;
    try {
      // Lock index for the key (slot)
      slot =  this.index.readLock(keyPtr, keySize);
      long result = index.find(keyPtr, keySize, true, buf, bSize);
      if (result < 0) {
        return NOT_FOUND;
      } else if (result > bSize) {
        UnsafeAccess.free(buf);
        bSize = (int) result;
        buf = UnsafeAccess.mallocZeroed(bSize);
        index.find(keyPtr, keySize, true, buf, bSize);
      }
      // First 4 bytes is a cached item size
      // This call returns TOTAL size: key + value + kSize + vSize
      int keyValueSize = format.getKeyValueSize(buf);
      // TODO: actually, not correct
      if (keyValueSize > buffer.length - bufOffset) {
        return keyValueSize;
      }
      dataEmbedded = dataEmbedded && (keyValueSize < this.config.getIndexDataEmbeddedSize());
      if (dataEmbedded) {
        // Return embedded data
        int off = format.getEmbeddedOffset();
        int kSize = Utils.readUVInt(buf + off);
        if (kSize != keySize) {
          return NOT_FOUND;
        }
        int kSizeSize = Utils.sizeUVInt(kSize);
        off += kSizeSize;
        int vSize = Utils.readUVInt(buf + off);
        int vSizeSize = Utils.sizeUVInt(vSize);
        off += vSizeSize;
        if (Utils.compareTo(keyPtr, keySize, buf + off, kSize) != 0) {
          return NOT_FOUND;
        }
        off -= kSizeSize + vSizeSize;
        // Copy data to buffer
        UnsafeAccess.copy(buf + off, buffer, bufOffset, keyValueSize);
        return keyValueSize;
      } else {
        // Offset in a segment - 4 bytes
        long offset = format.getOffset(buf);
        // Segment id
        int sid = (int) format.getSegmentId(buf);
        boolean res = get(sid, offset, keyValueSize, buffer, bufOffset);
        if (!res) {
          return NOT_FOUND;
        }
        // Now buffer contains both: key and value, we need to compare keys
        // Format of a key-value pair in a buffer: key-size, value-size, key, value
        int kSize = Utils.readUVInt(buffer, bufOffset);
        if (kSize != keySize) {
          return NOT_FOUND;
        }
        int kSizeSize = Utils.sizeUVInt(kSize);
        bufOffset += kSizeSize;
        int vSize = Utils.readUVInt(buffer, bufOffset);
        int vSizeSize = Utils.sizeUVInt(vSize);
        bufOffset += vSizeSize;

        // Now compare keys
        if (Utils.compareTo(buffer, bufOffset, keySize, keyPtr, keySize) == 0) {
          // If key is the same
          return keyValueSize;
        } else {
          return NOT_FOUND;
        }
      }
    } finally {
      UnsafeAccess.free(buf);
      this.index.readUnlock(slot);
    }
  }

  /**
   * Get item into a given byte buffer
   *
   * @param key key buffer
   * @param keyOffset offset
   * @param keySize size of a key
   * @param buffer buffer
   * @param bufOffset buffer offset
   * @return length of an item or -1
   * @throws IOException
   */
  public long get(byte[] key, int keyOffset, int keySize, byte[] buffer, int bufOffset)
      throws IOException {

    // TODO: locking
    IndexFormat format = this.index.getIndexFormat();
    // TODO: embedded entry case
    int bSize = format.indexEntrySize();
    long buf = UnsafeAccess.mallocZeroed(bSize);
    boolean dataEmbedded = this.config.isIndexDataEmbeddedSupported();
    int slot = 0;
    try {
      // Lock index for the key (slot)
      //TODO: remove locks?
      slot = this.index.readLock(key, keyOffset, keySize);

      long result = index.find(key, keyOffset, keySize, true, buf, bSize);
      if (result < 0) {
        return NOT_FOUND;
      } else if (result > bSize) {
        
        UnsafeAccess.free(buf);
        bSize = (int) result;
        buf = UnsafeAccess.mallocZeroed(bSize);
        index.find(key, keyOffset, keySize, true, buf, bSize);
      }
      // First 4 bytes is a cached item size
      // This call returns TOTAL size: key + value + kSize + vSize
      int keyValueSize = format.getKeyValueSize(buf);
      // TODO: actually, not correct
      if (keyValueSize > buffer.length - bufOffset) {
        return keyValueSize;
      }
      dataEmbedded = dataEmbedded && (keyValueSize < this.config.getIndexDataEmbeddedSize());
      if (dataEmbedded) {
        // Return embedded data
        int off = format.getEmbeddedOffset();
        int kSize = Utils.readUVInt(buf + off);
        if (kSize != keySize) {
          return NOT_FOUND;
        }
        int kSizeSize = Utils.sizeUVInt(kSize);
        off += kSizeSize;
        int vSize = Utils.readUVInt(buf + off);
        int vSizeSize = Utils.sizeUVInt(vSize);
        off += vSizeSize;
        if (Utils.compareTo(key, keyOffset, keySize, buf + off, kSize) != 0) {
          return NOT_FOUND;
        }
        off -= kSizeSize + vSizeSize;
        // Copy data to buffer
        UnsafeAccess.copy(buf + off, buffer, bufOffset, keyValueSize);
        return keyValueSize;
      } else {
        // Offset in a segment - 4 bytes
        long offset = format.getOffset(buf);
        // Segment id
        int sid = (int) format.getSegmentId(buf);
        boolean res = get(sid, offset, keyValueSize, buffer, bufOffset);
        if (!res) {
          return NOT_FOUND;
        }
        // Now buffer contains both: key and value, we need to compare keys
        // Format of a key-value pair in a buffer: key-size, value-size, key, value
        int kSize = Utils.readUVInt(buffer, bufOffset);
        if (kSize != keySize) {
          return NOT_FOUND;
        }
        int kSizeSize = Utils.sizeUVInt(kSize);
        bufOffset += kSizeSize;
        int vSize = Utils.readUVInt(buffer, bufOffset);
        int vSizeSize = Utils.sizeUVInt(vSize);
        bufOffset += vSizeSize;

        // Now compare keys
        if (Utils.compareTo(buffer, bufOffset, keySize, key, keyOffset, keySize) == 0) {
          // If key is the same
          return keyValueSize;
        } else {
          return NOT_FOUND;
        }
      }
    } finally {
      UnsafeAccess.free(buf);
      this.index.readUnlock(slot);
    }
  }

  /**
   * Get item into a given byte buffer
   *
   * @param keyPtr key address
   * @param keySize size of a key
   * @param buffer byte buffer
   * @return length of an item or -1
   * @throws IOException
   */
  public long get(long keyPtr, int keySize, ByteBuffer buffer) throws IOException {
    IndexFormat format = this.index.getIndexFormat();
    int bSize = format.indexEntrySize();
    long buf = UnsafeAccess.mallocZeroed(bSize);
    boolean dataEmbedded = this.config.isIndexDataEmbeddedSupported();
    int slot = 0;
    try {
      //TODO: double locking?
      slot = this.index.readLock(keyPtr, keySize);
      long result = index.find(keyPtr, keySize, true, buf, bSize);
      if (result < 0) {
        return NOT_FOUND;
      } else if (result > bSize) {
        UnsafeAccess.free(buf);
        bSize = (int) result;
        buf = UnsafeAccess.mallocZeroed(bSize);
        index.find(keyPtr, keySize, true, buf, bSize);
      }
      // First 4 bytes is a cached item size
      // This call returns TOTAL size: key + value + kSize + vSize
      int keyValueSize = format.getKeyValueSize(buf);
      // TODO: actually, not correct
      if (keyValueSize > buffer.remaining()) {
        return keyValueSize;
      }

      dataEmbedded = dataEmbedded && (keyValueSize < this.config.getIndexDataEmbeddedSize());
      if (dataEmbedded) {
        // Return embedded data
        int off = format.getEmbeddedOffset();
        int kSize = Utils.readUVInt(buf + off);
        if (kSize != keySize) {
          return NOT_FOUND;
        }
        int kSizeSize = Utils.sizeUVInt(kSize);
        off += kSizeSize;
        int vSize = Utils.readUVInt(buf + off);
        int vSizeSize = Utils.sizeUVInt(vSize);
        off += vSizeSize;
        if (Utils.compareTo(keyPtr, keySize, buf + off, kSize) != 0) {
          return NOT_FOUND;
        }
        off -= kSizeSize + vSizeSize;
        // Copy data to buffer
        UnsafeAccess.copy(buf + off, buffer, keyValueSize);
        return keyValueSize;
      } else {
        // Offset in a segment - 4 bytes
        long offset = format.getOffset(buf);
        // Segment id
        int sid = (int) format.getSegmentId(buf);
        int off = buffer.position();
        boolean res = get(sid, offset, keyValueSize, buffer);
        if (!res) {
          return NOT_FOUND;
        }
        buffer.position(off);
        // Now buffer contains both: key and value, we need to compare keys
        // Format of a key-value pair in a buffer: key-size, value-size, key, value
        int kSize = Utils.readUVInt(buffer);
        if (kSize != keySize) {
          return NOT_FOUND;
        }
        int kSizeSize = Utils.sizeUVInt(kSize);
        buffer.position(off + kSizeSize);

        int vSize = Utils.readUVInt(buffer);
        int vSizeSize = Utils.sizeUVInt(vSize);
        buffer.position(off + kSizeSize + vSizeSize);
        // Now compare keys
        if (Utils.compareTo(buffer, keySize, keyPtr, keySize) == 0) {
          // If key is the same
          buffer.position(off + keyValueSize);
          return keyValueSize;
        } else {
          return NOT_FOUND;
        }
      }
    } finally {
      UnsafeAccess.free(buf);
      this.index.readUnlock(slot);
    }
  }

  /**
   * Get item into a given byte buffer
   *
   * @param key key buffer
   * @param off offset
   * @param size size of a key
   * @param buffer buffer
   * @return length of an item or -1
   * @throws IOException
   */
  public long get(byte[] key, int keyOffset, int keySize, ByteBuffer buffer) throws IOException {

    IndexFormat format = this.index.getIndexFormat();
    int bSize = format.indexEntrySize();
    long buf = UnsafeAccess.mallocZeroed(bSize);
    boolean dataEmbedded = this.config.isIndexDataEmbeddedSupported();
    int slot = 0;
    try {
      slot = this.index.readLock(key, keyOffset, keySize);
      long result = index.find(key, keyOffset, keySize, true, buf, bSize);
      if (result < 0) {
        return NOT_FOUND;
      } else if (result > bSize) {
        UnsafeAccess.free(buf);
        bSize = (int) result;
        buf = UnsafeAccess.mallocZeroed(bSize);
        index.find(key, keyOffset, keySize, true, buf, bSize);
      }
      // First 4 bytes is a cached item size
      // This call returns TOTAL size: key + value + kSize + vSize
      int keyValueSize = format.getKeyValueSize(buf);
      // TODO: actually, not correct
      if (keyValueSize > buffer.remaining()) {
        return keyValueSize;
      }
      dataEmbedded = dataEmbedded && (keyValueSize < this.config.getIndexDataEmbeddedSize());
      if (dataEmbedded) {
        // Return embedded data
        int off = format.getEmbeddedOffset();
        int kSize = Utils.readUVInt(buf + off);
        if (kSize != keySize) {
          return NOT_FOUND;
        }
        int kSizeSize = Utils.sizeUVInt(kSize);
        off += kSizeSize;
        int vSize = Utils.readUVInt(buf + off);
        int vSizeSize = Utils.sizeUVInt(vSize);
        off += vSizeSize;
        if (Utils.compareTo(key, keyOffset, keySize, buf + off, kSize) != 0) {
          return NOT_FOUND;
        }
        off -= kSizeSize + vSizeSize;
        // Copy data to buffer
        UnsafeAccess.copy(buf + off, buffer, keyValueSize);
        return keyValueSize;
      } else {
        // Offset in a segment - 4 bytes
        long offset = format.getOffset(buf);
        // Segment id
        int sid = (int) format.getSegmentId(buf);
        int off = buffer.position();
        boolean res = get(sid, offset, keyValueSize, buffer);
        if (!res) {
          return NOT_FOUND;
        }
        buffer.position(off);
        // Now buffer contains both: key and value, we need to compare keys
        // Format of a key-value pair in a buffer: key-size, value-size, key, value
        int kSize = Utils.readUVInt(buffer);
        if (kSize != keySize) {
          return NOT_FOUND;
        }
        int kSizeSize = Utils.sizeUVInt(kSize);
        buffer.position(off + kSizeSize);

        int vSize = Utils.readUVInt(buffer);
        int vSizeSize = Utils.sizeUVInt(vSize);
        buffer.position(off + kSizeSize + vSizeSize);
        // Now compare keys
        if (Utils.compareTo(buffer, keySize, key, keyOffset, keySize) == 0) {
          // If key is the same
          buffer.position(off + keyValueSize);
          return keyValueSize;
        } else {
          return NOT_FOUND;
        }
      }
    } finally {
      UnsafeAccess.free(buf);
      this.index.readUnlock(slot);
    }
  }

  /**
   * Get cached item
   *
   * @param id data segment id to read from
   * @param offset data segment offset
   * @param size size of an item in bytes
   * @param buffer memory buffer to load data to
   * @param bufOffset offset
   * @return true - on success, false - otherwise
   */
  public boolean get(int id, long offset, int size, byte[] buffer, int bufOffset)
      throws IOException {
    if (buffer == null || (buffer.length - bufOffset) < size) {
      throw new IllegalArgumentException();
    }
    if (getFromRAMBuffers(id, offset, size, buffer, bufOffset)) {
      return true;
    }
    return getInternal(id, offset, size, buffer, bufOffset);
  }

  /**
   * Try to get data from RAM buffers
   *
   * @param id segment's id
   * @param offset offset in a segment (0 - based)
   * @param size size of an cached item
   * @param buffer buffer to load data to
   * @return true - on success, false - otherwise
   */
  private synchronized boolean getFromRAMBuffers(int id, long offset, int size, byte[] buffer, int bufOffset) {
    Segment s = getRAMSegment(id);
    try {
      if (s != null) {
        s.readLock();
        if (s.dataSize() < offset + size) {
          throw new IllegalArgumentException();
        }
        UnsafeAccess.copy(s.getAddress() + offset, buffer, bufOffset, size);
        return true;
      }
    } finally {
      if (s != null) {
        s.readUnlock();
      }
    }
    return false;
  }

  /**
   * Try to get data from RAM buffers
   *
   * @param id segment's id
   * @param offset offset in a segment (0 - based)
   * @param size size of an cached item
   * @param buffer buffer to load data to
   * @return true - on success, false - otherwise
   */
  private synchronized boolean getFromRAMBuffers(int id, long offset, int size, ByteBuffer buffer) {
    Segment s = getRAMSegment(id);
    try {
      if (s != null) {
        s.readLock();
        if (s.dataSize() < offset + size) {
          throw new IllegalArgumentException();
        }
        UnsafeAccess.copy(s.getAddress() + offset, buffer, size);
        return true;
      }
    } finally {
      if (s != null) {
        s.readUnlock();
      }
    }
    return false;
  }

  private Segment getRAMSegment(int id) {
    for (Segment s : ramBuffers) {
      if (s != null && s.getId() == id) {
        return s;
      }
    }
    return null;
  }

  /**
   * Get cached item from underlying IOEngine
   *
   * @param id segment id
   * @param offset offset in a segment
   * @param size size of an item
   * @param buffer buffer to load data to
   * @param bufOffset offset
   * @throws IOException
   */
  protected abstract boolean getInternal(
      int id, long offset, int size, byte[] buffer, int bufOffset) throws IOException;
  /**
   * Get cached item
   *
   * @param id data segment id to read from
   * @param offset data segment offset
   * @param size size of an item in bytes
   * @param buffer byte buffer to load data to
   * @return true - on success, false - otherwise
   */
  public boolean get(int id, long offset, int size, ByteBuffer buffer) throws IOException {
    if (buffer == null || buffer.remaining() < size) throw new IllegalArgumentException();
    if (getFromRAMBuffers(id, offset, size, buffer)) {
      return true;
    }
    return getInternal(id, offset, size, buffer);
  }

  /**
   * Get cached item from underlying IOEngine
   *
   * @param id segment id
   * @param offset offset in a segment
   * @param size size of an item
   * @param buffer buffer to load data to
   * @throws IOException
   */
  protected abstract boolean getInternal(int id, long offset, int size, ByteBuffer buffer)
      throws IOException;

  /**
   * Save data segment
   *
   * @param data data segment
   */
  public void save(Segment data) throws IOException {
    synchronized (data) {
      if (data.isSealed()) {
        return;
      }
      data.seal();
      // Move data to a main storage
      this.dataSegments[data.getId()] = data;
      // }
      // Call IOEngine - specific (FileIOEngine overrides it)
      saveInternal(data);
      // Notify listener
      if (this.aListener != null) {
        aListener.onEvent(this, IOEngineEvent.DATA_SIZE_CHANGED);
      }
    }
  }

  /**
   * IOEngine subclass can override this method
   *
   * @param data data segment
   * @throws IOException 
   */
  protected void saveInternal(Segment data) throws IOException {
    removeFromRAMBuffers(data);
  }

  protected boolean removeFromRAMBuffers(Segment data) {
    for (int i = 0; i < ramBuffers.length; i++) {
      if (data == ramBuffers[i]) {
        ramBuffers[i] = null;
        return true;
      }
    }
    return false;
  }

  /**
   * Get maximum storage size (depends on IOEngine)
   *
   * @return maximum storage size
   */
  public long getMaximumStorageSize() {
    return this.maxStorageSize;
  }

  /**
   * Creates segment scanner
   *
   * @param s segment
   * @return segment scanner
   * @throws IOException
   */
  public abstract SegmentScanner getScanner(Segment s) throws IOException;

  /**
   * Release id after segment recycling
   *
   * @param id data segment id
   */
  public synchronized void releaseSegmentId(Segment seg) {
    if (seg.isOffheap()) {
      seg.vacate();
    } else {
      dataSegments[seg.getId()] = null;
    }
    if (this.aListener != null) {
      aListener.onEvent(this, IOEngineEvent.DATA_SIZE_CHANGED);
    }
  }
  /**
   * Update statistics for a segment with a given id
   *
   * @param id segment id
   * @param itemIncrement number of items to increment
   * @param rankIncrement total rank to increment
   */
  public void updateStats(int id, int itemIncrement, int rankIncrement) {
    checkId(id);
    Objects.requireNonNull(dataSegments[id]);
    dataSegments[id].update(itemIncrement, rankIncrement);
  }

  /**
   * Get segment id with the minimum average rank (for recycling)
   *
   * @return segment id
   */
  public synchronized Segment getMinimumAvgRankSegment() {
    int id = -1;
    double min = Double.MAX_VALUE;
    for (int i = 0; i < dataSegments.length; i++) {
      if (dataSegments[i] == null || !dataSegments[i].isSealed()) {
        continue;
      }
      if (dataSegments[i].getRank() < min) {
        min = dataSegments[i].getRank();
        id = i;
      }
    }
    return id < 0 ? null : dataSegments[id];
  }

  /**
   * Scans and finds available id for a new data segment
   *
   * @return id (or -1)
   */
  public synchronized int getAvailableId() {
    for (int i = 0; i < dataSegments.length; i++) {
      if (dataSegments[i] == null || dataSegments[i].isVacated()) {
        return i;
      }
    }
    return NOT_FOUND; // not found
  }

  private void checkId(int id) {
    if (id < 0 || id >= dataSegments.length || dataSegments[id] == null) {
      throw new IllegalArgumentException(String.format("illegal id %d ", id));
    }
  }

  /**
   * Put key-value into a cache with a default rank
   *
   * @param key key buffer
   * @param value value buffer
   * @param rank cache item rank
   * @throws IOException
   */
  public void put(byte[] key, byte[] value, long expire) throws IOException {
    int rank = config.getSLRUInsertionPoint(this.cacheName);
    // TODO: check if it is the right rank
    // TODO: expire on rank
    put(key, 0, key.length, value, 0, value.length, expire, rank);
  }
  /**
   * Put key-value into a cache with a rank
   *
   * @param key key buffer
   * @param value value buffer
   * @param rank cache item rank
   * @throws IOException
   */
  public void put(byte[] key, byte[] value, long expire, int rank) throws IOException {
    put(key, 0, key.length, value, 0, value.length, expire, rank);
  }

  /**
   * Put key-value into a cache with a default rank
   *
   * @param key key buffer
   * @param keyOff key offset
   * @param keyLength key length
   * @param value value buffer
   * @param valueOff value offset
   * @param valueLength value length
   * @throws IOException
   */
  public void put(
      byte[] key,
      int keyOff,
      int keyLength,
      byte[] value,
      int valueOff,
      int valueLength,
      long expire)
      throws IOException {
    int rank = config.getSLRUInsertionPoint(this.cacheName);
    put(key, keyOff, keyLength, value, valueOff, valueLength, expire, rank);
  }
  /**
   * Put key-value into a cache
   *
   * @param key key buffer
   * @param keyOff key offset
   * @param keyLength key length
   * @param value value buffer
   * @param valueOff value offset
   * @param valueLength value length
   * @param rank rank of a cache item
   * @throws IOException
   */
  public void put(
      byte[] key,
      int keyOff,
      int keyLength,
      byte[] value,
      int valueOff,
      int valueLength,
      long expire,
      int rank)
      throws IOException {
    checkRank(rank);
    long iptr = 0;
    try {
      //TODO: check double locking
      this.rbLocks[rank].writeLock().lock();
      Segment s = getRAMSegmentByRank(rank);
      if (s == null) {
        // We silently ignore PUT operation due to lack of resources
        //TODO: update stats
        return;
      }
      // Offset must less 32bit
      long offset = s.append(key, keyOff, keyLength, value, valueOff, valueLength);
      if (offset < 0) {
        save(s); // removes segment from RAM buffers
        this.ramBuffers[rank] = null;
        s = getRAMSegmentByRank(rank);
        if (s == null) {
          // We silently ignore PUT operation due to lack of resources
          //TODO: update stats
          return;
        }
        offset = s.append(key, keyOff, keyLength, value, valueOff, valueLength);
      }
      
      IndexFormat format = this.index.getIndexFormat();
      int size = format.fullEntrySize(keyLength, valueLength);
      iptr = UnsafeAccess.malloc(size); 
      
      int dataSize = Utils.kvSize(keyLength, valueLength);
      
      format.writeIndex(iptr, key, keyOff, keyLength, value, valueOff, valueLength, 
          (short) s.getId(), (int) offset, dataSize, expire); 
      
      this.index.insertWithRank(key, keyOff, keyLength, iptr, size, rank);
    } finally {
      this.rbLocks[rank].writeLock().unlock();
      UnsafeAccess.free(iptr);
    }
  }


  /**
   * Put key-value into a cache with a default rank
   *
   * @param key key buffer
   * @param keyOff key offset
   * @param keyLength key length
   * @param value value buffer
   * @param valueOff value offset
   * @param valueLength value length
   * @throws IOException
   */
  public void put(long keyPtr, int keyLength, long valuePtr, int valueLength, long expire)
      throws IOException {
    int rank = config.getSLRUInsertionPoint(this.cacheName);
    put(keyPtr, keyLength, valuePtr, valueLength, expire, rank);
  }
  /**
   * Put key-value into a cache with a rank
   *
   * @param key key buffer
   * @param keyOff key offset
   * @param keyLength key length
   * @param value value buffer
   * @param valueOff value offset
   * @param valueLength value length
   * @param rank rank of a cache item
   * @throws IOException
   */
  public void put(long keyPtr, int keyLength, long valuePtr, int valueLength, long expire, int rank)
      throws IOException {
    checkRank(rank);
    long iptr = 0;
    try {
      this.rbLocks[rank].writeLock().lock();
      Segment s = getRAMSegmentByRank(rank);
      if (s == null) {
        // We silently ignore PUT operation due to lack of resources
        //TODO: update stats
        return;
      }
      long offset = s.append(keyPtr, keyLength, valuePtr, valueLength);
      if (offset < 0) {
        save(s); // removes segment from RAM buffers
        ramBuffers[rank] = null;
        s = getRAMSegmentByRank(rank);
        if (s == null) {
          // We silently ignore PUT operation due to lack of resources
          //TODO: update stats
          return;
        }
        offset = s.append(keyPtr, keyLength, valuePtr, valueLength);
      }
      int dataSize = Utils.kvSize(keyLength, valueLength);
      
      IndexFormat format = this.index.getIndexFormat();
      int size = format.fullEntrySize(keyLength, valueLength);
      iptr = UnsafeAccess.malloc(size); 
            
      format.writeIndex(iptr, keyPtr, keyLength, valuePtr,  valueLength, 
          (short) s.getId(), (int) offset, dataSize, expire); 
      this.index.insertWithRank(keyPtr, keyLength, iptr, size, rank);
      
    } finally {
      this.rbLocks[rank].writeLock().unlock();
      UnsafeAccess.free(iptr);
    }
  }

  private Segment getRAMSegmentByRank(int rank) {
    Segment s = this.ramBuffers[rank];
    if (s == null) {
      synchronized(this.ramBuffers) {
        s = this.ramBuffers[rank];
        if (s != null) {
          return s;
        }
        int id = getAvailableId();
        if (id < 0) {
          return null;
        }
        if (this.dataSegments[id] == null) {
          s = Segment.newSegment((int) this.segmentSize, id, rank, System.currentTimeMillis());
          this.dataSegments[id] = s;

        } else {
          s = this.dataSegments[id];
          s.reuse(id, rank, System.currentTimeMillis());
        }
        this.ramBuffers[rank] = s;
      }
    }
    return s;
  }

  private void checkRank(int rank) {
    if (rank < 0 || rank >= ramBuffers.length) {
      throw new IllegalArgumentException(String.format("Illegal rank value: %d", rank));
    }
  }

  @Override
  public void save (OutputStream os) throws IOException {
    // os == null - because this is multi-file
    String snapshotDir = this.config.getSnapshotDir(cacheName);
    Path filePath = Paths.get(snapshotDir, CacheConfig.CACHE_ENGINE_SNAPSHOT_NAME);
    Files.deleteIfExists(filePath);
    FileOutputStream fos = new FileOutputStream(filePath.toFile());
    DataOutputStream dos = new DataOutputStream(fos);
    
    int num = getNumberOfActiveSegments();
    dos.writeInt(num);
    // Save all segment meta info
    for(Segment s : this.dataSegments) {
      if (s == null) {
        continue;
      }
      num++;
      Segment.Info info = s.getInfo();
      info.save(fos);
    }
    // Save index
    this.index.save(dos);
    
    dos.close();  
  }
  
  protected int getNumberOfActiveSegments() {
    int num = 0;
    synchronized(this.dataSegments) {
      for(Segment s: this.dataSegments) {
        if (s != null) num++;
      }
    }
    return num;
  }
  
  @Override
  public void load(InputStream is) throws IOException {
    // is == null
    String snapshotDir = this.config.getSnapshotDir(cacheName);
    Path filePath = Paths.get(snapshotDir, CacheConfig.CACHE_ENGINE_SNAPSHOT_NAME);
    if(!Files.exists(filePath)) {
      LOG.warn("Can not load engine, path {} does not exist.", filePath);
      return;
    }
    FileInputStream fis = new FileInputStream(filePath.toFile());
    DataInputStream dis = new DataInputStream(fis);
    int num = dis.readInt();
    for (int i = 0; i < num; i++) {
      Segment.Info info = new Segment.Info();
      info.load(dis);
      int id = info.getId();
      this.dataSegments[id] = new Segment();
      this.dataSegments[i].setInfo(info);
    }
    
    this.index.load(dis);
    
    // We loaded all info and created empty segments
    // Now subclasses must implement additional data loading
    dis.close();    
  }
  
  /**
   * Get data segment file name
   * @param id data segment id
   * @return file name
   */
  protected String getSegmentFileName(int id) {
    return FILE_NAME + Utils.format(Integer.toString(id), 6);
  }
}
