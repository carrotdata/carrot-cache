/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.carrotdata.cache.io;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.carrotdata.cache.util.Persistent;
import com.carrotdata.cache.util.UnsafeAccess;
import com.carrotdata.cache.util.Utils;

public class WriteBatch implements Persistent {
  /**
   * Data block or batch recommended size
   */
  private int batchSize;
  /**
   * Capacity of this write batch - 2 * pageSize + META size
   */
  private int capacity;
  /**
   * Position of written so far data (key and values), when object is created this value must be
   * equals to META size
   */
  private volatile int position;

  /**
   * Memory pointer to the allocated buffer which keeps all batched data
   */
  private long memory;

  /**
   * Write batch id. It is used as the write batch address for look up operations
   */
  private int id;

  private ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

  /**
   * Empty ctor
   */
  WriteBatch() {

  }

  /**
   * Nice ctor
   * @param pageSize page size
   * @param metaSize meta section size
   */
  WriteBatch(int pageSize) {
    this.batchSize = pageSize;
    this.capacity = 2 * pageSize;
    this.position = 0;
    this.memory = UnsafeAccess.mallocZeroed(this.capacity);
  }

  /**
   * Gets memory buffer address
   * @return address
   */
  long memory() {
    return this.memory;
  }

  /**
   * Gets page size
   * @return page size
   */
  int batchSize() {
    return this.batchSize;
  }

  /**
   * Gets batch capacity
   * @return capacity
   */
  int capacity() {
    return this.capacity;
  }

  /**
   * Gets current write position
   * @return write position
   */
  int position() {
    return this.position;
  }

  boolean isFull() {
    return this.position >= this.batchSize;
  }

  /**
   * Write key, value, time stamp to this buffer
   * @param keyPtr key address
   * @param keySize key size
   * @param valuePtr value address
   * @param valueSize value size
   * @return record offset in the batch buffer or -1 if full
   */
  int addOrUpdate(long keyPtr, int keySize, long valuePtr, int valueSize) {
    try {
      lock.writeLock().lock();
      if (this.position >= this.batchSize) {
        return -1; // Its time to process write batch
      }

      // We have k-v size <= pageSize && position + kvSize < capacity
      // so we are fine - no buffer overflow

      int off = find(keyPtr, keySize);
      int oldPosition = this.position;

      if (off < 0) { // add
        // Segment must not allow any writes to a write batch buffer which are
        // are larger than page size, so we do not do any checks here
        // Write key
        this.position += Utils.writeUVInt(this.memory + this.position, keySize);
        this.position += Utils.writeUVInt(this.memory + this.position, valueSize);
        UnsafeAccess.copy(keyPtr, this.memory + this.position, keySize);
        this.position += keySize;
        UnsafeAccess.copy(valuePtr, this.memory + this.position, valueSize);
        this.position += valueSize;
        return oldPosition;
      } else {
        // Replace found
        int oldSize = Utils.kvSize(this.memory + off);
        int newSize = Utils.kvSize(keySize, valueSize);
        int toMove = this.position - oldSize - off;
        // Move
        UnsafeAccess.copy(this.memory + off + oldSize, this.memory + off + newSize, toMove);
        // Copy
        int $off = off;
        off += Utils.writeUVInt(this.memory + off, keySize);
        off += Utils.writeUVInt(this.memory + off, valueSize);
        UnsafeAccess.copy(keyPtr, this.memory + off, keySize);
        off += keySize;
        UnsafeAccess.copy(valuePtr, this.memory + off, valueSize);
        this.position += newSize - oldSize;
        return $off;
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Write key, value, time stamp to this buffer
   * @param key key buffer
   * @param keyOffset key offset
   * @param keySize key size
   * @param value value buffer
   * @param valueOffset value offset
   * @param valueSize value size
   * @param expire expiration time
   * @return new size or -1 if buffer is full
   */
  int addOrUpdate(byte[] key, int keyOffset, int keySize, byte[] value, int valueOffset,
      int valueSize) {
    try {
      lock.writeLock().lock();
      if (this.position >= this.batchSize) {
        return -1; // Its time to process write batch
      }

      // We have k-v size <= pageSize && position + kvSize < capacity
      // so we are fine - no buffer overflow

      int off = find(key, keyOffset, keySize);
      int oldPosition = this.position;

      if (off < 0) { // add
        // Segment must not allow any writes to a write batch buffer which are
        // are larger than batch size, so we do not do any checks here
        // Write key
        this.position += Utils.writeUVInt(this.memory + this.position, keySize);
        this.position += Utils.writeUVInt(this.memory + this.position, valueSize);
        UnsafeAccess.copy(key, keyOffset, this.memory + this.position, keySize);
        this.position += keySize;
        UnsafeAccess.copy(value, valueOffset, this.memory + this.position, valueSize);
        this.position += valueSize;
        return oldPosition;
      } else {
        // Replace found
        int oldSize = Utils.kvSize(this.memory + off);
        int newSize = Utils.kvSize(keySize, valueSize);
        int toMove = this.position - oldSize - off;
        // Move
        UnsafeAccess.copy(this.memory + off + oldSize, this.memory + off + newSize, toMove);
        // Copy
        int $off = off;
        off += Utils.writeUVInt(this.memory + off, keySize);
        off += Utils.writeUVInt(this.memory + off, valueSize);
        UnsafeAccess.copy(key, keyOffset, this.memory + off, keySize);
        off += keySize;
        UnsafeAccess.copy(value, valueOffset, this.memory + off, valueSize);
        this.position += newSize - oldSize;
        return $off;
      }
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Get key - value by key
   * @param keyPtr key pointer
   * @param keySize key size
   * @param buffer buffer pointer
   * @param bufferSize buffer size
   * @return size of a key - value, caller must check return value
   */
  int get(long keyPtr, int keySize, long buffer, int bufferSize) {
    try {
      lock.readLock().lock();
      int off = find(keyPtr, keySize);
      if (off < 0) {
        return -1;
      }
      int kSize = Utils.readUVInt(this.memory + off);
      int kSizeSize = Utils.sizeUVInt(kSize);
      int valSize = Utils.readUVInt(this.memory + off + kSizeSize);
      int vSizeSize = Utils.sizeUVInt(valSize);
      int kvSize = keySize + valSize + kSizeSize + vSizeSize;
      if (kvSize > bufferSize) {
        return kvSize;
      }
      UnsafeAccess.copy(this.memory + off, buffer, kvSize);
      return kvSize;
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Get value by key
   * @param keyPtr key pointer
   * @param keySize key size
   * @param buffer byte buffer
   * @param bufferOffset buffer offset
   * @return size of a value, caller must check return value
   */
  int get(long keyPtr, int keySize, byte[] buffer, int bufferOffset) {
    try {
      lock.readLock().lock();
      int off = find(keyPtr, keySize);
      if (off < 0) {
        return -1;
      }
      int kSize = Utils.readUVInt(this.memory + off);
      int kSizeSize = Utils.sizeUVInt(kSize);
      int valSize = Utils.readUVInt(this.memory + off + kSizeSize);
      int vSizeSize = Utils.sizeUVInt(valSize);
      int kvSize = keySize + valSize + kSizeSize + vSizeSize;
      if (kvSize > buffer.length - bufferOffset) {
        return kvSize;
      }
      UnsafeAccess.copy(this.memory + off, buffer, bufferOffset, kvSize);
      return kvSize;
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Get value by key
   * @param key key buffer
   * @param keyOffset key offset
   * @param keySize key size
   * @param buffer buffer pointer
   * @param bufferSize buffer size
   * @return size of a value, caller must check return value
   */
  int get(byte[] key, int keyOffset, int keySize, long buffer, int bufferSize) {
    try {
      lock.readLock().lock();
      int off = find(key, keyOffset, keySize);
      if (off < 0) {
        return -1;
      }
      int kSize = Utils.readUVInt(this.memory + off);
      int kSizeSize = Utils.sizeUVInt(kSize);
      int valSize = Utils.readUVInt(this.memory + off + kSizeSize);
      int vSizeSize = Utils.sizeUVInt(valSize);
      int kvSize = keySize + valSize + kSizeSize + vSizeSize;
      if (kvSize > bufferSize) {
        return kvSize;
      }
      UnsafeAccess.copy(this.memory + off, buffer, kvSize);
      return kvSize;
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Get value by key
   * @param key key buffer
   * @param keyOffset key offset
   * @param keySize key size
   * @param buffer buffer pointer
   * @param bufferOffset buffer offset
   * @return size of a value, caller must check return value
   */
  int get(byte[] key, int keyOffset, int keySize, byte[] buffer, int bufferOffset) {
    try {
      lock.readLock().lock();
      int off = find(key, keyOffset, keySize);
      if (off < 0) {
        return -1;
      }
      int kSize = Utils.readUVInt(this.memory + off);
      int kSizeSize = Utils.sizeUVInt(kSize);
      int valSize = Utils.readUVInt(this.memory + off + kSizeSize);
      int vSizeSize = Utils.sizeUVInt(valSize);
      int kvSize = keySize + valSize + kSizeSize + vSizeSize;
      if (kvSize > buffer.length - bufferOffset) {
        return kvSize;
      }
      UnsafeAccess.copy(this.memory + off, buffer, bufferOffset, kvSize);
      return kvSize;
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Get value by key
   * @param key key buffer
   * @param keyOffset key offset
   * @param keySize key size
   * @param buffer byte buffer
   * @return size of a value, caller must check return value
   */
  public int get(byte[] key, int keyOffset, int keySize, ByteBuffer buffer) {

    if (buffer.hasArray()) {
      byte[] buf = buffer.array();
      int off = buffer.position();
      int avail = buffer.remaining();
      int size = get(key, keyOffset, keySize, buf, off);
      if (size > avail) {
        return size;
      } else if (size >= 0) {
        // buffer.position(off + size);
        return size;
      }
    } else {
      long ptr = UnsafeAccess.address(buffer);
      int off = buffer.position();
      int avail = buffer.remaining();
      int size = get(key, keyOffset, keySize, ptr + off, avail);
      if (size > avail) {
        return size;
      } else if (size >= 0) {
        // buffer.position(off + size);
        return size;
      }
    }
    return -1;
  }

  /**
   * Get value by key
   * @param keyPtr key pointer
   * @param keySize key size
   * @param buffer byte buffer
   * @return size of a value, caller must check return value
   */
  public int get(long keyPtr, int keySize, ByteBuffer buffer) {
    if (buffer.hasArray()) {
      byte[] buf = buffer.array();
      int off = buffer.position();
      int avail = buffer.remaining();
      int size = get(keyPtr, keySize, buf, off);
      if (size > avail) {
        return size;
      } else if (size >= 0) {
        // buffer.position(off + size);
        return size;
      }
    } else {
      long ptr = UnsafeAccess.address(buffer);
      int off = buffer.position();
      int avail = buffer.remaining();
      int size = get(keyPtr, keySize, ptr + off, avail);
      if (size > avail) {
        return size;
      } else if (size >= 0) {
        // buffer.position(off + size);
        return size;
      }
    }
    return -1;
  }

  private int find(final long keyPtr, final int keySize) {
    final long ptr = this.memory;
    int off = 0;
    while (off < this.position) {
      final int oldOffset = off;
      final int kSize = Utils.readUVInt(ptr + off);
      // Compare
      off += Utils.sizeUVInt(kSize);
      final int vSize = Utils.readUVInt(ptr + off);
      off += Utils.sizeUVInt(vSize);
      if (Utils.equals(ptr + off, kSize, keyPtr, keySize)) {
        return oldOffset;
      }
      off += kSize + vSize;
    }
    return -1; // Not found
  }

  private int find(final byte[] key, final int keyOffset, final int keySize) {
    final long ptr = this.memory;
    int off = 0;
    while (off < this.position) {
      final int oldOffset = off;
      final int kSize = Utils.readUVInt(ptr + off);
      off += Utils.sizeUVInt(kSize);
      int vSize = Utils.readUVInt(ptr + off);
      off += Utils.sizeUVInt(vSize);
      if (Utils.compareTo(key, keyOffset, keySize, ptr + off, kSize) == 0) {
        return oldOffset;
      }
      off += kSize + vSize;
    }
    return -1; // Not found
  }

  void writeLock() {
    lock.writeLock().lock();
  }

  void writeUnlock() {
    lock.writeLock().unlock();
  }

  /**
   * Resets batch, makes it ready to accept new writes
   */
  void reset() {
    try {
      // Do we need this?
      lock.writeLock().lock();
      this.position = 0;
    } finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * Deallocates memory For testing only
   */
  void dispose() {
    UnsafeAccess.free(this.memory);
  }

  /**
   * Checks if its empty
   * @return
   */
  boolean isEmpty() {
    return this.position == 0;
  }

  /*
   * Sets Write batch id. This Id is used as the address identifier for look-up operations
   */
  void setId(int id) {
    this.id = id;
  }

  /**
   * Get write batch id
   * @return id
   */
  int getId() {
    return this.id;
  }

  /**
   * Get rank of segment group this batch belongs to
   * @return rank
   */
  int getRank() {
    return -this.id >>> 16;
  }

  /**
   * Get thread id which this batch belongs to
   * @return
   */
  int getThreadId() {
    return (-this.id & 0xffff) - 2;
  }

  /**
   * Does this batch accepts writes
   * @return true if accepts, false otherwise
   */
  boolean acceptsWrite(int kvSize) {
    return this.position < this.batchSize && this.position + kvSize <= this.capacity;
  }

  @Override
  public void save(OutputStream os) throws IOException {
    DataOutputStream dos = Utils.toDataOutputStream(os);
    dos.writeInt(this.batchSize);
    dos.writeInt(this.capacity);
    dos.writeInt(this.position);
    dos.writeInt(this.id);
    if (this.position > 0) {
      byte[] buf = new byte[this.position];
      UnsafeAccess.copy(this.memory, buf, 0, this.position);
      dos.write(buf);
    }
  }

  @Override
  public void load(InputStream is) throws IOException {
    DataInputStream dis = Utils.toDataInputStream(is);
    this.batchSize = dis.readInt();
    this.capacity = dis.readInt();
    this.position = dis.readInt();
    this.id = dis.readInt();
    this.memory = UnsafeAccess.mallocZeroed(this.capacity);
    if (this.position > 0) {
      byte[] buf = new byte[this.position];
      dis.readFully(buf);
      UnsafeAccess.copy(buf, 0, this.memory, buf.length);
    }
  }

}