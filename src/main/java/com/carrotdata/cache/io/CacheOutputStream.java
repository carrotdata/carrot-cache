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

import java.io.IOException;
import java.io.OutputStream;

import com.carrotdata.cache.Cache;
import com.carrotdata.cache.util.ObjectPool;
import com.carrotdata.cache.util.Utils;

public class CacheOutputStream extends OutputStream {
  
  /** Buffer to write data to */
  byte[] buffer;
  
  /** Current offset in the buffer*/
  int bufferOffset;
  
  /** Absolute position of a buffer start in the stream*/  
  long bufferPos;
  
  /** Key */
  byte[] keyBase;
  
  /** Parent cache */
  Cache parent;
  
  /** Page size - buffer size*/
  int pageSize;
  
  /** Expiration for the stream */
  long expire;
  
  /** Buffer pool */
  static ObjectPool<byte[]> pool;
  
  /** Stream is closed */
  boolean closed = false;
  
  /** Ignore flush() and close() */
  boolean ignoreFC = false;
  
  /**
   * Constructor 
   * @param parent cache
   * @param key stream key
   * @param expire 
   */
  public CacheOutputStream(Cache parent, byte[] key, int off, int len,  long expire) {
    this.parent = parent;
    int poolSize = parent.getCacheConfig().getIOStoragePoolSize(parent.getName());
    if (pool == null) {
      synchronized(ObjectPool.class) {
        if (pool == null) {
          pool = new ObjectPool<byte[]>(poolSize);
        }
      }
    }
    this.pageSize = parent.getCacheConfig().getCacheStreamingSupportBufferSize(parent.getName());
    this.buffer = pool.poll();
    if (this.buffer == null) {
      this.buffer = new byte[pageSize];
    }
    this.expire = expire;
    this.keyBase = new byte[len + Utils.SIZEOF_LONG];
    System.arraycopy(key, off, keyBase, 0, len);
    keyBase = getKey(bufferPos);
  }
  
  /**
   * Set ignore regular flush and close 
   * @param b
   */
  public void setIgnoreFlushAndClose(boolean b) {
    this.ignoreFC = b;
  }
  
  /**
   * Get ignore set and close
   * @return current setting
   */
  public boolean getIgnoreFlushAndClose() {
    return this.ignoreFC;
  }
  
  private byte[] getKey(long offset) {
    int size = this.keyBase.length;
    offset = offset / pageSize * pageSize;
    for (int i = 0; i < Utils.SIZEOF_LONG; i++) {
      int rem = (int) (offset % 256);
      this.keyBase[size - i - 1] = (byte) rem;
      offset /= 256;
    }
    return this.keyBase;
  }

  @Override
  public void write(int b) throws IOException {
    checkClosed();
    if (bufferOffset >= buffer.length) {
      nextPage();
    } 
    buffer[bufferOffset++] = (byte) (b & 0xff);
  }

  private void nextPage() throws IOException {
    // save current
    this.keyBase = getKey(bufferPos);
    this.parent.put(keyBase, 0, keyBase.length, buffer, 0, bufferOffset, expire);
    // bufferPos has been advanced
    bufferPos += pageSize; // must be page size
    bufferOffset = 0;
  }
  
  @Override
  public void write(byte[] b) throws IOException {
    write(b, 0, b.length);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    checkClosed();
    int written = 0;
    while(written < len) {
      int avail = buffer.length - bufferOffset;
      int toWrite = Math.min(avail,  len - written);
      System.arraycopy(b, off + written, buffer, bufferOffset, toWrite);
      bufferOffset += toWrite;
      written += toWrite;
      if (bufferOffset == buffer.length) {
        nextPage();
      }
    }
  }

  @Override
  public void flush() throws IOException {
    if (this.ignoreFC) {
      return;
    }
    checkClosed();
  }

  @Override
  public void close() throws IOException {
    if (this.ignoreFC) {
      return;
    }
    forcedClose();
  }

  public void forcedClose() throws IOException {
    checkClosed();
    keyBase = getKey(bufferPos);
    if (bufferPos != 0 || bufferOffset != 0) {
      this.parent.put(keyBase, 0, keyBase.length, buffer, 0, bufferOffset, expire);
    }
    // Release buffer
    pool.offer(buffer);
    this.closed = true;
  }
  
  private void checkClosed() throws IOException {
    if (closed) {
      throw new IOException("Stream is closed");
    }
  }
}
