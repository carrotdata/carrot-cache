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
package com.carrot.cache.io;

import java.io.IOException;
import java.io.InputStream;

import com.carrot.cache.Cache;
import com.carrot.cache.util.Utils;

public class CacheInputStream extends InputStream {
  
  private static int EXTRA_SPACE = 1 << 16; // 64KB for key
  /** Buffer to write data to */
  byte[] buffer;
  
  /** Current offset in the buffer */
  int bufferOffset;
  
  /** Buffer length */
  int bufferLength;
  
  /** Absolute position of a buffer start in the stream*/  
  long bufferPos;
  
  /** Key */
  byte[] keyBase;
  
  /** Parent cache */
  Cache parent;
  
  /** Page size - buffer size*/
  int pageSize;
  
  /** Buffer pool */
  static ByteBufferPool pool;
  
  /** Stream is closed */
  boolean closed = false;
  
  boolean EOS = false;
  
  public static CacheInputStream openStream(Cache parent, byte[] key, int off, int len)
      throws IOException {
    try {
      return new CacheInputStream(parent, key, off, len);
    } catch (IOException e) {
      // Stream does not exists
      return null;
    }
  }
  
  /**
   * Constructor 
   * @param parent cache
   * @param key stream key
   * @param expire 
   * @throws IOException 
   */
  private CacheInputStream(Cache parent, byte[] key, int off, int len) throws IOException {
    this.parent = parent;
    int poolSize = parent.getCacheConfig().getIOStoragePoolSize(parent.getName());
    if (pool == null) {
      synchronized(ByteBufferPool.class) {
        if (pool == null) {
          pool = new ByteBufferPool(poolSize);
        }
      }
    }
    this.pageSize = parent.getCacheConfig().getCacheStreamingSupportBufferSize(parent.getName());
    this.buffer = pool.poll();
    if (this.buffer == null) {
      this.buffer = new byte[pageSize + EXTRA_SPACE];
    }
    
    this.keyBase = new byte[len + Utils.SIZEOF_LONG];
    System.arraycopy(key, off, keyBase, 0, len);
    nextPage();
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

  private boolean nextPage() throws IOException {
    if (bufferPos / pageSize * pageSize != bufferPos) {
      return false;
    }
    // save current
    this.keyBase = getKey(bufferPos);  
    long size = this.parent.get(keyBase, 0, keyBase.length, true, buffer, 0);
    if (size < 0) {
      return false;
    }
    // bufferPos has been advanced
    bufferPos += (int) size; // must be page size
    bufferOffset = 0;
    bufferLength = (int) size;
    return true;
  }

  @Override
  public void close() throws IOException {
    checkClosed();
    this.closed = true;
    // Release buffer
    pool.offer(buffer);
  }

  private void checkClosed() throws IOException {
    if (closed) {
      throw new IOException("Stream is closed");
    }
  }

  @Override
  public int read() throws IOException {
    if (EOS) return -1;
    checkClosed();
    if (bufferOffset == bufferLength) {
      EOS = !nextPage();
    }
    if (EOS) return -1;
    return buffer[bufferOffset++];
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    if (EOS) return -1;
    checkClosed();
    int read = 0;
    while(read < len && !EOS) {
      int avail = bufferLength - bufferOffset;
      int toRead = Math.min(avail,  len - read);
      System.arraycopy(buffer, bufferOffset, b , off + read, toRead);
      bufferOffset += toRead;
      read += toRead;
      if (bufferOffset == bufferLength) {
        try {
          EOS = !nextPage();
        } catch (IOException e) {
          if (read > 0) {
            return read;
          }
          throw e;
        }
      }
    }
    return read > 0 || !EOS? read: -1;
  }
}
