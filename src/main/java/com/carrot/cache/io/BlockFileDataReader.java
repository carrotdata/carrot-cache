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

import static com.carrot.cache.util.BlockReaderWriterSupport.META_SIZE;
import static com.carrot.cache.util.BlockReaderWriterSupport.findInBlock;
import static com.carrot.cache.util.IOUtils.readFully;
import static com.carrot.cache.util.Utils.getKeyOffset;
import static com.carrot.cache.util.Utils.getItemSize;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.carrot.cache.util.CacheConfig;
import com.carrot.cache.util.UnsafeAccess;
import com.carrot.cache.util.Utils;

public class BlockFileDataReader implements DataReader {
  
  private ConcurrentLinkedQueue<byte[]> buffers = new ConcurrentLinkedQueue<>();
  
  private int blockSize;
  
  @Override
  public void init(String cacheName) {
    this.blockSize = CacheConfig.getInstance().getBlockWriterBlockSize(cacheName);      
  }

  @Override
  public int read(
      IOEngine engine,
      byte[] key,
      int keyOffset,
      int keySize,
      int sid,
      long offset,
      int size, // can be -1 (unknown)
      byte[] buffer,
      int bufOffset) throws IOException {
    
    int avail = buffer.length - bufOffset;
    // sanity check
    if (size > avail) {
      return size;
    }

    //TODO prevent file from being closed/deleted
    FileIOEngine fileEngine = (FileIOEngine) engine;
    RandomAccessFile file = fileEngine.getFileFor(sid);
    if (file == null) {
      //TODO: what kind of error is it?
      return IOEngine.NOT_FOUND;
    }
    if (size > 0 && file.length() < offset + size) {
      // Rare situation - wrong segment - hash collision
      return IOEngine.NOT_FOUND;
    }
    
    byte[] buf = getBuffer();
    int off = 0;
    try {
      readFully(file, offset, buf, 0, buf.length);
      int dataSize = UnsafeAccess.toInt(buf, 0);
      if (dataSize > blockSize - META_SIZE) {
        // means that this is a single item larger than a block
        if (dataSize > avail) {
          return dataSize;
        }
        System.arraycopy(buf, META_SIZE, buffer, bufOffset, buf.length - META_SIZE);
        releaseBuffer(buf);
        readFully(file, offset + blockSize, buffer, 
            bufOffset + blockSize - META_SIZE, dataSize - blockSize + META_SIZE);
        buf = buffer;
        off = bufOffset;
      } else {
        // Now buffer contains both: key and value, we need to compare keys
        // Format of a key-value pair in a buffer: key-size, value-size, key, value
        off = (int) findInBlock(buf, key, keyOffset, keySize);
        if (off < 0) {
          return IOEngine.NOT_FOUND;
        }
      }
      
      int itemSize = getItemSize(buf, off);
      int $off = getKeyOffset(buf, off);
      off += $off;
      
      // Now compare keys
      if (Utils.compareTo(buf, off, keySize, key, keyOffset, keySize) == 0) {
        // Copy data from buf to buffer
        if (buf != buffer) {
          off -= $off;
          System.arraycopy(buf,  off,  buffer, bufOffset, itemSize);
        }
        // If key is the same
        return itemSize;
      } else {
        return IOEngine.NOT_FOUND;
      }
    } finally {
      if (buf != buffer && buf != null) {
        releaseBuffer(buf);
      }
    }
  }

  @Override
  public int read(
      IOEngine engine,
      byte[] key,
      int keyOffset,
      int keySize,
      int sid,
      long offset,
      int size,
      ByteBuffer buffer)
      throws IOException {

    int avail = buffer.remaining();
    // sanity check
    if (size > avail) {
      return size;
    }
    // TODO prevent file from being closed/deleted
    FileIOEngine fileEngine = (FileIOEngine) engine;
    RandomAccessFile file = fileEngine.getFileFor(sid);
    if (file == null) {
      // TODO: what kind of error is it?
      return IOEngine.NOT_FOUND;
    }
    if (size > 0 && file.length() < offset + size) {
      // Rare situation - wrong segment - hash collision
      return IOEngine.NOT_FOUND;
    }

    byte[] buf = getBuffer();
    int pos = buffer.position();
    int off = pos;
    try {
      // TODO: make file read a separate method
      readFully(file, offset, buf, 0, buf.length);
      
      int dataSize = UnsafeAccess.toInt(buf, 0);
      if (dataSize > blockSize - META_SIZE) {
        // means that this is a single item larger than a block
        if (dataSize > avail) {
          return dataSize;
        }
        // copy from buf to buffer
        buffer.put(buf, META_SIZE, buf.length - META_SIZE);
        // Read the rest
        readFully(file, offset + blockSize, buffer, dataSize - blockSize + META_SIZE);
        
        buffer.position(off);
        // Format of a key-value pair in a buffer: key-size, value-size, key, value
        int itemSize = getItemSize(buffer);
        int $off = getKeyOffset(buf, off);
        buffer.position(buffer.position() + $off);
        // Now compare keys
        if (Utils.compareTo(buffer, keySize, key, keyOffset, keySize) == 0) {
          // If key is the same
          buffer.position(off + itemSize);
          return itemSize;
        } else {
          return IOEngine.NOT_FOUND;
        }
      } else {
        // Now buffer contains both: key and value, we need to compare keys
        // Format of a key-value pair in a buffer: key-size, value-size, key, value
        off = (int) findInBlock(buf, key, keyOffset, keySize);
        if (off < 0) {
          return IOEngine.NOT_FOUND;
        }

        int itemSize = getItemSize(buf, off);
        int $off = getKeyOffset(buf, off);
        off += $off;
        
        // Now compare keys
        if (Utils.compareTo(buf, off, keySize, key, keyOffset, keySize) == 0) {
          // If key is the same copy K-V to buffer
          off -= $off;
          buffer.put(buf, off, itemSize);
          //TODO:rewind to start?
          return itemSize;
        } else {
          return IOEngine.NOT_FOUND;
        }
      }
    } finally {
      if (buf != null) {
        releaseBuffer(buf);
      }
      buffer.position(off);
    }
  }

  @Override
  public int read(
      IOEngine engine,
      long keyPtr,
      int keySize,
      int sid,
      long offset,
      int size,
      byte[] buffer,
      int bufOffset) throws IOException {
    int avail = buffer.length - bufOffset;
    // sanity check
    if (size > avail) {
      return size;
    }
    //TODO prevent file from being closed/deleted
    FileIOEngine fileEngine = (FileIOEngine) engine;
    RandomAccessFile file = fileEngine.getFileFor(sid);
    if (file == null) {
      //TODO: what kind of error is it?
      return IOEngine.NOT_FOUND;
    }
    if (size > 0 && file.length() < offset + size) {
      // Rare situation - wrong segment - hash collision
      return IOEngine.NOT_FOUND;
    }
    
    byte[] buf = getBuffer();

    try {

      readFully(file, offset, buf, 0, buf.length);
      
      int off = bufOffset;
      int dataSize = UnsafeAccess.toInt(buf, 0);
      if (dataSize > blockSize - META_SIZE) {
        // means that this is a single item larger than a block
        if (dataSize > avail) {
          return dataSize;
        }
        System.arraycopy(buf, META_SIZE, buffer, bufOffset, buf.length - META_SIZE);
        releaseBuffer(buf);
        readFully(file, offset + blockSize, buffer, 
          bufOffset + blockSize - META_SIZE, dataSize - blockSize + META_SIZE);
        buf = buffer;
      } else {
        // Now buffer contains both: key and value, we need to compare keys
        // Format of a key-value pair in a buffer: key-size, value-size, key, value
        off = (int) findInBlock(buf, keyPtr, keySize);
        if (off < 0) {
          return IOEngine.NOT_FOUND;
        }
      }

      int itemSize = getItemSize(buf, off);
      int $off = getKeyOffset(buf, off);
      off += $off;
      
      // Now compare keys
      if (Utils.compareTo(buf, off, keySize, keyPtr, keySize) == 0) {
        // If key is the same
        if (buf != buffer) {
          off -= $off;
          System.arraycopy(buf,  off,  buffer, bufOffset, itemSize);
        }
        return itemSize;
      } else {
        return IOEngine.NOT_FOUND;
      }
    } finally {
      if (buf != buffer && buf != null) {
        releaseBuffer(buf);
      }
    }
  }

  @Override
  public int read(
      IOEngine engine, long keyPtr, int keySize, int sid, 
      long offset, int size, ByteBuffer buffer) throws IOException {
    int avail = buffer.remaining();
    // sanity check
    if (size > avail) {
      return size;
    }
    // TODO prevent file from being closed/deleted
    FileIOEngine fileEngine = (FileIOEngine) engine;
    RandomAccessFile file = fileEngine.getFileFor(sid);
    if (file == null) {
      // TODO: what kind of error is it?
      return IOEngine.NOT_FOUND;
    }
    if (file.length() < offset + size) {
      // Rare situation - wrong segment - hash collision
      return IOEngine.NOT_FOUND;
    }

    byte[] buf = getBuffer();
    int pos = buffer.position();
    int off = pos;
    try {
      // TODO: make file read a separate method

      readFully(file, offset, buf, 0, buf.length);
      
      int dataSize = UnsafeAccess.toInt(buf, 0);
      if (dataSize > blockSize - META_SIZE) {
        // means that this is a single item larger than a block
        if (dataSize > avail) {
          return dataSize;
        }

        // copy from buf to buffer
        buffer.put(buf, META_SIZE, buf.length - META_SIZE);

        // Read the rest
        readFully(file, offset + blockSize, buffer, dataSize - blockSize + META_SIZE);
        
        buffer.position(pos);
        // Format of a key-value pair in a buffer: key-size, value-size, key, value
        int itemSize = getItemSize(buffer);
        int $off = getKeyOffset(buf, off);
        buffer.position(pos + $off);
        
        // Now compare keys
        if (Utils.compareTo(buffer, keySize, keyPtr, keySize) == 0) {
          // If key is the same
          buffer.position(pos + itemSize);
          return itemSize;
        } else {
          return IOEngine.NOT_FOUND;
        }
      } else {
        // Now buffer contains both: key and value, we need to compare keys
        // Format of a key-value pair in a buffer: key-size, value-size, key, value
        off = (int) findInBlock(buf, keyPtr, keySize);
        if (off < 0) {
          return IOEngine.NOT_FOUND;
        }

        int itemSize = getItemSize(buf, off);
        int $off = getKeyOffset(buf, off);
        off += $off;
        
        // Now compare keys
        if (Utils.compareTo(buf, off, keySize, keyPtr, keySize) == 0) {
          // If key is the same copy K-V to buffer
          off -= $off;
          buffer.put(buf, off, itemSize);
          //TODO:rewind to start?
          return itemSize;
        } else {
          return IOEngine.NOT_FOUND;
        }
      }
    } finally {
      if (buf != null) {
        releaseBuffer(buf);
      }
      buffer.position(off);
    }
  }
  
  private byte[] getBuffer() {
    byte[] buffer = buffers.poll();
    if (buffer == null) {
      buffer = new byte[blockSize];
    }
    return buffer;
  }
  
  private void releaseBuffer(byte[] buffer) {
    buffers.offer(buffer);
  }
}
