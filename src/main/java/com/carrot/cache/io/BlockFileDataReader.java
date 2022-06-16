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
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.carrot.cache.util.CacheConfig;
import com.carrot.cache.util.UnsafeAccess;
import com.carrot.cache.util.Utils;
import static com.carrot.cache.util.BlockReaderWriterSupport.*;

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
    //TODO prevent file from being closed/deleted
    FileIOEngine fileEngine = (FileIOEngine) engine;
    RandomAccessFile file = fileEngine.getFileFor(sid);
    if (file == null) {
      //TODO: what kind of error is it?
      return IOEngine.NOT_FOUND;
    }
    if (file.length() < offset + size) {
      // Rare situation - wrong segment - hash collision
      return IOEngine.NOT_FOUND;
    }
    
    byte[] buf = getBuffer();

    try {
      synchronized (file) {
        file.seek(offset);
        file.readFully(buf, 0, buf.length);
      }
      int off = bufOffset;
      int dataSize = UnsafeAccess.toInt(buf, 0);
      if (dataSize > blockSize - META_SIZE) {
        // means that this is a single item larger than a block
        if (dataSize > avail) {
          return dataSize;
        }
        System.arraycopy(buf, META_SIZE, buffer, bufOffset, buf.length - META_SIZE);
        releaseBuffer(buf);
        synchronized (file) {
          file.seek(offset + blockSize);
          file.readFully(buffer, bufOffset + blockSize - META_SIZE, dataSize - blockSize + META_SIZE);
        }
        buf = buffer;
      } else {
        // Now buffer contains both: key and value, we need to compare keys
        // Format of a key-value pair in a buffer: key-size, value-size, key, value
        off = (int) findInBlock(buf, key, keyOffset, keySize);
        if (off < 0) {
          return IOEngine.NOT_FOUND;
        }
      }

      int kSize = Utils.readUVInt(buf, off);
      if (kSize != keySize) {
        return IOEngine.NOT_FOUND;
      }
      int kSizeSize = Utils.sizeUVInt(kSize);
      off += kSizeSize;
      int vSize = Utils.readUVInt(buf, off);
      int vSizeSize = Utils.sizeUVInt(vSize);
      off += vSizeSize;
      int itemSize = kSize + kSizeSize + vSize + vSizeSize;
      // Now compare keys
      if (Utils.compareTo(buf, off, keySize, key, keyOffset, keySize) == 0) {
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
      synchronized (file) {
        file.seek(offset);
        file.readFully(buf, 0, buf.length);
      }

      int dataSize = UnsafeAccess.toInt(buf, 0);
      if (dataSize > blockSize - META_SIZE) {
        // means that this is a single item larger than a block
        if (dataSize > avail) {
          return dataSize;
        }

        // copy from buf to buffer
        buffer.put(buf, META_SIZE, buf.length - META_SIZE);

        // Read the rest
        synchronized (file) {
          FileChannel fc = file.getChannel();
          fc.position(offset + blockSize);
          int read = 0;
          while (read < dataSize - blockSize + META_SIZE) {
            read += fc.read(buffer);
          }
        }
        buffer.position(off);
        // Format of a key-value pair in a buffer: key-size, value-size, key, value
        int kSize = Utils.readUVInt(buffer);
        if (kSize != keySize) {
          return IOEngine.NOT_FOUND;
        }
        int kSizeSize = Utils.sizeUVInt(kSize);
        buffer.position(off + kSizeSize);

        int vSize = Utils.readUVInt(buffer);
        int vSizeSize = Utils.sizeUVInt(vSize);
        buffer.position(off + kSizeSize + vSizeSize);
        int itemSize = kSize + vSize + kSizeSize + vSizeSize;
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

        int kSize = Utils.readUVInt(buf, off);
        if (kSize != keySize) {
          return IOEngine.NOT_FOUND;
        }
        int kSizeSize = Utils.sizeUVInt(kSize);
        off += kSizeSize;
        int vSize = Utils.readUVInt(buf, off);
        int vSizeSize = Utils.sizeUVInt(vSize);
        off += vSizeSize;
        int itemSize = kSize + kSizeSize + vSize + vSizeSize;
        // Now compare keys
        if (Utils.compareTo(buf, off, keySize, key, keyOffset, keySize) == 0) {
          // If key is the same
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
    //TODO prevent file from being closed/deleted
    FileIOEngine fileEngine = (FileIOEngine) engine;
    RandomAccessFile file = fileEngine.getFileFor(sid);
    if (file == null) {
      //TODO: what kind of error is it?
      return IOEngine.NOT_FOUND;
    }
    if (file.length() < offset + size) {
      // Rare situation - wrong segment - hash collision
      return IOEngine.NOT_FOUND;
    }
    
    byte[] buf = getBuffer();

    try {
      synchronized (file) {
        file.seek(offset);
        file.readFully(buf, 0, buf.length);
      }
      int off = bufOffset;
      int dataSize = UnsafeAccess.toInt(buf, 0);
      if (dataSize > blockSize - META_SIZE) {
        // means that this is a single item larger than a block
        if (dataSize > avail) {
          return dataSize;
        }
        System.arraycopy(buf, META_SIZE, buffer, bufOffset, buf.length - META_SIZE);
        releaseBuffer(buf);
        synchronized (file) {
          file.seek(offset + blockSize);
          file.readFully(buffer, bufOffset + blockSize - META_SIZE, dataSize - blockSize + META_SIZE);
        }
        buf = buffer;
      } else {
        // Now buffer contains both: key and value, we need to compare keys
        // Format of a key-value pair in a buffer: key-size, value-size, key, value
        off = (int) findInBlock(buf, keyPtr, keySize);
        if (off < 0) {
          return IOEngine.NOT_FOUND;
        }
      }

      int kSize = Utils.readUVInt(buf, off);
      if (kSize != keySize) {
        return IOEngine.NOT_FOUND;
      }
      int kSizeSize = Utils.sizeUVInt(kSize);
      off += kSizeSize;
      int vSize = Utils.readUVInt(buf, off);
      int vSizeSize = Utils.sizeUVInt(vSize);
      off += vSizeSize;
      int itemSize = kSize + kSizeSize + vSize + vSizeSize;
      // Now compare keys
      if (Utils.compareTo(buf, off, keySize, keyPtr, keySize) == 0) {
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
      IOEngine engine, long keyPtr, int keySize, int sid, 
      long offset, int size, ByteBuffer buffer) throws IOException {
    int avail = buffer.remaining();
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
      synchronized (file) {
        file.seek(offset);
        file.readFully(buf, 0, buf.length);
      }

      int dataSize = UnsafeAccess.toInt(buf, 0);
      if (dataSize > blockSize - META_SIZE) {
        // means that this is a single item larger than a block
        if (dataSize > avail) {
          return dataSize;
        }

        // copy from buf to buffer
        buffer.put(buf, META_SIZE, buf.length - META_SIZE);

        // Read the rest
        synchronized (file) {
          FileChannel fc = file.getChannel();
          fc.position(offset + blockSize);
          int read = 0;
          while (read < dataSize - blockSize + META_SIZE) {
            read += fc.read(buffer);
          }
        }
        buffer.position(off);
        // Format of a key-value pair in a buffer: key-size, value-size, key, value
        int kSize = Utils.readUVInt(buffer);
        if (kSize != keySize) {
          return IOEngine.NOT_FOUND;
        }
        int kSizeSize = Utils.sizeUVInt(kSize);
        buffer.position(off + kSizeSize);

        int vSize = Utils.readUVInt(buffer);
        int vSizeSize = Utils.sizeUVInt(vSize);
        buffer.position(off + kSizeSize + vSizeSize);
        int itemSize = kSize + vSize + kSizeSize + vSizeSize;
        // Now compare keys
        if (Utils.compareTo(buffer, keySize, keyPtr, keySize) == 0) {
          // If key is the same
          buffer.position(off + itemSize);
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

        int kSize = Utils.readUVInt(buf, off);
        if (kSize != keySize) {
          return IOEngine.NOT_FOUND;
        }
        int kSizeSize = Utils.sizeUVInt(kSize);
        off += kSizeSize;
        int vSize = Utils.readUVInt(buf, off);
        int vSizeSize = Utils.sizeUVInt(vSize);
        off += vSizeSize;
        int itemSize = kSize + kSizeSize + vSize + vSizeSize;
        // Now compare keys
        if (Utils.compareTo(buf, off, keySize, keyPtr, keySize) == 0) {
          // If key is the same
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
