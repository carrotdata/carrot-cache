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

import com.carrot.cache.util.Utils;
import static com.carrot.cache.util.Utils.getItemSize;
import static com.carrot.cache.util.Utils.getKeyOffset;

import static com.carrot.cache.util.IOUtils.readFully;

public class BaseFileDataReader implements DataReader {

  private final int blockSize = 4096;
  @Override
  public void init(String cacheName) {
  }

  @Override
  public int read(
      IOEngine engine,
      byte[] key,
      int keyOffset,
      int keySize,
      int sid,
      long offset,
      int size, // can be < 0 - unknown
      byte[] buffer,
      int bufOffset)
      throws IOException {

    int avail = buffer.length - bufOffset;
    // sanity check
    if (size < 0 && avail < 8) {
      return blockSize; // just in case
    }
    
    if (size > avail) {
      return size;
    }
    
    // TODO prevent file from being closed/deleted
    FileIOEngine fileEngine = (FileIOEngine) engine;
    RandomAccessFile file = fileEngine.getFileFor(sid);
    if (file == null) {
      return IOEngine.NOT_FOUND;
    }
    boolean loaded = false;
    if (size < 0) {
      int toRead = (int) Math.min(blockSize, file.length() - offset);
      toRead = Math.min(toRead, avail);
      readFully(file, offset, buffer, bufOffset, toRead);
      size = getItemSize(buffer, bufOffset);

      if (size > avail) {
        return size;
      }
      if (size < toRead) {
        loaded = true;
      }
    }
    
    if (file.length() < offset + size) {
      // Rare situation - wrong segment - hash collision
      return IOEngine.NOT_FOUND;
    }
    if (!loaded) {
      readFully(file, offset, buffer, bufOffset, size);
    }

    // Now buffer contains both: key and value, we need to compare keys
    // Format of a key-value pair in a buffer: key-size, value-size, key, value

    bufOffset += getKeyOffset(buffer, bufOffset);
    // Now compare keys
    if (Utils.compareTo(buffer, bufOffset, keySize, key, keyOffset, keySize) == 0) {
      // If key is the same
      return size;
    } else {
      return IOEngine.NOT_FOUND;
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
      int size, /* can be < 0*/
      ByteBuffer buffer)
      throws IOException {

    int avail = buffer.remaining();
    // Sanity check
    if (size > avail) {
      return size;
    }
    if (size < 0 && avail < 8) {
      return blockSize; // just in case
    }

    int pos = buffer.position();

    // TODO prevent file from being closed/deleted
    FileIOEngine fileEngine = (FileIOEngine) engine;
    RandomAccessFile file = fileEngine.getFileFor(sid);
    if (file == null) {
      return IOEngine.NOT_FOUND;
    }
    boolean loaded = false;

    if (size < 0) {
      // Get size of an item
      int toRead = (int) Math.min(blockSize, file.length() - offset);
      toRead = Math.min(toRead, avail);
      readFully(file, offset, buffer, toRead);
      size = getItemSize(buffer);
      if (size > avail) {
        return size;
      }
      if (size < toRead) {
        loaded = true;
      }
    }

    if (file.length() < offset + size) {
      // Rare situation - wrong segment - hash collision
      return IOEngine.NOT_FOUND;
    }

    if (!loaded) {
      buffer.position(pos);
      readFully(file, offset, buffer, size);
    }
    buffer.position(pos);

    // Now buffer contains both: key and value, we need to compare keys
    // Format of a key-value pair in a buffer: key-size, value-size, key, value

    int $off = getKeyOffset(buffer);
    buffer.position(pos + $off);

    // Now compare keys
    if (Utils.compareTo(buffer, keySize, key, keyOffset, keySize) == 0) {
      // If key is the same
      // TODO: position?
      buffer.position(pos + size);
      return size;
    } else {
      return IOEngine.NOT_FOUND;
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
      int bufOffset)
      throws IOException {
    int avail = buffer.length - bufOffset;
    // sanity check
    if (size < 0 && avail < 8) {
      return blockSize; // just in case
    }
    if (size > avail) {
      return size;
    }
    // TODO prevent file from being closed/deleted
    FileIOEngine fileEngine = (FileIOEngine) engine;
    RandomAccessFile file = fileEngine.getFileFor(sid);
    if (file == null) {
      return IOEngine.NOT_FOUND;
    }
    boolean loaded = false;
    if (size < 0) {
      int toRead = (int) Math.min(blockSize, file.length() - offset);
      toRead = Math.min(toRead, avail);
      readFully(file, offset, buffer, bufOffset, toRead);
      if (size > avail) {
        return size;
      }
      if (size < toRead) {
        loaded = true;
      }
    }
    if (file.length() < offset + size) {
      // Rare situation - wrong segment - hash collision
      return IOEngine.NOT_FOUND;
    }

    if (!loaded) {
      readFully(file, offset, buffer, bufOffset, size);
    }

    // Now buffer contains both: key and value, we need to compare keys
    // Format of a key-value pair in a buffer: key-size, value-size, key, value
    
    bufOffset += getKeyOffset(buffer, bufOffset);
    
    // Now compare keys
    if (Utils.compareTo(buffer, bufOffset, keySize, keyPtr, keySize) == 0) {
      // If key is the same
      return size;
    } else {
      return IOEngine.NOT_FOUND;
    }
  }

  @Override
  public int read(
      IOEngine engine, long keyPtr, int keySize, int sid, long offset, int size, ByteBuffer buffer)
      throws IOException {
    int avail = buffer.remaining();
    int pos = buffer.position();

    // sanity check
    if (size < 0 && avail < 8) {
      return blockSize; // just in case
    }
    if (size > avail) {
      return size;
    }
    // TODO prevent file from being closed/deleted
    FileIOEngine fileEngine = (FileIOEngine) engine;
    RandomAccessFile file = fileEngine.getFileFor(sid);
    if (file == null) {
      return IOEngine.NOT_FOUND;
    }
    boolean loaded = false;

    if (size < 0) {
      // Get size of an item
      int toRead = (int) Math.min(blockSize, file.length() - offset);
      toRead = Math.min(toRead, avail);
      readFully(file, offset, buffer, toRead);
      size = getItemSize(buffer);
      if (size > avail) {
        return size;
      }
      if (size < toRead) {
        loaded = true;
      }
    }

    if (file.length() < offset + size) {
      // Rare situation - wrong segment - hash collision
      return IOEngine.NOT_FOUND;
    }

    if (!loaded) {
      buffer.position(pos);
      readFully(file, offset, buffer, size);
    }
    buffer.position(pos);
    int $off = getKeyOffset(buffer);
    buffer.position(pos + $off);

    // Now compare keys
    if (Utils.compareTo(buffer, keySize, keyPtr, keySize) == 0) {
      // If key is the same
      buffer.position(pos + size);
      return size;
    } else {
      return IOEngine.NOT_FOUND;
    }
  }

  @Override
  public SegmentScanner getSegmentScanner(IOEngine engine, Segment s) throws IOException {
    RandomAccessFile file = ((FileIOEngine)engine).getFileFor(s.getId());
    int prefetchBuferSize = ((FileIOEngine)engine).getFilePrefetchBufferSize();
    return new BaseFileSegmentScanner(s, file, prefetchBuferSize);
  }
}
