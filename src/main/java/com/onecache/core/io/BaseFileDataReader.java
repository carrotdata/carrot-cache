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
package com.onecache.core.io;

import static com.onecache.core.io.IOUtils.readFully;
import static com.onecache.core.util.Utils.getItemSize;
import static com.onecache.core.util.Utils.getKeyOffset;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

import com.onecache.core.util.Utils;

public class BaseFileDataReader implements DataReader {

  private final int blockSize = 4096;
  @Override
  public void init(String cacheName) {
    //TODO init blockSize from config 
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
    // FIXME: Dirty hack
    offset += Segment.META_SIZE; // add 8 bytes to 
    
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
    // FIXME: Dirty hack
    offset += Segment.META_SIZE; // add 8 bytes to 
    
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
      buffer.position(pos);
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
    // FIXME: Dirty hack
    offset += Segment.META_SIZE; // add 8 bytes to 
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
    // FIXME: Dirty hack
    offset += Segment.META_SIZE; // add 8 bytes to 
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
      buffer.position(pos);
      return size;
    } else {
      return IOEngine.NOT_FOUND;
    }
  }

  @Override
  public int readValueRange(IOEngine engine, byte[] key, int keyOffset, int keySize, int sid,
      long offset, int size, byte[] buffer, int bufOffset, int rangeStart, int rangeSize)
      throws IOException {
    
    offset += Segment.META_SIZE; // add 8 bytes to the offset

    int avail = buffer.length - bufOffset;
    // Sanity check
    if (avail < 8) {
      // 8 bytes will allow to read at least key size and value size
      //TODO: is it safe? The caller might not expect this
      return blockSize;
    }

    // sanity check
    if (rangeSize > avail) {
      rangeSize = avail;
    }

    // TODO prevent file from being closed/deleted
    FileIOEngine fileEngine = (FileIOEngine) engine;
    RandomAccessFile file = fileEngine.getFileFor(sid);

    if (file == null) {
      return IOEngine.NOT_FOUND;
    }

    boolean loaded = false;
    int toRead = (int) Math.min(blockSize, file.length() - offset);
    toRead = Math.min(toRead, avail);
    
    readFully(file, offset, buffer, bufOffset, toRead);
    
    int valueSize = Utils.getValueSize(buffer, bufOffset);
    int valueOffset = Utils.getValueOffset(buffer, bufOffset);

    if (size < 0) {
      size = Utils.getItemSize(buffer, bufOffset);
    }
    
    if (file.length() < offset + size) {
      // Rare situation - wrong segment - hash collision
      return IOEngine.NOT_FOUND;
    }
    
    if (size < toRead) {
      loaded = true;
    }
    
    if(rangeStart > valueSize) {
      return IOEngine.NOT_FOUND;
    }
    
    if (rangeStart + rangeSize > valueSize) {
      rangeSize = valueSize - rangeStart;
    }
    
    // Now buffer contains both: key and value, we need to compare keys
    // Format of a key-value pair in a buffer: key-size, value-size, key, value
    int kSize = Utils.getKeySize(buffer, bufOffset);
    int kOffset = Utils.getKeyOffset(buffer, bufOffset);
    
    if (kSize != keySize) {
      // Hash collision
      return IOEngine.NOT_FOUND;
    }
    // Now compare keys
    if (Utils.compareTo(buffer, bufOffset + kOffset, kSize, key, keyOffset, keySize) != 0) {
      // Hash collision
      return IOEngine.NOT_FOUND;
    }
    
    if (!loaded) {
      readFully(file, offset + valueOffset + rangeStart, buffer, bufOffset, rangeSize);
    } else {
      Utils.extractValueRange(buffer, bufOffset, rangeStart, rangeSize);
    }
    
    return rangeSize;
  }

  @Override
  public int readValueRange(IOEngine engine, byte[] key, int keyOffset, int keySize, int sid,
      long offset, int size, ByteBuffer buffer, int rangeStart, int rangeSize) throws IOException {

    int pos = buffer.position();

    offset += Segment.META_SIZE; // add 8 bytes to the offset

    int avail = buffer.remaining();
    // Sanity check
    if (avail < 8) {
      // 8 bytes will allow to read at least key size and value size
      //TODO: is it safe? The caller might not expect this
      return blockSize;
    }

    // sanity check
    if (rangeSize > avail) {
      rangeSize = avail;
    }

    // TODO prevent file from being closed/deleted
    FileIOEngine fileEngine = (FileIOEngine) engine;
    RandomAccessFile file = fileEngine.getFileFor(sid);

    if (file == null) {
      return IOEngine.NOT_FOUND;
    }
    
    boolean loaded = false;

    int toRead = (int) Math.min(blockSize, file.length() - offset);
    toRead = Math.min(toRead, avail);
    
    readFully(file, offset, buffer,toRead);
    
    int valueSize = Utils.getValueSize(buffer);
    int valueOffset = Utils.getValueOffset(buffer);

    if (size < 0) {
      size = Utils.getItemSize(buffer);
    }
    
    if (file.length() < offset + size) {
      // Rare situation - wrong segment - hash collision
      return IOEngine.NOT_FOUND;
    }
    
    if (size < toRead) {
      loaded = true;
    }
    
    if(rangeStart > valueSize) {
      return IOEngine.NOT_FOUND;
    }
    
    if (rangeStart + rangeSize > valueSize) {
      rangeSize = valueSize - rangeStart;
    }
    
    // Now buffer contains both: key and value, we need to compare keys
    // Format of a key-value pair in a buffer: key-size, value-size, key, value
    int kSize = Utils.getKeySize(buffer);
    int kOffset = Utils.getKeyOffset(buffer);
    
    if (kSize != keySize) {
      // Hash collision
      return IOEngine.NOT_FOUND;
    }
    // Now compare keys
    buffer.position(pos + kOffset);
    if (Utils.compareTo(buffer, kSize, key, keyOffset, keySize) != 0) {
      // Hash collision
      return IOEngine.NOT_FOUND;
    }

    buffer.position(pos);

    if (!loaded) {
      readFully(file, offset + valueOffset + rangeStart, buffer, rangeSize);
    } else {
      Utils.extractValueRange(buffer, rangeStart, rangeSize);
    }
    buffer.position(pos);
    
    return rangeSize;
  }

  @Override
  public int readValueRange(IOEngine engine, long keyPtr, int keySize, int sid, long offset,
      int size, byte[] buffer, int bufOffset, int rangeStart, int rangeSize) throws IOException {
    offset += Segment.META_SIZE; // add 8 bytes to the offset

    int avail = buffer.length - bufOffset;
    // Sanity check
    if (avail < 8) {
      // 8 bytes will allow to read at least key size and value size
      //TODO: is it safe? The caller might not expect this
      return blockSize;
    }

    // sanity check
    if (rangeSize > avail) {
      rangeSize = avail;
    }

    // TODO prevent file from being closed/deleted
    FileIOEngine fileEngine = (FileIOEngine) engine;
    RandomAccessFile file = fileEngine.getFileFor(sid);

    if (file == null) {
      return IOEngine.NOT_FOUND;
    }

    boolean loaded = false;
    int toRead = (int) Math.min(blockSize, file.length() - offset);
    toRead = Math.min(toRead, avail);
    
    readFully(file, offset, buffer, bufOffset, toRead);
    
    int valueSize = Utils.getValueSize(buffer, bufOffset);
    int valueOffset = Utils.getValueOffset(buffer, bufOffset);

    if (size < 0) {
      size = Utils.getItemSize(buffer, bufOffset);
    }
    
    if (file.length() < offset + size) {
      // Rare situation - wrong segment - hash collision
      return IOEngine.NOT_FOUND;
    }
    
    if (size < toRead) {
      loaded = true;
    }
    
    if(rangeStart > valueSize) {
      return IOEngine.NOT_FOUND;
    }
    
    if (rangeStart + rangeSize > valueSize) {
      rangeSize = valueSize - rangeStart;
    }
    
    // Now buffer contains both: key and value, we need to compare keys
    // Format of a key-value pair in a buffer: key-size, value-size, key, value
    int kSize = Utils.getKeySize(buffer, bufOffset);
    int kOffset = Utils.getKeyOffset(buffer, bufOffset);
    
    if (kSize != keySize) {
      // Hash collision
      return IOEngine.NOT_FOUND;
    }
    // Now compare keys
    if (Utils.compareTo(buffer, bufOffset + kOffset, kSize, keyPtr, keySize) != 0) {
      // Hash collision
      return IOEngine.NOT_FOUND;
    }
    
    if (!loaded) {
      readFully(file, offset + valueOffset + rangeStart, buffer, bufOffset, rangeSize);
    } else {
      Utils.extractValueRange(buffer, bufOffset, rangeStart, rangeSize);
    }
    
    return rangeSize;
  }

  @Override
  public int readValueRange(IOEngine engine, long keyPtr, int keySize, int sid, long offset,
      int size, ByteBuffer buffer, int rangeStart, int rangeSize) throws IOException {
    int pos = buffer.position();

    offset += Segment.META_SIZE; // add 8 bytes to the offset

    int avail = buffer.remaining();
    // Sanity check
    if (avail < 8) {
      // 8 bytes will allow to read at least key size and value size
      //TODO: is it safe? The caller might not expect this
      return blockSize;
    }

    // sanity check
    if (rangeSize > avail) {
      rangeSize = avail;
    }

    // TODO prevent file from being closed/deleted
    FileIOEngine fileEngine = (FileIOEngine) engine;
    RandomAccessFile file = fileEngine.getFileFor(sid);

    if (file == null) {
      return IOEngine.NOT_FOUND;
    }
    
    boolean loaded = false;

    int toRead = (int) Math.min(blockSize, file.length() - offset);
    toRead = Math.min(toRead, avail);
    
    readFully(file, offset, buffer,toRead);
    
    int valueSize = Utils.getValueSize(buffer);
    int valueOffset = Utils.getValueOffset(buffer);

    if (size < 0) {
      size = Utils.getItemSize(buffer);
    }
    
    if (file.length() < offset + size) {
      // Rare situation - wrong segment - hash collision
      return IOEngine.NOT_FOUND;
    }
    
    if (size < toRead) {
      loaded = true;
    }
    
    if(rangeStart > valueSize) {
      return IOEngine.NOT_FOUND;
    }
    
    if (rangeStart + rangeSize > valueSize) {
      rangeSize = valueSize - rangeStart;
    }
    
    // Now buffer contains both: key and value, we need to compare keys
    // Format of a key-value pair in a buffer: key-size, value-size, key, value
    int kSize = Utils.getKeySize(buffer);
    int kOffset = Utils.getKeyOffset(buffer);
    
    if (kSize != keySize) {
      // Hash collision
      return IOEngine.NOT_FOUND;
    }
    // Now compare keys
    buffer.position(pos + kOffset);
    if (Utils.compareTo(buffer, kSize, keyPtr, keySize) != 0) {
      // Hash collision
      return IOEngine.NOT_FOUND;
    }

    buffer.position(pos);

    if (!loaded) {
      readFully(file, offset + valueOffset + rangeStart, buffer, rangeSize);
    } else {
      Utils.extractValueRange(buffer, rangeStart, rangeSize);
    }
    buffer.position(pos);
    
    return rangeSize;
  }

  @Override
  public SegmentScanner getSegmentScanner(IOEngine engine, Segment s) throws IOException {
    RandomAccessFile file = ((FileIOEngine)engine).getFileFor(s.getId());
    int prefetchBuferSize = ((FileIOEngine)engine).getFilePrefetchBufferSize();
    return new BaseFileSegmentScanner(s, file, prefetchBuferSize);
  }
}
