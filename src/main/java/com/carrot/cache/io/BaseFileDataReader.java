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

import com.carrot.cache.util.Utils;

public class BaseFileDataReader implements DataReader {

  @Override
  public void init(String cacheName) {
    // TODO Auto-generated method stub
    
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
      byte[] buffer,
      int bufOffset) throws IOException {
    //TODO prevent file from being closed/deleted
    FileIOEngine fileEngine = (FileIOEngine) engine;
    RandomAccessFile file = fileEngine.getFileFor(sid);
    if (file == null) {
      return IOEngine.NOT_FOUND;
    }
    
    if (file.length() < offset + size) {
      // Rare situation - wrong segment - hash collision
      return IOEngine.NOT_FOUND;
    }
    synchronized (file) {
      file.seek(offset);
      file.readFully(buffer, bufOffset, size);
    }
    // Now buffer contains both: key and value, we need to compare keys
    // Format of a key-value pair in a buffer: key-size, value-size, key, value
    int kSize = Utils.readUVInt(buffer, bufOffset);
    if (kSize != keySize) {
      return IOEngine.NOT_FOUND;
    }
    int kSizeSize = Utils.sizeUVInt(kSize);
    bufOffset += kSizeSize;
    int vSize = Utils.readUVInt(buffer, bufOffset);
    int vSizeSize = Utils.sizeUVInt(vSize);
    bufOffset += vSizeSize;

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
      int size,
      ByteBuffer buffer) throws IOException {
    
    FileIOEngine fileEngine = (FileIOEngine) engine;
    RandomAccessFile file = fileEngine.getFileFor(sid);
    int off = buffer.position();
    if (file == null) {
      return IOEngine.NOT_FOUND;
    }
    
    if (file.length() < offset + size) {
      // Rare situation - wrong segment - hash collision
      return IOEngine.NOT_FOUND;
    }
    synchronized (file) {
      FileChannel fc = file.getChannel();
      fc.position(offset);
      int read = 0;
      while(read < size) {
        read += fc.read(buffer);
      }
    }
    
    buffer.position(off);
    // Now buffer contains both: key and value, we need to compare keys
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
    // Now compare keys
    if (Utils.compareTo(buffer, keySize, key, keyOffset, keySize) == 0) {
      // If key is the same
      buffer.position(off + size);
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
      int bufOffset) throws IOException {
    FileIOEngine fileEngine = (FileIOEngine) engine;
    RandomAccessFile file = fileEngine.getFileFor(sid);
    if (file == null) {
      return IOEngine.NOT_FOUND;
    }
    
    if (file.length() < offset + size) {
      // Rare situation - wrong segment - hash collision
      return IOEngine.NOT_FOUND;
    }
    //TODO: can file be NULL?
    synchronized (file) {
      file.seek(offset);
      file.readFully(buffer, bufOffset, size);
    }
    // Now buffer contains both: key and value, we need to compare keys
    // Format of a key-value pair in a buffer: key-size, value-size, key, value
    int kSize = Utils.readUVInt(buffer, bufOffset);
    if (kSize != keySize) {
      return IOEngine.NOT_FOUND;
    }
    int kSizeSize = Utils.sizeUVInt(kSize);
    bufOffset += kSizeSize;
    int vSize = Utils.readUVInt(buffer, bufOffset);
    int vSizeSize = Utils.sizeUVInt(vSize);
    bufOffset += vSizeSize;

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
      IOEngine engine, long keyPtr, int keySize, int sid, 
      long offset, int size, ByteBuffer buffer) throws IOException {
    FileIOEngine fileEngine = (FileIOEngine) engine;
    RandomAccessFile file = fileEngine.getFileFor(sid);
    if (file == null) {
      return IOEngine.NOT_FOUND;
    }
    
    if (file.length() < offset + size) {
      // Rare situation - wrong segment - hash collision
      return IOEngine.NOT_FOUND;
    }
    int off = buffer.position();

    synchronized (file) {
      FileChannel fc = file.getChannel();
      fc.position(offset);
      int read = 0;
      while(read < size) {
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
    // Now compare keys
    if (Utils.compareTo(buffer, keySize, keyPtr, keySize) == 0) {
      // If key is the same
      buffer.position(off + size);
      return size;
    } else {
      return IOEngine.NOT_FOUND;
    }
  }
}
