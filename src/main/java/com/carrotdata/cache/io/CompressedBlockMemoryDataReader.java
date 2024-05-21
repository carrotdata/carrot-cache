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

import static com.carrotdata.cache.compression.CompressionCodec.COMP_META_SIZE;
import static com.carrotdata.cache.compression.CompressionCodec.COMP_SIZE_OFFSET;
import static com.carrotdata.cache.compression.CompressionCodec.DICT_VER_OFFSET;
import static com.carrotdata.cache.compression.CompressionCodec.SIZE_OFFSET;
import static com.carrotdata.cache.io.BlockReaderWriterSupport.META_SIZE;
import static com.carrotdata.cache.util.Utils.getItemSize;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.carrotdata.cache.compression.CodecFactory;
import com.carrotdata.cache.compression.CompressionCodec;
import com.carrotdata.cache.util.UnsafeAccess;
import com.carrotdata.cache.util.Utils;

public class CompressedBlockMemoryDataReader implements DataReader {
  private static final Logger LOG = LoggerFactory.getLogger(CompressedBlockMemoryDataReader.class);

  private static int INIT_BUFFER_SIZE = 1 << 16;
  
  private static ThreadLocal<byte[]> buffers = new ThreadLocal<byte[]>() {
    @Override
    protected byte[] initialValue() {
      return new byte[INIT_BUFFER_SIZE];
    }
  };
  
  private static void checkBuffer(int required) {
    byte[] buf = buffers.get();
    if (buf.length < required) {
      buf = new byte[required];
      buffers.set(buf);
    }
  }
  private String cacheName;
  
  private CompressionCodec codec;
  
  public CompressedBlockMemoryDataReader() {
  }

  @Override
  public void init(String cacheName) {
    // Can be null on initialization
    this.codec = CodecFactory.getInstance().getCompressionCodecForCache(cacheName);
    this.cacheName = cacheName;
  }
  
  private void checkCodec() {
    if (this.codec == null) {
      this.codec = CodecFactory.getInstance().getCompressionCodecForCache(cacheName);
      if (this.codec == null) {
        throw new RuntimeException(String.format("Codec type is undefined for cache '%s'", cacheName));
      }
    }
  }

  @Override
  public int read(IOEngine engine, byte[] key, int keyOffset, int keySize, int sid, long offset,
      int size, // TODO size can be -1
      byte[] buffer, int bufOffset) {
    
    checkCodec();
    int avail = buffer.length - bufOffset;
    if (size > avail) {
      return size;
    }
    // Segment read lock is already held by this thread
    Segment s = engine.getSegmentById(sid);
    if (s == null) {
      // TODO: error
      return IOEngine.NOT_FOUND;
    }

    if (!s.isOffheap()) {
      return IOEngine.NOT_FOUND;
    }
    long ptr = s.getAddress();
    
    int uncompressedSize = UnsafeAccess.toInt(ptr + offset + SIZE_OFFSET);
    int dictVersion = UnsafeAccess.toInt(ptr + offset + DICT_VER_OFFSET);
    int compressedSize = UnsafeAccess.toInt(ptr + offset + COMP_SIZE_OFFSET);
    // sanity check 
    //TODO: add to corrupted reads
    if (uncompressedSize <= 0 || compressedSize < 0) {
      return IOEngine.READ_ERROR;
    }
    long segSize = s.getSegmentDataSize();
    if (uncompressedSize > segSize || compressedSize > segSize) {
      return IOEngine.READ_ERROR;
    }
    if (dictVersion >= 0) {
      // TODO: sanity check on values
      if (segSize < offset + COMP_META_SIZE + compressedSize) {
        return IOEngine.READ_ERROR;
      }
      checkBuffer(uncompressedSize);
      // Decompress
      byte[] buf = buffers.get();
      int dsize = codec.decompress(ptr + offset + COMP_META_SIZE, compressedSize, buf, dictVersion);
      if (dsize == 0) {
        // dictionary not found
        return IOEngine.NOT_FOUND;
      }
      // Find key-value in the buffer
      int offAdj = -META_SIZE;
      int off = (int) BlockReaderWriterSupport.findInBlock(buf, offAdj, dsize, key, keyOffset, keySize);
      if (off < 0) {
        return IOEngine.NOT_FOUND;
      }
      size = getItemSize(buf, off);
      if (size > avail) {
        return size;
      }
      UnsafeAccess.copy(buf, off, buffer, bufOffset, size);
    } else if (dictVersion == -1) { // Block is not compressed
      if (segSize < offset + COMP_META_SIZE + uncompressedSize) {
        return IOEngine.READ_ERROR;
      }
      // Find key-value in the buffer
      int offAdj = COMP_META_SIZE - META_SIZE;
      
      long addr = BlockReaderWriterSupport.findInBlock(ptr + offset + offAdj, uncompressedSize, key, keyOffset, keySize);
      
      if (addr < 0) {
        return IOEngine.NOT_FOUND;
      }
      size = getItemSize(addr);
      if (size > avail) {
        return size;
      }
      if (segSize < offset + size) {
      // Rare situation - wrong segment - hash collision
        return IOEngine.READ_ERROR;
      }
      UnsafeAccess.copy(addr, buffer, bufOffset, size);
    } else {
      // Race condition with Scavenger
      //TODO: add to corrupted reads
      return IOEngine.READ_ERROR;
    }
    return size;
  }

  @Override
  public int read(IOEngine engine, byte[] key, int keyOffset, int keySize, int sid, long offset,
      int size, // can be < 0 - unknown
      ByteBuffer buffer) {
    // Race condition. Get location from index, get segment (can be different)
    checkCodec();
    int avail = buffer.remaining();
    if (size > avail) {
      return size;
    }
    // Segment read lock is already held by this thread
    Segment s = engine.getSegmentById(sid);
    if (s == null) {
      // TODO: error
      return IOEngine.NOT_FOUND;
    }

    if (!s.isOffheap()) {
      return IOEngine.NOT_FOUND;
    }
    long ptr = s.getAddress();
    int uncompressedSize = UnsafeAccess.toInt(ptr + offset + SIZE_OFFSET);
    int dictVersion = UnsafeAccess.toInt(ptr + offset + DICT_VER_OFFSET);
    int compressedSize = UnsafeAccess.toInt(ptr + offset + COMP_SIZE_OFFSET);
    // sanity check
    if (uncompressedSize <= 0 ||compressedSize < 0) {
      return IOEngine.READ_ERROR;
    }
    long segSize = s.getSegmentDataSize();
    if (uncompressedSize > segSize || compressedSize > segSize) {
      return IOEngine.READ_ERROR;
    }
    if (dictVersion >= 0) {
      // TODO: sanity check on values
      if (segSize < offset + COMP_META_SIZE + compressedSize) {
        return IOEngine.READ_ERROR;
      }
      checkBuffer(uncompressedSize);
      // Decompress
      byte[] buf = buffers.get();
      int dsize = codec.decompress(ptr + offset + COMP_META_SIZE, compressedSize, buf, dictVersion);
      if (dsize == 0) {
        return IOEngine.NOT_FOUND;
      }
      // Find key-value in the buffer
      int offAdj = - META_SIZE;
      int off = (int) BlockReaderWriterSupport.findInBlock(buf, offAdj, dsize, key, keyOffset, keySize);
      if (off < 0) {
        return IOEngine.NOT_FOUND;
      }
      size = getItemSize(buf, off);
      if (size > avail) {
        return size;
      }
      int pos = buffer.position();
      UnsafeAccess.copy(buf, off, buffer, pos, size);
      buffer.position(pos);
    } else if (dictVersion == -1){ // Block is not compressed
      if (segSize < offset + COMP_META_SIZE + uncompressedSize) {
        return IOEngine.READ_ERROR;
      }
      // Find key-value in the buffer
      int offAdj = COMP_META_SIZE - META_SIZE;
      long addr = BlockReaderWriterSupport.findInBlock(ptr + offset + offAdj, uncompressedSize, key, keyOffset, keySize);
      if (addr < 0) {
        return IOEngine.NOT_FOUND;
      }
      size = getItemSize(addr);
      if (size > avail) {
        return size;
      }
      // TODO: remove this?
       if (segSize < offset + size) {
         // Rare situation - wrong segment - hash collision
         return IOEngine.READ_ERROR;
       }
      int pos = buffer.position();
      UnsafeAccess.copy(addr, buffer, size);
      buffer.position(pos);
    } else {
      return IOEngine.READ_ERROR;
    }
    return size;
  }

  @Override
  public int read(IOEngine engine, long keyPtr, int keySize, int sid, long offset, int size,
      byte[] buffer, int bufOffset) {
    checkCodec();
    int avail = buffer.length - bufOffset;
    if (size > avail) {
      return size;
    }
    // Segment read lock is already held by this thread
    Segment s = engine.getSegmentById(sid);
    if (s == null) {
      // TODO: error
      return IOEngine.NOT_FOUND;
    }

    if (!s.isOffheap()) {
      return IOEngine.NOT_FOUND;
    }
    long ptr = s.getAddress();
    
    int uncompressedSize = UnsafeAccess.toInt(ptr + offset + SIZE_OFFSET);
    int dictVersion = UnsafeAccess.toInt(ptr + offset + DICT_VER_OFFSET);
    int compressedSize = UnsafeAccess.toInt(ptr + offset + COMP_SIZE_OFFSET);
    // sanity check
    if (uncompressedSize <= 0 || compressedSize < 0) {
      return IOEngine.READ_ERROR;
    }
    long segSize = s.getSegmentDataSize();
    if (uncompressedSize > segSize || compressedSize > segSize) {
      return IOEngine.READ_ERROR;
    }
    if (dictVersion >= 0) {
      // TODO: sanity check on values
      if (segSize < offset + COMP_META_SIZE + compressedSize) {
        return IOEngine.READ_ERROR;
      }
      checkBuffer(uncompressedSize);
      // Decompress
      byte[] buf = buffers.get();
      int dsize = codec.decompress(ptr + offset + COMP_META_SIZE, compressedSize, buf, dictVersion);
      if (dsize == 0) {
        // dictionary not found
        return IOEngine.NOT_FOUND;
      }
      // Find key-value in the buffer
      int offAdj = -META_SIZE;
      int off = (int) BlockReaderWriterSupport.findInBlock(buf, offAdj, dsize, keyPtr, keySize);
      if (off < 0) {
        return IOEngine.NOT_FOUND;
      }
      size = getItemSize(buf, off);
      if (size > avail) {
        return size;
      }
      UnsafeAccess.copy(buf, off, buffer, bufOffset, size);
    } else if (dictVersion == -1){ // Block is not compressed
      if (segSize < offset + COMP_META_SIZE + uncompressedSize) {
        return IOEngine.READ_ERROR;
      }
      // Find key-value in the buffer
      int offAdj = COMP_META_SIZE - META_SIZE;
      long addr = BlockReaderWriterSupport.findInBlock(ptr + offset + offAdj, uncompressedSize, keyPtr, keySize);
      if (addr < 0) {
        return IOEngine.NOT_FOUND;
      }
      size = getItemSize(addr);
      if (size > avail) {
        return size;
      }
      // TODO: remove this?
      if (segSize < offset + size) {
        // Rare situation - wrong segment - hash collision
        return IOEngine.READ_ERROR;
      }
      UnsafeAccess.copy(addr, buffer, bufOffset, size);
    } else {
      return IOEngine.READ_ERROR;
    }
    return size;
  }

  @Override
  public int read(IOEngine engine, long keyPtr, int keySize, int sid, long offset, int size,
      ByteBuffer buffer) {

    // Race condition. Get location from index, get segment (can be different)
    checkCodec();
    int avail = buffer.remaining();
    if (size > avail) {
      return size;
    }
    // Segment read lock is already held by this thread
    Segment s = engine.getSegmentById(sid);
    if (s == null) {
      // TODO: error
      return IOEngine.NOT_FOUND;
    }

    if (!s.isOffheap()) {
      return IOEngine.NOT_FOUND;
    }
    long ptr = s.getAddress();
    int uncompressedSize = UnsafeAccess.toInt(ptr + offset + SIZE_OFFSET);
    int dictVersion = UnsafeAccess.toInt(ptr + offset + DICT_VER_OFFSET);
    int compressedSize = UnsafeAccess.toInt(ptr + offset + COMP_SIZE_OFFSET);
    // sanity check
    if (uncompressedSize <= 0 || compressedSize < 0) {
      return IOEngine.READ_ERROR;
    }
    long segSize = s.getSegmentDataSize();
    if (uncompressedSize > segSize || compressedSize > segSize) {
      return IOEngine.READ_ERROR;
    }
    if (dictVersion >= 0) {
      // TODO: sanity check on values
      if (segSize < offset + COMP_META_SIZE + compressedSize) {
        return IOEngine.READ_ERROR;
      }
      checkBuffer(uncompressedSize);
      // Decompress
      byte[] buf = buffers.get();
      int dsize = codec.decompress(ptr + offset + COMP_META_SIZE, compressedSize, buf, dictVersion);
      if (dsize == 0) {
        return IOEngine.NOT_FOUND;
      }
      // Find key-value in the buffer
      int offAdj = - META_SIZE;
      int off = (int) BlockReaderWriterSupport.findInBlock(buf, offAdj, dsize, keyPtr, keySize);
      if (off < 0) {
        return IOEngine.NOT_FOUND;
      }
      size = getItemSize(buf, off);
      if (size > avail) {
        return size;
      }
      int pos = buffer.position();
      UnsafeAccess.copy(buf, off, buffer, pos, size);
      buffer.position(pos);
    } else if (dictVersion == -1){ // Block is not compressed
      // TODO: sanity check on values
      if (segSize < offset + COMP_META_SIZE + uncompressedSize) {
        return IOEngine.READ_ERROR;
      }
      // Find key-value in the buffer
      int offAdj = COMP_META_SIZE - META_SIZE;
      long addr = 0;
      try {
        addr = BlockReaderWriterSupport.findInBlock(ptr + offset + offAdj, uncompressedSize, keyPtr, keySize);
      } catch (Exception e) {
        LOG.error(" : ptr="+ ptr + " usize="+ uncompressedSize + 
          " cSize=" + compressedSize + " dictVersion=" + dictVersion + " offset=" + offset+ " segmentSize="+ s.getSegmentDataSize()+ 
          " valid=" + s.isValid(), e);
        Thread.dumpStack();
        System.exit(-1);;
        //throw new RuntimeException();
      }
      if (addr < 0) {
        return IOEngine.NOT_FOUND;
      }
      size = getItemSize(addr);
      if (size > avail) {
        return size;
      }
      // TODO: remove this?
      if (s.getSegmentDataSize() < offset + size) {
        // Rare situation - wrong segment - hash collision
        return IOEngine.READ_ERROR;
      }
      int pos = buffer.position();
      UnsafeAccess.copy(addr, buffer, size);
      buffer.position(pos);
    } else {
      return IOEngine.READ_ERROR;
    }
    return size;
  }

  @Override
  public int readValueRange(IOEngine engine, byte[] key, int keyOffset, int keySize, int sid,
      long offset, int size, byte[] buffer, int bufOffset, int rangeStart, int rangeSize)
      throws IOException {
    
    checkCodec();
    int avail = buffer.length - bufOffset;

    if (rangeSize > avail) {
      rangeSize = avail;
    }

    // Segment read lock is already held by this thread
    Segment s = engine.getSegmentById(sid);
    if (s == null) {
      // TODO: error
      return IOEngine.NOT_FOUND;
    }

    if (!s.isOffheap()) {
      return IOEngine.NOT_FOUND;
    }
    long ptr = s.getAddress();
    
    int uncompressedSize = UnsafeAccess.toInt(ptr + offset + SIZE_OFFSET);
    int dictVersion = UnsafeAccess.toInt(ptr + offset + DICT_VER_OFFSET);
    int compressedSize = UnsafeAccess.toInt(ptr + offset + COMP_SIZE_OFFSET);
    // sanity check 
    //TODO: add to corrupted reads
    if (uncompressedSize <= 0 || compressedSize < 0) {
      return IOEngine.READ_ERROR;
    }
    long segSize = s.getSegmentDataSize();
    if (uncompressedSize > segSize || compressedSize > segSize) {
      return IOEngine.READ_ERROR;
    }
    if (dictVersion >= 0) {
      // TODO: sanity check on values
      if (segSize < offset + COMP_META_SIZE + compressedSize) {
        return IOEngine.READ_ERROR;
      }      
      checkBuffer(uncompressedSize);
      // Decompress
      byte[] buf = buffers.get();
      int dsize = codec.decompress(ptr + offset + COMP_META_SIZE, compressedSize, buf, dictVersion);
      if (dsize == 0) {
        return IOEngine.NOT_FOUND;
      }

      // Find key-value in the buffer
      int offAdj = -META_SIZE;
      int off = (int) BlockReaderWriterSupport.findInBlock(buf, offAdj, dsize, key, keyOffset, keySize);
      if (off < 0) {
        return IOEngine.NOT_FOUND;
      }

      int valueSize = Utils.getValueSize(buf, off);

      if (valueSize < rangeStart) {
        // TODO: better handling
        return IOEngine.NOT_FOUND;
      }

      if (valueSize < rangeStart + rangeSize) {
        rangeSize = valueSize - rangeStart;
      }

      int valueOffset = Utils.getValueOffset(buf, off);

      valueOffset += rangeStart;

      UnsafeAccess.copy(buf, off + valueOffset, buffer, bufOffset, rangeSize);
      // Now buffer contains both: key and value, we need to compare keys
      // Format of a key-value pair in a buffer: key-size, value-size, key, value

    } else if (dictVersion == -1) {
      // TODO: sanity check on values
      if (segSize < offset + COMP_META_SIZE + uncompressedSize) {
        return IOEngine.READ_ERROR;
      }
      // Find key-value in the buffer
      int offAdj = COMP_META_SIZE - META_SIZE;
      long addr = BlockReaderWriterSupport.findInBlock(ptr + offset + offAdj, uncompressedSize, key, keyOffset, keySize);
      if (addr < 0) {
        return IOEngine.NOT_FOUND;
      }
      
      int valueSize = Utils.getValueSize(addr);

      if (valueSize < rangeStart) {
        // TODO: better handling
        return IOEngine.NOT_FOUND;
      }

      if (valueSize < rangeStart + rangeSize) {
        rangeSize = valueSize - rangeStart;
      }

      int valueOffset = Utils.getValueOffset(addr);

      valueOffset += rangeStart;

      if (segSize < offset + valueOffset + rangeSize) {
        // Rare situation - wrong segment - hash collision
        return IOEngine.READ_ERROR;
      }
      UnsafeAccess.copy(addr + valueOffset, buffer, bufOffset, rangeSize);
      // Now buffer contains both: key and value, we need to compare keys
      // Format of a key-value pair in a buffer: key-size, value-size, key, value
    } else {
      return IOEngine.READ_ERROR;
    }
    return rangeSize;

  }

  @Override
  public int readValueRange(IOEngine engine, byte[] key, int keyOffset, int keySize, int sid,
      long offset, int size, ByteBuffer buffer, int rangeStart, int rangeSize) throws IOException {
    checkCodec();
    int avail = buffer.remaining();

    if (rangeSize > avail) {
      rangeSize = avail;
    }

    // Segment read lock is already held by this thread
    Segment s = engine.getSegmentById(sid);
    if (s == null) {
      // TODO: error
      return IOEngine.NOT_FOUND;
    }

    if (!s.isOffheap()) {
      return IOEngine.NOT_FOUND;
    }
    long ptr = s.getAddress();
    
    int uncompressedSize = UnsafeAccess.toInt(ptr + offset + SIZE_OFFSET);
    int dictVersion = UnsafeAccess.toInt(ptr + offset + DICT_VER_OFFSET);
    int compressedSize = UnsafeAccess.toInt(ptr + offset + COMP_SIZE_OFFSET);
    int pos = buffer.position();
    //TODO: add to corrupted reads
    if (uncompressedSize <= 0 || compressedSize < 0) {
      return IOEngine.READ_ERROR;
    }
    long segSize = s.getSegmentDataSize();
    if (uncompressedSize > segSize || compressedSize > segSize) {
      return IOEngine.READ_ERROR;
    }
    if (dictVersion >= 0) {
      // TODO: sanity check on values
      if (segSize < offset + COMP_META_SIZE + compressedSize) {
        return IOEngine.READ_ERROR;
      }   
      checkBuffer(uncompressedSize);
      // Decompress
      byte[] buf = buffers.get();
      int dsize = codec.decompress(ptr + offset + COMP_META_SIZE, compressedSize, buf, dictVersion);
      if (dsize == 0) {
        return IOEngine.NOT_FOUND;
      }
      // Find key-value in the buffer
      int offAdj = -META_SIZE;
      int off = (int) BlockReaderWriterSupport.findInBlock(buf, offAdj, dsize, key, keyOffset, keySize);
      if (off < 0) {
        return IOEngine.NOT_FOUND;
      }
      int valueSize = Utils.getValueSize(buf, off);
      if (valueSize < rangeStart) {
        // TODO: better handling
        return IOEngine.NOT_FOUND;
      }
      if (valueSize < rangeStart + rangeSize) {
        rangeSize = valueSize - rangeStart;
      }
      int valueOffset = Utils.getValueOffset(buf, off);
      valueOffset += rangeStart;
      UnsafeAccess.copy(buf, off + valueOffset, buffer, pos, rangeSize);
    } else if (dictVersion == -1){
      if (segSize < offset + COMP_META_SIZE + uncompressedSize) {
        return IOEngine.READ_ERROR;
      }  
      // Find key-value in the buffer
      int offAdj = COMP_META_SIZE - META_SIZE;
      long addr = BlockReaderWriterSupport.findInBlock(ptr + offset + offAdj, uncompressedSize, key, keyOffset, keySize);
      if (addr < 0) {
        return IOEngine.NOT_FOUND;
      }      
      int valueSize = Utils.getValueSize(addr);

      if (valueSize < rangeStart) {
        // TODO: better handling
        return IOEngine.NOT_FOUND;
      }
      if (valueSize < rangeStart + rangeSize) {
        rangeSize = valueSize - rangeStart;
      }
      int valueOffset = Utils.getValueOffset(addr);
      valueOffset += rangeStart;
      if (segSize < offset + valueOffset + rangeSize) {
        // Rare situation - wrong segment - hash collision
        return IOEngine.READ_ERROR;
      }
      UnsafeAccess.copy(addr + valueOffset, buffer,  rangeSize);
    } else {
      return IOEngine.READ_ERROR;
    }
    buffer.position(pos);
    return rangeSize;
  }

  @Override
  public int readValueRange(IOEngine engine, long keyPtr, int keySize, int sid, long offset,
      int size, byte[] buffer, int bufOffset, int rangeStart, int rangeSize) throws IOException {
    checkCodec();
    int avail = buffer.length - bufOffset;

    if (rangeSize > avail) {
      rangeSize = avail;
    }

    // Segment read lock is already held by this thread
    Segment s = engine.getSegmentById(sid);
    if (s == null) {
      // TODO: error
      return IOEngine.NOT_FOUND;
    }

    if (!s.isOffheap()) {
      return IOEngine.NOT_FOUND;
    }
    long ptr = s.getAddress();
    
    int uncompressedSize = UnsafeAccess.toInt(ptr + offset + SIZE_OFFSET);
    int dictVersion = UnsafeAccess.toInt(ptr + offset + DICT_VER_OFFSET);
    int compressedSize = UnsafeAccess.toInt(ptr + offset + COMP_SIZE_OFFSET);
    //TODO: add to corrupted reads
    if (uncompressedSize <= 0 || compressedSize < 0) {
      return IOEngine.READ_ERROR;
    }
    long segSize = s.getSegmentDataSize();
    if (uncompressedSize > segSize || compressedSize > segSize) {
      return IOEngine.READ_ERROR;
    }
    if (dictVersion >= 0) {
      // TODO: sanity check on values
      if (segSize < offset + COMP_META_SIZE + compressedSize) {
        return IOEngine.READ_ERROR;
      }  
      checkBuffer(uncompressedSize);
      // Decompress
      byte[] buf = buffers.get();
      int dsize = codec.decompress(ptr + offset + COMP_META_SIZE, compressedSize, buf, dictVersion);
      if (dsize == 0) {
        return IOEngine.NOT_FOUND;
      }

      // Find key-value in the buffer
      int offAdj = -META_SIZE;
      int off = (int) BlockReaderWriterSupport.findInBlock(buf, offAdj, dsize, keyPtr, keySize);
      if (off < 0) {
        return IOEngine.NOT_FOUND;
      }

      int valueSize = Utils.getValueSize(buf, off);

      if (valueSize < rangeStart) {
        // TODO: better handling
        return IOEngine.NOT_FOUND;
      }

      if (valueSize < rangeStart + rangeSize) {
        rangeSize = valueSize - rangeStart;
      }

      int valueOffset = Utils.getValueOffset(buf, off);

      valueOffset += rangeStart;

      UnsafeAccess.copy(buf, off + valueOffset, buffer, bufOffset, rangeSize);
      // Now buffer contains both: key and value, we need to compare keys
      // Format of a key-value pair in a buffer: key-size, value-size, key, value

    } else if (dictVersion == -1) {
      // TODO: sanity check on values
      if (segSize < offset + COMP_META_SIZE + uncompressedSize) {
        return IOEngine.READ_ERROR;
      }  
      // Find key-value in the buffer
      int offAdj = COMP_META_SIZE - META_SIZE;
      long addr = BlockReaderWriterSupport.findInBlock(ptr + offset + offAdj, uncompressedSize, keyPtr, keySize);
      if (addr < 0) {
        return IOEngine.NOT_FOUND;
      }
      
      int valueSize = Utils.getValueSize(addr);

      if (valueSize < rangeStart) {
        // TODO: better handling
        return IOEngine.NOT_FOUND;
      }

      if (valueSize < rangeStart + rangeSize) {
        rangeSize = valueSize - rangeStart;
      }

      int valueOffset = Utils.getValueOffset(addr);

      valueOffset += rangeStart;

      if (segSize < offset + valueOffset + rangeSize) {
        // Rare situation - wrong segment - hash collision
        return IOEngine.READ_ERROR;
      }
      UnsafeAccess.copy(addr + valueOffset, buffer, bufOffset, rangeSize);
      // Now buffer contains both: key and value, we need to compare keys
      // Format of a key-value pair in a buffer: key-size, value-size, key, value

    } else {
      return IOEngine.READ_ERROR;
    }
    return rangeSize;
  }

  @Override
  public int readValueRange(IOEngine engine, long keyPtr, int keySize, int sid, long offset,
      int size, ByteBuffer buffer, int rangeStart, int rangeSize) throws IOException {
    checkCodec();
    int avail = buffer.remaining();

    if (rangeSize > avail) {
      rangeSize = avail;
    }

    // Segment read lock is already held by this thread
    Segment s = engine.getSegmentById(sid);
    if (s == null) {
      // TODO: error
      return IOEngine.NOT_FOUND;
    }

    if (!s.isOffheap()) {
      return IOEngine.NOT_FOUND;
    }
    long ptr = s.getAddress();
    
    int uncompressedSize = UnsafeAccess.toInt(ptr + offset + SIZE_OFFSET);
    int dictVersion = UnsafeAccess.toInt(ptr + offset + DICT_VER_OFFSET);
    int compressedSize = UnsafeAccess.toInt(ptr + offset + COMP_SIZE_OFFSET);
    int pos = buffer.position();
    //TODO: add to corrupted reads
    if (uncompressedSize <= 0 || compressedSize < 0) {
      return IOEngine.READ_ERROR;
    }
    long segSize = s.getSegmentDataSize();
    if (uncompressedSize > segSize || compressedSize > segSize) {
      return IOEngine.READ_ERROR;
    }
    if (dictVersion >= 0) {
      // TODO: sanity check on values
      if (segSize < offset + COMP_META_SIZE + compressedSize) {
        return IOEngine.READ_ERROR;
      }
      checkBuffer(uncompressedSize);
      // Decompress
      byte[] buf = buffers.get();
      int dsize = codec.decompress(ptr + offset + COMP_META_SIZE, compressedSize, buf, dictVersion);
      if (dsize == 0) {
        return IOEngine.NOT_FOUND;
      }
      // Find key-value in the buffer
      int offAdj = -META_SIZE;
      int off = (int) BlockReaderWriterSupport.findInBlock(buf, offAdj, dsize, keyPtr, keySize);
      if (off < 0) {
        return IOEngine.NOT_FOUND;
      }
      int valueSize = Utils.getValueSize(buf, off);
      if (valueSize < rangeStart) {
        // TODO: better handling
        return IOEngine.NOT_FOUND;
      }
      if (valueSize < rangeStart + rangeSize) {
        rangeSize = valueSize - rangeStart;
      }
      int valueOffset = Utils.getValueOffset(buf, off);
      valueOffset += rangeStart;
      UnsafeAccess.copy(buf, off + valueOffset, buffer, pos, rangeSize);
    } else if (dictVersion == -1){
      // TODO: sanity check on values
      if (segSize < offset + COMP_META_SIZE + uncompressedSize) {
        return IOEngine.READ_ERROR;
      }
      // Find key-value in the buffer
      int offAdj = COMP_META_SIZE - META_SIZE;
      long addr = BlockReaderWriterSupport.findInBlock(ptr + offset + offAdj, uncompressedSize, keyPtr, keySize);
      if (addr < 0) {
        return IOEngine.NOT_FOUND;
      }      
      int valueSize = Utils.getValueSize(addr);

      if (valueSize < rangeStart) {
        // TODO: better handling
        return IOEngine.NOT_FOUND;
      }
      if (valueSize < rangeStart + rangeSize) {
        rangeSize = valueSize - rangeStart;
      }
      int valueOffset = Utils.getValueOffset(addr);
      valueOffset += rangeStart;
      if (segSize < offset + valueOffset + rangeSize) {
        // Rare situation - wrong segment - hash collision
        return IOEngine.READ_ERROR;
      }
      UnsafeAccess.copy(addr + valueOffset, buffer,  rangeSize);
    } else {
      return IOEngine.READ_ERROR;
    }
    buffer.position(pos);
    return rangeSize;
  }

  @Override
  public SegmentScanner getSegmentScanner(IOEngine engine, Segment s) throws IOException {
    return new CompressedBlockMemorySegmentScanner(s, this.codec);
  }
  
}
