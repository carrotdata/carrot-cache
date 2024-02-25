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
package com.onecache.core.io;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;

import com.onecache.core.io.IOUtils;
import com.onecache.core.io.Segment;
import com.onecache.core.util.Utils;
//FIXME: handling last sK-V in a file which is less than 6 bytes total
public class PrefetchBuffer {
  /*
   * File to prefetch - all operations on file must be 
   * synchronized
   */
  protected RandomAccessFile file;
  /*
   * Current offset in a file
   */
  protected long fileOffset = Segment.META_SIZE;
  /**
   * File length
   */
  protected long fileLength = 0;
  /*
   * Prefetch buffer data
   */
  protected byte[] buffer;
  /*
   * Buffer size
   */
  protected int bufferSize = 0;
  /**
   * Offset in a prefetch buffer
   */
  protected int bufferOffset = 0;
  /**
   * Buffer data size - the size of data currently in the prefetch buffer
   */
  protected int bufferDataSize = 0;
  
  protected int keyLength = -1;
  
  protected int valueLength = -1;
  
  /**
   * Constructor
   * @param file file 
   * @param bufferSize buffer size
   * @throws IOException
   */
  public PrefetchBuffer(RandomAccessFile file, int bufferSize) throws IOException {
    this.file = file;
    this.bufferSize = bufferSize;
    this.buffer = new byte[bufferSize];
    this.fileLength = this.file.length();
    this.bufferDataSize = (int) Math.min(bufferSize, file.length());
    // we need this for prefetch
    this.bufferOffset = this.bufferDataSize;
    prefetch();
  }
  /**
   * Skip bytes
   * @param nBytes
   * @return true or false (can't skip)
   * @throws IOException 
   */
  public boolean skip(int nBytes) throws IOException {
    if (nBytes < 0 && this.bufferOffset + nBytes < 0) {
      // we can not reverse back into previous buffer
      return false;
    }
    if (this.fileOffset + nBytes > this.fileLength) {
      return false;
    }
    if (nBytes > 0 && this.bufferOffset + nBytes >= this.bufferDataSize) {
      prefetch();
    } 
    this.bufferOffset += nBytes;
    this.fileOffset += nBytes;
    return true;
  }
  
  /** advance to the next K-V*/
  public boolean next() throws IOException {
    int kLength = keyLength();
    int vLength = valueLength();
    int n = Utils.kvSize(kLength, vLength);
    boolean result = advance(n);
    
    // reset key-value sizes
    this.keyLength = -1;
    this.valueLength = -1;
    return result;
  }
  /**
   * Advance - skip 
   * @param nBytes
   * @return true or false
   */
  public boolean advance(int nBytes) throws IOException{
    return skip(nBytes);
  }
  /**
   * Ensure remaining capacity
   * @param nBytes
   * @return true or false
   * @throws IOException
   */
  public boolean ensure(int nBytes) throws IOException {
    if (nBytes + this.fileOffset > this.fileLength) {
      return false;
    }
    if (nBytes + this.bufferOffset > this.bufferDataSize) {
      prefetch();
      if (nBytes + this.bufferOffset > this.bufferDataSize) {
        return false;
      }
    } 
    return true;
  }
  
  void prefetch() throws IOException {
    int toRead = (int) Math.min(this.bufferOffset, 
      this.fileLength - this.fileOffset - (bufferDataSize - bufferOffset)); 
    System.arraycopy(buffer, bufferOffset, buffer, 0, bufferDataSize - bufferOffset);
    
    IOUtils.readFully(file, fileOffset + (bufferDataSize - bufferOffset), 
      buffer, bufferDataSize - bufferOffset, toRead);
    this.bufferDataSize = this.bufferSize - this.bufferOffset + toRead;
    this.bufferOffset = 0;
  }
  
  /**
   * Get file offset
   * @return file offset
   */
  public long getFileOffset() {
    return this.fileOffset;
  }
  
  /**
   * Key length
   * @return key length
   * @throws IOException
   */
  public int keyLength() throws IOException {
    if (this.keyLength > 0) return keyLength;
    //FIXME: This can break if at the end of the file is very small K-V
    // Key length is maximum 4 bytes
    ensure(4);
    this.keyLength = Utils.readUVInt(buffer, bufferOffset);
    return this.keyLength;
  }
  
  /**
   * value length
   * @return
   * @throws IOException
   */
  public int valueLength() throws IOException {
    if (this.valueLength > 0) return this.valueLength;
    int kSize = keyLength();
    int kSizeSize = Utils.sizeUVInt(kSize);
    
    //FIXME: This can break if at the end of the file is very small K-V
    // We all start at K-V offset
    ensure(4 + kSizeSize);
    this.valueLength = Utils.readUVInt(buffer, bufferOffset + kSizeSize);
    return this.valueLength;
  }
  
  /**
   * Get key from this prefetch buffer to another byte array
   * @param buf byte array
   * @param bufOffset offset
   * @return key size
   * @throws IOException 
   */
  public int getKey(byte[] buf, int bufOffset) throws IOException {
    int kSize = keyLength();
    int vSize = valueLength();
    int kSizeSize = Utils.sizeUVInt(kSize);
    int vSizeSize = Utils.sizeUVInt(vSize);
    int n = Utils.kvSize(kSize, vSize);
    boolean result = ensure(n);
    if (!result) {
      return -1;
    }
    if (buf.length - bufOffset >= kSize) {
      System.arraycopy(this.buffer, this.bufferOffset + kSizeSize + vSizeSize, buf, bufOffset, kSize);
    }
    return kSize;
  }
  
  /**
   * Get key from this prefetch buffer to another byte buffer
   * @param buf byte buffer
   * @return number of bytes copied or -1
   * @throws IOException 
   */
  public int getKey(ByteBuffer buf) throws IOException {
    int kSize = keyLength();
    int vSize = valueLength();
    int kSizeSize = Utils.sizeUVInt(kSize);
    int vSizeSize = Utils.sizeUVInt(vSize);
    int n = Utils.kvSize(kSize, vSize);
    boolean result = ensure(n);
    if (!result) {
      return -1;
    }
    if (buf.remaining() >= kSize) {
      buf.put(this.buffer, this.bufferOffset + kSizeSize + vSizeSize, kSize);
    }
    //TODO restore old position?
    return vSize;
  }
  
  /**
   * Get value from this prefetch buffer to another byte array
   * @param buf byte array
   * @param bufOffset offset
   * @return bytes copied or -1
   * @throws IOException 
   */
  public int getValue(byte[] buf, int bufOffset) throws IOException {
    int kSize = keyLength();
    int vSize = valueLength();
    int kSizeSize = Utils.sizeUVInt(kSize);
    int vSizeSize = Utils.sizeUVInt(vSize);
    int n = Utils.kvSize(kSize, vSize);
    boolean result = ensure(n);
    if (!result) {
      return -1;
    }
    if (buf.length - bufOffset >= vSize) {
      System.arraycopy(this.buffer, this.bufferOffset + kSizeSize + vSizeSize + kSize, buf, bufOffset, vSize);
    } 
    return vSize;

  }
  
  /**
   * Get value from this prefetch buffer to another byte buffer
   * @param buf byte buffer
   * @return number of bytes copied or -1
   * @throws IOException 
   */
  public int getValue(ByteBuffer buf) throws IOException {
    int kSize = keyLength();
    int vSize = valueLength();
    int kSizeSize = Utils.sizeUVInt(kSize);
    int vSizeSize = Utils.sizeUVInt(vSize);
    int n = Utils.kvSize(kSize, vSize);
    boolean result = ensure(n);
    if (!result) {
      return -1;
    }
    if (buf.remaining() >= kSize) { 
      buf.put(this.buffer, this.bufferOffset + kSizeSize + vSizeSize + kSize, vSize);
    }
    //TODO restore old position?
    return vSize;
  }
  
  /**
   * Get byte buffer
   * @return byte buffer
   */
  public byte[] getBuffer() {
    return this.buffer;
  }
  
  /**
   * Get buffer offset
   * @return buffer offset
   */
  public int getBufferOffset() {
    return this.bufferOffset;
  }
  
  /**
   * Get current offset in the file
   * @return offset
   */
  public long getOffset() {
    return this.fileOffset;// + this.bufferOffset;
  }
  
  /**
   * Return number of available bytes in the prefetch buffer
   * @return available bytes
   */
  public int available() {
    return this.bufferDataSize - this.bufferOffset;
  }
}
