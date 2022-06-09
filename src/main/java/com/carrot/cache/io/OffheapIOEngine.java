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
package com.carrot.cache.io;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.carrot.cache.Cache;
import com.carrot.cache.util.UnsafeAccess;
import com.carrot.cache.util.Utils;

public class OffheapIOEngine extends IOEngine {
  
  /**
   * Segment scanner
   * 
   * Usage:
   * 
   * while(scanner.hasNext()){
   *   // do job
   *   // ...
   *   // next()
   *   scanner.next();
   * }
   */
  static class Scanner implements SegmentScanner {
    /*
     * Parent segment
     */
    Segment parent;
    /*
     * Current scanner index 
     */
    int currentIndex = 0;
    
    /**
     * Current offset in a parent segment
     */
    int offset = 0;
    
    /*
     * Private constructor
     */
    Scanner(Segment s){
      this.parent = s;
    }
    
    public boolean hasNext() {
      return currentIndex < parent.numberOfEntries();
    }
    
    public void next() {
      long ptr = parent.getAddress();
      // Skip expiration time
      offset += Utils.SIZEOF_LONG;
      
      int keySize = Utils.readUVInt(ptr + offset);
      int keySizeSize = Utils.sizeUVInt(keySize);
      offset += keySizeSize;
      int valueSize = Utils.readUVInt(ptr + offset);
      int valueSizeSize = Utils.sizeUVInt(valueSize);
      offset += valueSizeSize;
      offset += keySize + valueSize;
      currentIndex++;
    }
    
    /**
     * Get expiration time of a current cached entry
     * @return expiration time
     */
    public final long getExpire() {
      return UnsafeAccess.toLong(offset);
    }
    
    /**
     * Get key size of a current cached entry
     * @return key size
     */
    public final int keyLength() {
      long ptr = parent.getAddress();
      return Utils.readUVInt(ptr + offset + Utils.SIZEOF_LONG);
    }
    
    /**
     * Get current value size
     * @return value size
     */
    
    public final int valueLength() {
      long ptr = parent.getAddress();
      int off = offset + Utils.SIZEOF_LONG;
      int keySize = Utils.readUVInt(ptr + off);
      int keySizeSize = Utils.sizeUVInt(keySize);
      off += keySizeSize;
      return Utils.readUVInt(ptr + off);
    }
    
    /**
     * Get current key's address
     * @return keys address
     */
    public final long keyAddress() {
      long ptr = parent.getAddress();
      int off = offset + Utils.SIZEOF_LONG;
      int keySize = Utils.readUVInt(ptr + off);
      int keySizeSize = Utils.sizeUVInt(keySize);
      off += keySizeSize;
      int valueSize = Utils.readUVInt(ptr + off);
      int valueSizeSize = Utils.sizeUVInt(valueSize);
      off += valueSizeSize;
      return ptr + off;
    }
    
    /**
     * Get current value's address
     * @return values address
     */
    public final long valueAddress() {
      long ptr = parent.getAddress();
      int off = offset + Utils.SIZEOF_LONG;
      int keySize = Utils.readUVInt(ptr + off);
      int keySizeSize = Utils.sizeUVInt(keySize);
      off += keySizeSize;
      int valueSize = Utils.readUVInt(ptr + off);
      int valueSizeSize = Utils.sizeUVInt(valueSize);
      off += valueSizeSize + keySize;
      return ptr + off;
    }

    @Override
    public void close() throws IOException {
      // TODO Auto-generated method stub
      
    }

    @Override
    public int getKey(ByteBuffer b) {
      int keySize = keyLength();
      long keyAddress = keyAddress();
      int toRead = Math.min(keySize, b.remaining());
      UnsafeAccess.copy(keyAddress, b, toRead);
      return toRead;
    }

    @Override
    public int getValue(ByteBuffer b) {
      int valueSize = valueLength();
      long valueAddress = valueAddress();
      int toRead = Math.min(valueSize, b.remaining());
      UnsafeAccess.copy(valueAddress, b, toRead);
      return toRead;
    }

    @Override
    public int getKey(byte[] buffer) throws IOException {
      int keySize = keyLength();
      if (keySize > buffer.length) {
        return keySize;
      }
      long keyAddress = keyAddress();
      UnsafeAccess.copy(keyAddress, buffer, 0, keySize); 
      return keySize;
    }

    @Override
    public int getValue(byte[] buffer) throws IOException {
      int valueSize = valueLength();
      if (valueSize > buffer.length) {
        return valueSize;
      }
      long valueAddress = valueAddress();
      UnsafeAccess.copy(valueAddress, buffer, 0, valueSize);
      return valueSize;
    }
  }
  
  /** Logger */
  @SuppressWarnings("unused")
  private static final Logger LOG = LogManager.getLogger(OffheapIOEngine.class);
  
  public OffheapIOEngine(Cache parent) {
    super(parent);
  }


  @Override
  protected boolean getInternal(int id, long offset, int size, byte[] buffer, int bufOffset)  {
    //FIXME: segment read lock
    Segment s = this.dataSegments[id];
    if (s == null) {
      // TODO: error
      return false; 
    }
    long ptr = s.getAddress(); 
 
    if ( s.dataSize() < offset + size) {
      throw new IllegalArgumentException();
    }
    //TODO: offset ? HEADER_SIZE?
    //must be > 0
    UnsafeAccess.copy(ptr + offset /*+ Segment.HEADER_SIZE*/, buffer, bufOffset, size);
    return true;
  }


  @Override
  protected boolean getInternal(int id, long offset, int size, ByteBuffer buffer) throws IOException {
    //FIXME: segment read lock
    Segment s = this.dataSegments[id];
    if (s == null) {
      // TODO: error
      return false; 
    }
    long ptr = s.getAddress(); 
 
    if ( s.dataSize() < offset + size) {
      throw new IllegalArgumentException();
    }
    //TODO: offset ? HEADER_SIZE?
    //must be > 0
    UnsafeAccess.copy(ptr + offset /*+ Segment.HEADER_SIZE*/, buffer,  size);
    return true;
  }


  @Override
  public SegmentScanner getScanner(Segment s) throws IOException {
    return new Scanner(s);
  }


  @Override
  public void save(OutputStream os) throws IOException {
    super.save(os);
    String dataDir = this.config.getDataDir(cacheName);
    //TODO: Saving RAM buffers? 
    // Now save all active segments
    for(Segment s: this.dataSegments) {
      if(s == null) {
        continue;
      }
      Path filePath = Paths.get(getSegmentFileName(s.getId()));
      if (Files.exists(filePath)) {
        continue; // we skip it
      }
      FileOutputStream fos = new FileOutputStream(filePath.toFile());
      DataOutputStream dos = new DataOutputStream(fos);
      s.save(os);
      
      dos.close();
    }
  }

  @Override
  public void load(InputStream is) throws IOException {
    super.load(is);
    // now load segments
    String dataDir = this.config.getDataDir(cacheName);
    Path path = Paths.get(dataDir);
    Stream<Path> ls =Files.list(path);
    Iterator<Path> it = ls.iterator();
    while(it.hasNext()) {
      Path p = it.next();
      int id = getSegmentIdFromPath(p);
      Segment s = this.dataSegments[id];
      FileInputStream fis = new FileInputStream(p.toFile());
      DataInputStream dis = new DataInputStream(fis);
      s.load(dis);
      dis.close();
    }
    ls.close();
  }
  
  private int getSegmentIdFromPath(Path p) {
    String s = p.toString();
    int indx = s.lastIndexOf("-");
    // There are 0's in heading!!!
    s = s.substring(indx + 1);
    return Integer.parseInt(s);
  }
}
