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

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.carrot.cache.Cache;
import com.carrot.cache.util.UnsafeAccess;
import com.carrot.cache.util.Utils;

public class FileIOEngine extends IOEngine {
    
  static class Scanner implements SegmentScanner {

    RandomAccessFile file;
    long offset = 0;
    int numEntries;
    int currentEntry = 0;
    FileIOEngine engine;
    
    int keyLength;
    int valueLength;
    long expire;
    
    ByteBuffer buf = ByteBuffer.allocateDirect(10);
    long bufAddress = UnsafeAccess.address(buf);
    
    Scanner(Segment s, FileIOEngine engine) throws IOException{
      this.file = engine.getFileFor(s.getId());
      this.offset = 0;
      this.numEntries = s.getInfo().getTotalItems();
    }
    
    @Override
    public boolean hasNext() throws IOException {
      // TODO Auto-generated method stub
      if (currentEntry <= numEntries - 1) {
        // Read key size, value size, expire
        this.expire = file.readLong();
        int toRead = (int) Math.min(10L, (file.length() - offset));
        buf.clear();
        int read = 0;
        while(read < toRead) {
          read += this.file.getChannel().read(buf);
        }
        buf.flip();
        this.keyLength = Utils.readUVInt(bufAddress);
        int kSizeSize = Utils.sizeUVInt(this.keyLength);
        this.valueLength = Utils.readUVInt(bufAddress + kSizeSize);
        long pos = file.getFilePointer();
        // Rewind position back
        file.seek(pos - Utils.SIZEOF_LONG - toRead);
        return true;
      };
      return false;
    }

    @Override
    public void next() throws IOException {
      this.currentEntry ++;
      // advance offset
      int kSizeSize = Utils.sizeUVInt(this.keyLength);
      int vSizeSize = Utils.sizeUVInt(this.valueLength);
      long pos = file.getFilePointer();
      file.seek(pos + Utils.SIZEOF_LONG /* timestamp */ + this.keyLength + kSizeSize + this.valueLength + vSizeSize);
    }

    @Override
    public int keyLength() {
      return this.keyLength;
    }

    @Override
    public int valueLength() {
      // Caller must check return value
      return this.valueLength;
    }

    @Override
    public long keyAddress() {
      // Caller must check return value
      return 0;
    }

    @Override
    public long valueAddress() {
      return 0;
    }

    @Override
    public long getExpire() {
      return this.expire;
    }

    @Override
    public void close() throws IOException {
      file.close();
    }

    @Override
    public int getKey(ByteBuffer b) throws IOException {
      if (b.remaining() < this.keyLength) {
        return this.keyLength;
      }
      int off = Utils.SIZEOF_LONG + Utils.sizeUVInt(this.keyLength) + 
          Utils.sizeUVInt(this.valueLength);
      FileChannel fc = file.getChannel();
      long pos = fc.position();
      fc.position(pos + off);
      int read = 0;
      while (read < this.keyLength) {
        read += fc.read(buf);
      }
      fc.position(pos);
      return this.keyLength;
    }

    @Override
    public int getValue(ByteBuffer b) throws IOException {
      if (b.remaining() < this.valueLength) {
        return this.valueLength;
      }
      int off = Utils.SIZEOF_LONG + Utils.sizeUVInt(this.keyLength) + 
          Utils.sizeUVInt(this.valueLength) + this.keyLength;
      FileChannel fc = file.getChannel();
      long pos = fc.position();
      fc.position(pos + off);
      int read = 0;
      while (read < this.valueLength) {
        read += fc.read(buf);
      }
      fc.position(pos);
      return this.valueLength;
    }
    
    @Override
    public boolean isDirect() {
      return false;
    }

    @Override
    public int getKey(byte[] buffer) throws IOException {
      if (buffer.length < this.keyLength) {
        return this.keyLength;
      }
      int off = Utils.SIZEOF_LONG + Utils.sizeUVInt(this.keyLength) + 
          Utils.sizeUVInt(this.valueLength);
      long pos = file.getFilePointer();
      file.seek(pos + off);
      int read =0;
      while (read < this.keyLength) {
        read += file.read(buffer, read, this.keyLength - read);
      }
      file.seek(pos);
      return this.keyLength;
    }

    @Override
    public int getValue(byte[] buffer) throws IOException {
      if (buffer.length < this.valueLength) {
        return this.valueLength;
      }
      int off = Utils.SIZEOF_LONG + Utils.sizeUVInt(this.keyLength) + 
          Utils.sizeUVInt(this.valueLength) + this.keyLength;
      long pos = file.getFilePointer();
      file.seek(pos + off);
      int read =0;
      while (read < this.valueLength) {
        read += file.read(buffer, read, this.valueLength - read);
      }
      file.seek(pos);
      return this.keyLength;
    }
  }
    
  /** Logger */
  private static final Logger LOG = LogManager.getLogger(FileIOEngine.class);
  /**
   * Maps data segment Id to a disk file
   */
  Map<Integer, RandomAccessFile> dataFiles = new HashMap<Integer, RandomAccessFile>();
  
  
  /**
   * Constructor
   * @param numSegments
   * @param segmentSize
   */
  public FileIOEngine(Cache parent) {
    super(parent);
  }
 
  /**
   * IOEngine subclass can override this method
   *
   * @param data data segment
   * @throws FileNotFoundException 
   */
  protected void saveInternal(Segment data) throws IOException {
    Runnable r = () -> {
      data.seal();
      int id = data.getId();
      try {
        OutputStream os = getOSFor(id);
        // Save to file
        data.save(os);
        // Close stream
        os.close();
        // Remove from RAM buffers
        removeFromRAMBuffers(data);
      } catch (IOException e) {
        LOG.error(e);
      }
    };
    new Thread(r).start();
  }
  
  @Override
  protected boolean getInternal(int id, long offset, int size, byte[] buffer, int bufOffset) throws IOException {
    RandomAccessFile file = dataFiles.get(id);
    if (file == null) {
      return false;
    }
    
    if (file.length() < offset + size) {
      throw new IllegalArgumentException();
    }
    //TODO: can file be NULL?
    synchronized (file) {
      file.seek(offset);
      file.readFully(buffer, bufOffset, size);
    }
    return true;
  }
  
  OutputStream getOSFor(int id) throws FileNotFoundException {
    Path p = getPathForDataSegment(id);
    FileOutputStream fos = new FileOutputStream(p.toFile());
    return fos;
  }
  
  RandomAccessFile getFileFor(int id) throws FileNotFoundException {
    RandomAccessFile file = dataFiles.get(id);
    if (file == null) {
      // open
      Path p = getPathForDataSegment(id);
      file = new RandomAccessFile(p.toFile(), "rw");
      dataFiles.put(id, file);
    }
    return file;
  }
  
  @Override
  public synchronized void releaseSegmentId(Segment data) {
    super.releaseSegmentId(data);
    // close and delete file
    RandomAccessFile f = dataFiles.get(data.getId());
    if (f != null) {
      try {
        f.close();
        Files.deleteIfExists(getPathForDataSegment(data.getId()));
      } catch(IOException e) {
        //TODO
      }
    }
  }
  /**
   * Get file path for a data segment
   * @param id data segment id
   * @return path to a file
   */
  private Path getPathForDataSegment(int id) {
    return Paths.get(dataDir, getSegmentFileName(id));
  }


  @Override
  protected boolean getInternal(int id, long offset, int size, ByteBuffer buffer) throws IOException {
    RandomAccessFile file = dataFiles.get(id);
    if (file == null) {
      return false;
    }
    
    if (file.length() < offset + size) {
      throw new IllegalArgumentException();
    }
    synchronized (file) {
      FileChannel fc = file.getChannel();
      fc.position(offset);
      int read = 0;
      while(read < size) {
        read += fc.read(buffer);
      }
    }
    return true;
  }

  @Override
  public SegmentScanner getScanner(Segment s) throws IOException {
    return new Scanner (s, this);
  }

  @Override
  public void save(OutputStream os) throws IOException {
    super.save(os);
  }

  @Override
  public void load(InputStream is) throws IOException {
    super.load(is);
  }
  
}
