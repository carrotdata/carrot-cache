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

public class OffheapIOEngine extends IOEngine {
  
  /** Logger */
  @SuppressWarnings("unused")
  private static final Logger LOG = LogManager.getLogger(OffheapIOEngine.class);
    
  public OffheapIOEngine(String cacheName) {
    super(cacheName);
  }

  @Override
  protected int getInternal(int sid, long offset, int size, byte[] key, 
      int keyOffset, int keySize, byte[] buffer, int bufOffset)  {
    try {
      return this.memoryDataReader.read(this, key, keyOffset, keySize, sid, offset, size, buffer, bufOffset);
    } catch (IOException e) {
      // never happens
    }
    return NOT_FOUND;
  }

  @Override
  protected int getInternal(int sid, long offset, int size, byte[] key, 
      int keyOffset, int keySize, ByteBuffer buffer) throws IOException {
    try {
      return this.memoryDataReader.read(this, key, keyOffset, keySize, sid, offset, size, buffer);
    } catch (IOException e) {
      // never happens
    }
    return NOT_FOUND;
  }

  @Override
  protected int getInternal(int sid, long offset, int size, long keyPtr, 
      int keySize, byte[] buffer, int bufOffset)  {
    try {
      return this.memoryDataReader.read(this, keyPtr, keySize, sid, offset, size, buffer, bufOffset);
    } catch (IOException e) {
      // never happens
    }
    return NOT_FOUND;
 }

  @Override
  protected int getInternal(int sid, long offset, int size, long keyPtr, 
      int keySize, ByteBuffer buffer) throws IOException {
    try {
      return this.memoryDataReader.read(this, keyPtr, keySize, sid, offset, size, buffer);
    } catch (IOException e) {
      // never happens
    }
    return NOT_FOUND;
  }
  
  @Override
  public SegmentScanner getScanner(Segment s) throws IOException {
    return this.memoryDataReader.getSegmentScanner(null, s);
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
      Path filePath = Paths.get(dataDir, getSegmentFileName(s.getId()));
//      if (Files.exists(filePath)) {
//        //FIXME: old segment? MUST be deleted when segment ID is reassigned
//        continue; // we skip it
//      }
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
    Stream<Path> ls = Files.list(path);
    Iterator<Path> it = ls.iterator();
    while(it.hasNext()) {
      Path p = it.next();
      int id = getSegmentIdFromPath(p);
      // FIXME: what is this is null?
      Segment s = this.dataSegments[id];
      FileInputStream fis = new FileInputStream(p.toFile());
      DataInputStream dis = new DataInputStream(fis);
      s.load(dis);
      s.setOffheap(true);
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
