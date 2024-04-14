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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.ConcurrentHashMap;

import com.onecache.core.util.Persistent;
import com.onecache.core.util.Utils;

public class WriteBatches implements Persistent {
  /**
   * One instance per IOEngine
   * 
   * Keeps one write batch per thread per segment rank
   * Integer id is a the key, which composed from thread id (lower 16 bits)
   * and from segment's rank (also 16 bits). There is a limited amount of ranks
   * default is 8, can be more but definitely not 64K 
   */
  private ConcurrentHashMap<Integer, WriteBatch> wbMap = new ConcurrentHashMap<Integer, WriteBatch>();
  
  private DataWriter writer;
  
  WriteBatches(DataWriter writer){
    this.writer = writer;
  }
  /**
   * This method is thread -safe, because
   * write batch can not be shared between threads by design
   * @param id write batch integer id.
   * @return write batch
   */
  WriteBatch getWriteBatch(int id) {
    WriteBatch wb = wbMap.get(id);
    if (wb == null) {
      wb = this.writer.newWriteBatch();
      wb.setId(id);
      wbMap.put(id, wb);
    }
    return wb;
  }
  
  void dispose() {
    for(WriteBatch wb: wbMap.values()) {
      wb.dispose();
    }
    wbMap.clear();
  }
  
  @Override
  public void save(OutputStream os) throws IOException {
    DataOutputStream dos = Utils.toDataOutputStream(os);
    int size = wbMap.size();
    dos.writeInt(size);
    for(Integer key: wbMap.keySet()) {
      WriteBatch wb = wbMap.get(key);
      wb.save(dos);
    }
  }
  
  @Override
  public void load(InputStream is) throws IOException {
    DataInputStream dis = Utils.toDataInputStream(is);
    int size = dis.readInt();
    for (int i = 0; i < size; i++) {
      WriteBatch wb = new WriteBatch();
      wb.load(dis);
      wbMap.put(wb.getId(), wb);
    }
  }
  
  
}
