/*
 * Copyright (C) 2024-present Carrot Data, Inc. 
 * <p>This program is free software: you can redistribute it
 * and/or modify it under the terms of the Server Side Public License, version 1, as published by
 * MongoDB, Inc.
 * <p>This program is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
 * PURPOSE. See the Server Side Public License for more details. 
 * <p>You should have received a copy of the Server Side Public License along with this program. If not, see
 * <http://www.mongodb.com/licensing/server-side-public-license>.
 */
package com.carrotdata.cache.io;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.ConcurrentHashMap;

import com.carrotdata.cache.util.Persistent;
import com.carrotdata.cache.util.Utils;

public class WriteBatches implements Persistent {
  /**
   * One instance per IOEngine Keeps one write batch per thread per segment rank Integer id is a the
   * key, which composed from thread id (lower 16 bits) and from segment's rank (also 16 bits).
   * There is a limited amount of ranks default is 8, can be more but definitely not 64K
   */
  private ConcurrentHashMap<Integer, WriteBatch> wbMap =
      new ConcurrentHashMap<Integer, WriteBatch>();

  private DataWriter writer;

  WriteBatches(DataWriter writer) {
    this.writer = writer;
  }

  /**
   * This method is thread-safe, because write batch can not be shared for write operations
   * between threads by design
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
    for (WriteBatch wb : wbMap.values()) {
      wb.dispose();
    }
    wbMap.clear();
  }

  /**
   * Number of write batch objects
   * @return size
   */
  public int size() {
    return wbMap.size();
  }

  @Override
  public void save(OutputStream os) throws IOException {
    DataOutputStream dos = Utils.toDataOutputStream(os);
    int size = wbMap.size();
    dos.writeInt(size);
    for (Integer key : wbMap.keySet()) {
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
