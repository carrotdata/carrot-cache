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
package com.carrot.cache.index;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

import com.carrot.cache.util.UnsafeAccess;
import com.carrot.cache.util.Utils;

public class TestMemoryIndexMQ extends TestMemoryIndexBase{
  /** Logger */
  @SuppressWarnings("unused")
  private static final Logger LOG = LogManager.getLogger(TestMemoryIndexMQ.class);
  
  @Before
  public void setUp() {
    memoryIndex = new MemoryIndex("default", MemoryIndex.Type.MQ);
  }
  
  private void loadIndexBytes() {
    IndexFormat format = memoryIndex.getIndexFormat();
    int entrySize = format.indexEntrySize();
    long buf = UnsafeAccess.mallocZeroed(entrySize);
    
    for(int i = 0; i < numRecords; i++) {
      format.writeIndex(buf, keys[i], 0, keys[i].length, values[i], 0, values[i].length, 
        sids[i], offsets[i], lengths[i], 0);
      memoryIndex.insert(keys[i], 0, keySize, buf, entrySize);
    }
    UnsafeAccess.free(buf);
  }
  
  private void loadIndexBytesWithEviction(int evictionStartFrom) {
    IndexFormat format = memoryIndex.getIndexFormat();
    int entrySize = format.indexEntrySize();
    long buf = UnsafeAccess.mallocZeroed(entrySize);
    
    for(int i = 0; i < numRecords; i++) {
      format.writeIndex(buf, keys[i], 0, keys[i].length, values[i], 0, values[i].length, 
        sids[i], offsets[i], lengths[i], 0);
      memoryIndex.insert(keys[i], 0, keySize, buf, entrySize);
      if (i == evictionStartFrom - 1) {
        memoryIndex.setEvictionEnabled(true);
      }
    }
    UnsafeAccess.free(buf);
  }
  
  private void verifyIndexBytes() {
    verifyIndexBytes(false);
  }

  private void verifyIndexBytes(boolean hit) {
    
    IndexFormat format = memoryIndex.getIndexFormat();
    int entrySize = format.indexEntrySize();
    long buf = UnsafeAccess.mallocZeroed(entrySize);

    for (int i = 0; i < numRecords; i++) {
      long hash = Utils.hash64(keys[i], 0, keySize);
      int result = (int) memoryIndex.find(keys[i], 0, keySize, hit, buf, entrySize);
      assertEquals(entrySize, result);
      assertEquals(hash, format.getHash(buf));
      short sid = (short)format.getSegmentId(buf);
      int offset = (int) format.getOffset(buf);
      int size = (int) format.getKeyValueSize(buf);
      assertEquals(sids[i], sid);
      assertEquals(offsets[i], offset);
      assertEquals(lengths[i], size);
    }
    UnsafeAccess.free(buf);
  }
  
  private void loadIndexMemory() {
    IndexFormat format = memoryIndex.getIndexFormat();
    int entrySize = format.indexEntrySize();
    long buf = UnsafeAccess.mallocZeroed(entrySize);
    
    for(int i = 0; i < numRecords; i++) {
      format.writeIndex(buf, mKeys[i], keySize, mValues[i], valueSize, 
        sids[i], offsets[i], lengths[i], 0);
      memoryIndex.insert(mKeys[i], keySize, buf, entrySize);
    }
    UnsafeAccess.free(buf);
  }
  
  private void loadIndexMemoryWithEviction(int evictionStartFrom) {
    IndexFormat format = memoryIndex.getIndexFormat();
    int entrySize = format.indexEntrySize();
    long buf = UnsafeAccess.mallocZeroed(entrySize);
    
    for(int i = 0; i < numRecords; i++) {
      format.writeIndex(buf, mKeys[i], keySize, mValues[i], valueSize, 
        sids[i], offsets[i], lengths[i], 0);
      memoryIndex.insert(mKeys[i], keySize, buf, entrySize);
      if (i == evictionStartFrom - 1) {
        memoryIndex.setEvictionEnabled(true);
      }
    }
    UnsafeAccess.free(buf);
  }
  
  private void verifyIndexMemory() {
    verifyIndexMemory(false);
  }
  
  private void verifyIndexMemory(boolean hit) {
    IndexFormat format = memoryIndex.getIndexFormat();
    int entrySize = format.indexEntrySize();
    long buf = UnsafeAccess.mallocZeroed(entrySize);

    for (int i = 0; i < numRecords; i++) {
      long hash = Utils.hash64(mKeys[i], keySize);
      int result = (int) memoryIndex.find(mKeys[i], keySize, hit, buf, entrySize);
      assertEquals(entrySize, result);
      assertEquals(hash, format.getHash(buf));
      short sid = (short)format.getSegmentId(buf);
      int offset = (int) format.getOffset(buf);
      int size = (int) format.getKeyValueSize(buf);
      assertEquals(sids[i], sid);
      assertEquals(offsets[i], offset);
      assertEquals(lengths[i], size);
    }
    UnsafeAccess.free(buf);
  }
  
  @Test
  public void testLoadReadNoRehashBytes() {
    System.out.println("Test load and read no rehash bytes");
    loadReadBytes(100000);
  }
  
  @Test
  public void testLoadReadNoRehashMemory() {
    System.out.println("Test load and read no rehash memory");
    loadReadMemory(100000);
  }
  
  @Test
  public void testLoadReadNoRehashBytesWithHit() {
    System.out.println("Test load and read no rehash bytes - with hit");
    loadReadBytesWithHit(100000);
  }
  
  @Test
  public void testLoadReadNoRehashMemoryWithHit() {
    System.out.println("Test load and read no rehash memory - with hit");
    loadReadMemoryWithHit(100000);
  }
  
  
  @Test
  public void testLoadReadDeleteNoRehashBytes() {
    System.out.println("Test load and read-delete no rehash bytes");
    loadReadBytes(100000);
    deleteIndexBytes();
    verifyIndexBytesNot();
  }
  
  @Test
  public void testLoadReadDeleteNoRehashMemory() {
    System.out.println("Test load and read-delete no rehash memory");
    loadReadMemory(100000);
    deleteIndexMemory();
    verifyIndexMemoryNot();
  }
  
  @Test
  public void testLoadReadWithRehashBytes() {
    System.out.println("Test load and read with rehash bytes");
    loadReadBytes(1000000);
  }
  
  @Test
  public void testLoadReadWithRehashMemory() {
    System.out.println("Test load and read with rehash memory");
    loadReadMemory(1000000);
  }
  
  @Test
  public void testLoadReadWithRehashBytesWithHit() {
    System.out.println("Test load and read with rehash bytes - with hit");
    loadReadBytesWithHit(1000000);
  }
  
  @Test
  public void testLoadReadWithRehashMemoryWithHit() {
    System.out.println("Test load and read with rehash memory - with hit");
    loadReadMemoryWithHit(1000000);
  }
  
  @Test
  public void testLoadReadDeleteWithRehashBytes() {
    System.out.println("Test load and read-delete with rehash bytes");
    loadReadBytes(1000000);
    deleteIndexBytes();
    verifyIndexBytesNot();
  }
  
  @Test
  public void testLoadReadDeleteWithRehashMemory() {
    System.out.println("Test load and read with rehash memory");
    loadReadMemory(1000000);
    deleteIndexMemory();
    verifyIndexMemoryNot();
  }
  
  @Test
  public void testEvictionBytes() {
    System.out.println("Test eviction bytes");

    prepareData(200000);
    loadIndexBytesWithEviction(100000);
    
    long size = memoryIndex.size();
    assertEquals(100000L, size);
    
    int entrySize = memoryIndex.getIndexFormat().indexEntrySize();
    long buf = UnsafeAccess.mallocZeroed(entrySize);
    
    int evicted1 = 0;
    int evicted2 = 0;
    for (int i = 0; i < 100000; i++) {
      int result = (int) memoryIndex.find(keys[i], 0, keySize, false, buf, entrySize);
      if (result == -1) evicted1++;
    }
    
    for (int i = 100000; i < 200000; i++) {
      int result = (int) memoryIndex.find(keys[i], 0, keySize, false, buf, entrySize);
      if (result == -1) evicted2 ++;
    }
    System.out.println("evicted1=" + evicted1 + " evicted2="+ evicted2);
    assertEquals(100000, evicted1 + evicted2);
    UnsafeAccess.free(buf); 
  }
  
  @Test
  public void testEvictionMemory() {
    System.out.println("Test eviction memory");

    prepareData(200000);
    loadIndexMemoryWithEviction(100000);
    
    long size = memoryIndex.size();
    assertEquals(100000L, size);
    
    int entrySize = memoryIndex.getIndexFormat().indexEntrySize();
    long buf = UnsafeAccess.mallocZeroed(entrySize);
    
    int evicted1 = 0;
    int evicted2 = 0;
    for (int i = 0; i < 100000; i++) {
      int result = (int) memoryIndex.find(mKeys[i], keySize, false, buf, entrySize);
      if (result == -1) evicted1++;
    }
    
    for (int i = 100000; i < 200000; i++) {
      int result = (int) memoryIndex.find(mKeys[i], keySize, false, buf, entrySize);
      if (result == -1) evicted2 ++;
    }
    System.out.println("evicted1=" + evicted1 + " evicted2="+ evicted2);
    assertEquals(100000, evicted1 + evicted2);
    UnsafeAccess.free(buf);
  }
  
  @Test
  public void testLoadSave() throws IOException {
    System.out.println("Test load save");
    prepareData(200000);
    loadIndexMemory();
    long size = memoryIndex.size();
    assertEquals(200000L, size);
    
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    
    memoryIndex.save(dos);
    
    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    DataInputStream dis = new DataInputStream(bais);
    
    memoryIndex.dispose();
    
    memoryIndex = new MemoryIndex();
    memoryIndex.load(dis);
    
    verifyIndexMemory();

  }
  
  private void loadReadBytes(int num) {
    prepareData(num);
    System.out.println("prepare done");
    loadIndexBytes();
    System.out.println("load done");
    long size = memoryIndex.size();
    assertEquals((long)num, size);
    verifyIndexBytes();
  }
  
  
  private void loadReadMemory(int num) {
    prepareData(num);
    loadIndexMemory();
    long size = memoryIndex.size();
    assertEquals((long)num, size);
    verifyIndexMemory();
  }
  
  private void loadReadBytesWithHit(int num) {
    loadReadBytes(num);
    verifyIndexBytes(true);
    verifyIndexBytes();
    long size = memoryIndex.size();
    assertEquals(num, (int) size);
  }
  
  
  private void loadReadMemoryWithHit(int num) {
    loadReadMemory(num);
    verifyIndexMemory(true);
    verifyIndexMemory();
    long size = memoryIndex.size();
    assertEquals(num, (int) size);
  }
}
