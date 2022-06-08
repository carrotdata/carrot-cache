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

import com.carrot.cache.index.MemoryIndex.MutationResult;
import com.carrot.cache.util.UnsafeAccess;
import com.carrot.cache.util.Utils;

public class TestMemoryIndexAQ extends TestMemoryIndexBase{
  /** Logger */
  private static final Logger LOG = LogManager.getLogger(TestMemoryIndexAQ.class);

  
  @Before
  public void setUp() {
    memoryIndex = new MemoryIndex("default", MemoryIndex.Type.AQ);
    memoryIndex.setMaximumSize(10000000);
  }
  
  
  
  private void loadIndexBytes() {
    for(int i = 0; i < numRecords; i++) {
      MutationResult result = memoryIndex.aarp(keys[i], 0, keySize);
      assertEquals(MutationResult.INSERTED, result);
    }
  }
  
  
  private void verifyIndexBytes() {
    int entrySize = memoryIndex.getIndexFormat().indexEntrySize();
    long buf = UnsafeAccess.mallocZeroed(entrySize);
    
    for (int i = 0; i < numRecords; i++) {
      long hash = Utils.hash64(keys[i], 0, keySize);
      int result = (int) memoryIndex.find(keys[i], 0, keySize, false, buf, entrySize);
      assertEquals(entrySize, result);
      assertEquals(hash, UnsafeAccess.toLong(buf));
    }
    UnsafeAccess.free(buf);
  }
  
  private void loadIndexBytesNot() {
    for(int i = 0; i < numRecords; i++) {
      MutationResult result = memoryIndex.aarp(keys[i], 0, keySize);
      assertEquals(MutationResult.DELETED, result);
    }
  }
    
  private void loadIndexMemory() {
    for(int i = 0; i < numRecords; i++) {
      MutationResult result = memoryIndex.aarp(mKeys[i], keySize);
      assertEquals(MutationResult.INSERTED, result);
    }
  }
    
  private void verifyIndexMemory() {
    int entrySize = memoryIndex.getIndexFormat().indexEntrySize();
    long buf = UnsafeAccess.mallocZeroed(entrySize);
    
    for (int i = 0; i < numRecords; i++) {
      long hash = Utils.hash64(mKeys[i], keySize);
      int result = (int) memoryIndex.find(mKeys[i], keySize, false, buf, entrySize);
      assertEquals(entrySize, result);
      assertEquals(hash, UnsafeAccess.toLong(buf));
    }
    UnsafeAccess.free(buf);
  }
  
  private void loadIndexMemoryNot() {
    for(int i = 0; i < numRecords; i++) {
      MutationResult result = memoryIndex.aarp(mKeys[i], keySize);
      assertEquals(MutationResult.DELETED, result);
    }
  }
    
  @Test
  public void testLoadReadNoRehashBytes() {
    LOG.info("Test load and read no rehash bytes");
    loadReadBytes(100000);
  }
  
  @Test
  public void testLoadReadNoRehashMemory() {
    LOG.info("Test load and read no rehash memory");
    loadReadMemory(100000);
  }
  
  @Test
  public void testLoadReadDeleteNoRehashBytes() {
    LOG.info("Test load and read-delete no rehash bytes");
    loadReadBytes(100000);
    deleteIndexBytes();
    verifyIndexBytesNot();
  }
  
  @Test
  public void testLoadReadDeleteNoRehashMemory() {
    LOG.info("Test load and read-delete no rehash memory");
    loadReadMemory(100000);
    deleteIndexMemory();
    verifyIndexMemoryNot();
  }
  
  @Test
  public void testDoubleLoadReadNoRehashBytes() {
    LOG.info("Test double load and read no rehash bytes");
    doubleLoadReadBytes(100000);
  }
  
  @Test
  public void testDoubleLoadReadNoRehashMemory() {
    LOG.info("Test double load and read no rehash memory");
    doubleLoadReadMemory(100000);
  }
  
  @Test
  public void testLoadReadWithRehashBytes() {
    LOG.info("Test load and read with rehash bytes");
    loadReadBytes(1000000);
  }
  
  @Test
  public void testLoadReadWithRehashMemory() {
    LOG.info("Test load and read with rehash memory");
    loadReadMemory(1000000);
  }

  @Test
  public void testLoadReadDeleteWithRehashBytes() {
    LOG.info("Test load and read-delete with rehash bytes");
    loadReadBytes(1000000);
    deleteIndexBytes();
    verifyIndexBytesNot();
  }
  
  @Test
  public void testLoadReadDeleteWithRehashMemory() {
    LOG.info("Test load and read with rehash memory");
    loadReadMemory(1000000);
    deleteIndexMemory();
    verifyIndexMemoryNot();
  }
  
  @Test
  public void testDoubleLoadReadWithRehashBytes() {
    LOG.info("Test double load and read with rehash bytes");
    doubleLoadReadBytes(1000000);
  }
  
  @Test
  public void testDoubleLoadReadWithRehashMemory() {
    LOG.info("Test double load and read with rehash memory");
    doubleLoadReadMemory(1000000);
  }
  
  @Test
  public void testEvictionBytes() {
    LOG.info("Test eviction bytes");

    memoryIndex.setMaximumSize(100000);
    prepareData(200000);
    loadIndexBytes();
    
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
    LOG.info("Test eviction memory");

    memoryIndex.setMaximumSize(100000);
    prepareData(200000);
    loadIndexMemory();
    
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
    LOG.info("Test load save");
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
  
  private void doubleLoadReadBytes(int num) {
    prepareData(num);
    loadIndexBytes();
    long size = memoryIndex.size();
    assertEquals((long)num, size);
    verifyIndexBytes();
    loadIndexBytesNot();
    size = memoryIndex.size();
    assertEquals(0L, size);
    verifyIndexBytesNot();
  }
  
  private void loadReadMemory(int num) {
    prepareData(num);
    loadIndexMemory();
    long size = memoryIndex.size();
    assertEquals((long)num, size);
    verifyIndexMemory();
  }
  
  private void doubleLoadReadMemory(int num) {
    prepareData(num);
    loadIndexMemory();
    long size = memoryIndex.size();
    assertEquals((long)num, size);
    verifyIndexMemory();
    loadIndexMemoryNot();
    size = memoryIndex.size();
    assertEquals(0L, size);
    verifyIndexMemoryNot();
  }
}
