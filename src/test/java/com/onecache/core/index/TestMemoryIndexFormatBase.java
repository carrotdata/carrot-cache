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
package com.onecache.core.index;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.onecache.core.index.IndexFormat;
import com.onecache.core.index.MemoryIndex;
import com.onecache.core.index.MemoryIndex.MutationResult;
import com.onecache.core.util.UnsafeAccess;

public abstract class TestMemoryIndexFormatBase extends TestMemoryIndexBase{
  /** Logger */
  @SuppressWarnings("unused")
  private static final Logger LOG = LogManager.getLogger(TestMemoryIndexFormatBase.class);
  
  @Before
  public void setUp() {
    memoryIndex = getMemoryIndex();
  }
  
  /**
   * Subclasses must provide memory index implementation for testing
   * @return
   */
  protected abstract MemoryIndex getMemoryIndex();
  
  protected int loadIndexBytes() {
    int loaded = 0;
    for(int i = 0; i < numRecords; i++) {
      MutationResult res = memoryIndex.insert(keys[i], 0, keySize, values[i], 0, values[i].length, 
        sids[i], offsets[i], expires[i]);
      if (res == MutationResult.INSERTED) {
        loaded++;
      }
    }
    return loaded;
  }
  
  protected int loadIndexBytesWithEviction(int evictionStartFrom) {
    int loaded = 0;
    for(int i = 0; i < numRecords; i++) {
      MutationResult res = memoryIndex.insert(keys[i], 0, keySize, values[i], 0, values[i].length, 
        sids[i], offsets[i], expires[i]);
      if (res == MutationResult.INSERTED && !memoryIndex.isEvictionEnabled()) {
        loaded++;
      } else if (res == MutationResult.UPDATED && memoryIndex.isEvictionEnabled()) {
        loaded--;
      }
      if (i == evictionStartFrom - 1) {
        memoryIndex.setEvictionEnabled(true);
      }
    }
    return loaded;
  }
  
  protected void verifyIndexBytes(int expected) {
    verifyIndexBytes(false, expected);
  }

  protected void verifyIndexBytes(boolean hit, int expected) {
    
    IndexFormat format = memoryIndex.getIndexFormat();
    int entrySize = format.indexEntrySize();
    long buf = UnsafeAccess.mallocZeroed(entrySize);
    int verified = 0;
    for (int i = 0; i < numRecords; i++) {
      int result = (int) memoryIndex.find(keys[i], 0, keySize, hit, buf, entrySize);
      if (result > 0) {
        assertEquals(entrySize, result);
      } else {
        continue;
      }
      
      short sid = (short)format.getSegmentId(buf);
      int offset = (int) format.getOffset(buf);
      int size = (int) format.getKeyValueSize(buf);
      if (sids[i] != sid) {
        continue;
      }
      if(offsets[i] != offset) {
        continue;
      }
      if (sizes[i] != size) {
        continue;
      }
      long expire = memoryIndex.getExpire(keys[i], 0, keySize);
      if (!sameExpire(expires[i], expire)) {
        continue;
      }
      verified ++;
    }
    UnsafeAccess.free(buf);
    assertEquals(expected, verified);
  }
  
  protected void verifyGetSetExpireBytes(int expected) {
    
    IndexFormat format = memoryIndex.getIndexFormat();
    if (!format.isExpirationSupported()) {
      return;
    }
    int verified = 0;
    for (int i = 0; i < numRecords; i++) {
      
      long expire = memoryIndex.getExpire(keys[i], 0, keySize);
      if (!sameExpire(expires[i], expire)) {
        continue;
      }
      // Set expire + 100000
      long newExpire = expire + 100000;
      long result = memoryIndex.getAndSetExpire(keys[i], 0,  keySize, newExpire);
      if(!sameExpire(expires[i], result)) {
        continue;
      }
      expire = memoryIndex.getExpire(keys[i], 0, keySize);
      if(!sameExpire(newExpire, expire)) {
        continue;
      }
      verified ++;
    }
    System.out.println("verified="+ verified);
    assertEquals(expected, verified);
  }
  
 protected void verifyGetSetExpireMemory(int expected) {
    
    IndexFormat format = memoryIndex.getIndexFormat();
    if (!format.isExpirationSupported()) {
      return;
    }
    int verified = 0;
    for (int i = 0; i < numRecords; i++) {
      long expire = memoryIndex.getExpire(mKeys[i], keySize);
      if (!sameExpire(expires[i], expire)) {
        continue;
      }
      // Set expire + 100000
      long newExpire = expire + 100000;
      long result = memoryIndex.getAndSetExpire(mKeys[i], keySize, newExpire);
      if(!sameExpire(expires[i], result)) {
        continue;
      }
      expire = memoryIndex.getExpire(mKeys[i], keySize);
      if(!sameExpire(newExpire, expire)) {
        continue;
      }
      verified ++;
    }
    System.out.println("verified="+ verified);
    assertEquals(expected, verified);
  }
  
  protected int loadIndexMemory() {
    int loaded = 0;
    for(int i = 0; i < numRecords; i++) {
      MutationResult res = memoryIndex.insert(mKeys[i], keySize, mValues[i], valueSize, sids[i], offsets[i], expires[i]);
      if (res == MutationResult.INSERTED) {
        loaded++;
      }
    }
    return loaded;
  }
  
  protected int loadIndexMemoryWithEviction(int evictionStartFrom) {
    int loaded = 0;
    for(int i = 0; i < numRecords; i++) {
      MutationResult res = memoryIndex.insert(mKeys[i], keySize, mValues[i], valueSize, sids[i], offsets[i], expires[i]);
      if (res == MutationResult.INSERTED && !memoryIndex.isEvictionEnabled()) {
        loaded++;
      } else if (res == MutationResult.UPDATED && memoryIndex.isEvictionEnabled()) {
        loaded--;
      }
      if (i == evictionStartFrom - 1) {
        /*DEBUG*/ System.out.println("Eviction enabled");
        memoryIndex.setEvictionEnabled(true);
      }
    }
    return loaded;
  }
  
  protected void verifyIndexMemory(int expected) {
    verifyIndexMemory(false, expected);
  }
  
  protected void verifyIndexMemory(boolean hit, int expected) {
    IndexFormat format = memoryIndex.getIndexFormat();
    int entrySize = format.indexEntrySize();
    long buf = UnsafeAccess.mallocZeroed(entrySize);
    int verified = 0;
    for (int i = 0; i < numRecords; i++) {
      int result = (int) memoryIndex.find(mKeys[i], keySize, hit, buf, entrySize);
      if (result > 0) {
        assertEquals(entrySize, result);
      } else {
        continue;
      }
      short sid = (short)format.getSegmentId(buf);
      int offset = (int) format.getOffset(buf);
      int size = (int) format.getKeyValueSize(buf);
      if (sids[i] !=  sid) {
        continue;
      }
      if (offsets[i] != offset) {
        continue;
      }
      if (sizes[i] != size) {
        continue;
      }
      long expire = memoryIndex.getExpire(mKeys[i], keySize);
      if (!sameExpire(expires[i], expire)) {
        continue;
      }
      verified++;
    }
    UnsafeAccess.free(buf);
    assertEquals(expected, verified);
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
    int loaded = loadReadBytes(100000);
    deleteIndexBytes(loaded);
    verifyIndexBytesNot();
  }
  
  @Test
  public void testLoadReadDeleteNoRehashMemory() {
    System.out.println("Test load and read-delete no rehash memory");
    int loaded = loadReadMemory(100000);
    deleteIndexMemory(loaded);
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
    int loaded = loadReadBytes(1000000);
    deleteIndexBytes(loaded);
    verifyIndexBytesNot();
  }
  
  @Test
  public void testLoadReadDeleteWithRehashMemory() {
    System.out.println("Test load and read with rehash memory");
    int loaded = loadReadMemory(1000000);
    deleteIndexMemory(loaded);
    verifyIndexMemoryNot();
  }
  
  @Test
  public void testGetSetExpireBytes() {
    System.out.println("Test get set expire bytes");
    loadVerifyGetSetExpireBytes(100000);
  }
  
  @Test
  public void testGetSetExpireMemory() {
    System.out.println("Test get set expire memory");
    loadVerifyGetSetExpireMemory(100000);
  }
  
  @Ignore
  @Test
  public void testEvictionBytes() {
    // We do not support pure eviction anymore - only expired items
    System.out.println("Test eviction bytes");
    int toLoad = 200000;
    
    prepareData(toLoad);
    int loaded = loadIndexBytesWithEviction(toLoad / 2);
    
    long size = memoryIndex.size();
    assertEquals(loaded, size);
    
    IndexFormat format = memoryIndex.getIndexFormat();
    int entrySize = format.indexEntrySize();
    long buf = UnsafeAccess.mallocZeroed(entrySize);
    
    int evicted1 = 0;
    int evicted2 = 0;
    for (int i = 0; i < toLoad / 2; i++) {
      int result = (int) memoryIndex.find(keys[i], 0, keySize, false, buf, entrySize);
      if (result == -1) {
        evicted1++;
      } else {
        int sid = format.getSegmentId(buf);
        long off = format.getOffset(buf);
        int kvSize = format.getKeyValueSize(buf);
        long expire = memoryIndex.getExpire(keys[i], 0, keys[i].length);
        if (sids[i] != sid || offsets[i] != off || 
            sizes[i] != kvSize || !sameExpire(expires[i], expire)) {
          evicted1++;
        }
      }
    }
    
    for (int i = toLoad / 2; i < toLoad; i++) {
      int result = (int) memoryIndex.find(keys[i], 0, keySize, false, buf, entrySize);
      if (result == -1) {
        evicted2 ++;
      } else {
        int sid = format.getSegmentId(buf);
        long off = format.getOffset(buf);
        int kvSize = format.getKeyValueSize(buf);
        long expire = memoryIndex.getExpire(keys[i], 0, keySize);
        if (sids[i] != sid || offsets[i] != off || 
            sizes[i] != kvSize || !sameExpire(expires[i], expire)) {
          evicted1++;
        }
      }
    }
    System.out.println("evicted1=" + evicted1 + " evicted2="+ evicted2);
    assertEquals(toLoad - loaded, evicted1 + evicted2);
    UnsafeAccess.free(buf); 
  }
  
  @Ignore
  @Test
  public void testEvictionMemory() {
    // We do not support pure eviction anymore - only expired items
    System.out.println("Test eviction memory");
    int toLoad = 200000;
    prepareData(toLoad);
    long loaded = loadIndexMemoryWithEviction(toLoad / 2);
    
    long size = memoryIndex.size();
    assertEquals(loaded, size);
    
    IndexFormat format = memoryIndex.getIndexFormat();
    int entrySize = format.indexEntrySize();
    long buf = UnsafeAccess.mallocZeroed(entrySize);
    
    int evicted1 = 0;
    int evicted2 = 0;
    for (int i = 0; i < toLoad / 2; i++) {
      int result = (int) memoryIndex.find(mKeys[i], keySize, false, buf, entrySize);
      if (result == -1) {
        evicted1++;
      } else {
        int sid = format.getSegmentId(buf);
        long off = format.getOffset(buf);
        int kvSize = format.getKeyValueSize(buf);
        long expire = memoryIndex.getExpire(mKeys[i], keySize);
        if (sids[i] != sid || offsets[i] != off || 
            sizes[i] != kvSize || !sameExpire(expires[i], expire)) {
          evicted1++;
        }
      }
    }
    
    for (int i = toLoad / 2; i < toLoad; i++) {
      int result = (int) memoryIndex.find(mKeys[i], keySize, false, buf, entrySize);
      if (result == -1) {
        evicted2 ++;
      } else {
        int sid = format.getSegmentId(buf);
        long off = format.getOffset(buf);
        int kvSize = format.getKeyValueSize(buf);
        long expire = memoryIndex.getExpire(mKeys[i], keySize);
        if (sids[i] != sid || offsets[i] != off || 
            sizes[i] != kvSize || !sameExpire(expires[i], expire)) {
          evicted1++;
        }
      }
    }
    System.out.println("evicted1=" + evicted1 + " evicted2="+ evicted2);
    assertEquals((int)(toLoad - loaded), evicted1 + evicted2);
    UnsafeAccess.free(buf);
  }
  
  @Test
  public void testLoadSave() throws IOException {
    System.out.println("Test load save");
    prepareData(100000);
    long loaded = loadIndexMemory();
    long size = memoryIndex.size();
    assertEquals(loaded, size);
    verifyIndexMemory((int)loaded);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    
    memoryIndex.save(dos);
    
    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    DataInputStream dis = new DataInputStream(bais);
    
    memoryIndex.dispose();
    
    memoryIndex = new MemoryIndex();
    memoryIndex.load(dis);
    
    verifyIndexMemory((int)loaded);

  }
  
  protected int loadReadBytes(int num) {
    prepareData(num);
    System.out.println("prepare done");
    int loaded = loadIndexBytes();
    System.out.println("load done: "+ loaded);
    long size = memoryIndex.size();
    assertEquals(loaded, (int)size);
    verifyIndexBytes(loaded);
    return loaded;
  }
  
  
  protected int loadVerifyGetSetExpireBytes(int num) {
    prepareData(num);
    System.out.println("prepare done");
    int loaded = loadIndexBytes();
    System.out.println("load done: "+ loaded);
    long size = memoryIndex.size();
    assertEquals(loaded, (int)size);
    verifyGetSetExpireBytes(loaded);
    return loaded;
  }
  
  protected int loadReadMemory(int num) {
    prepareData(num);
    int loaded = loadIndexMemory();
    long size = memoryIndex.size();
    assertEquals(loaded, (int) size);
    verifyIndexMemory(loaded);
    return loaded;
  }
  
  protected int loadVerifyGetSetExpireMemory(int num) {
    prepareData(num);
    int loaded = loadIndexMemory();
    long size = memoryIndex.size();
    assertEquals(loaded, (int) size);
    verifyGetSetExpireMemory(loaded);
    return loaded;
  }
  
  protected void loadReadBytesWithHit(int num) {
    int loaded = loadReadBytes(num);
    verifyIndexBytes(true, loaded);
    verifyIndexBytes(loaded);
    long size = memoryIndex.size();
    assertEquals(loaded, (int) size);
  }
  
  
  protected void loadReadMemoryWithHit(int num) {
    int loaded = loadReadMemory(num);
    verifyIndexMemory(true, loaded);
    verifyIndexMemory(loaded);
    long size = memoryIndex.size();
    assertEquals(loaded, (int) size);
  }
}
