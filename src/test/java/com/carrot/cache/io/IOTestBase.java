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
package com.carrot.cache.io;

import static com.carrot.cache.io.BlockReaderWriterSupport.META_SIZE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Arrays;
import java.util.Random;

import org.junit.After;
import org.junit.BeforeClass;

import com.carrot.cache.index.IndexFormat;
import com.carrot.cache.index.MemoryIndex;
import com.carrot.cache.util.TestUtils;
import com.carrot.cache.util.UnsafeAccess;
import com.carrot.cache.util.Utils;

public abstract class IOTestBase {
  
  int blockSize = 4096;
  int numRecords = 10;
  int maxKeySize = 32;
  int maxValueSize = 5000;
  Random r;
  
  byte[][] keys;
  byte[][] values;
  long[] mKeys;
  long[] mValues;
  long[] expires;
  
  int segmentSize;
  
  Segment segment;
  MemoryIndex index;
  
  @BeforeClass
  public static void enableMallocDebug() {
//    UnsafeAccess.setMallocDebugEnabled(true);
//    UnsafeAccess.setMallocDebugStackTraceEnabled(true);
//    UnsafeAccess.setStackTraceRecordingFilter(x -> x == 1024);
//    UnsafeAccess.setStackTraceRecordingLimit(20000);
  }
  
  @After
  public void tearDown() {
    if (this.index != null) {
      this.index.dispose();
    }
    Arrays.stream(mKeys).forEach(x -> UnsafeAccess.free(x));
    Arrays.stream(mValues).forEach(x -> UnsafeAccess.free(x));
  }
  
  protected void prepareData(int numRecords) {
    this.numRecords = numRecords;
    keys = new byte[numRecords][];
    values = new byte[numRecords][];
    mKeys = new long[numRecords];
    mValues = new long[numRecords];
    expires = new long[numRecords];
    
    Random r = new Random();
    long seed = System.currentTimeMillis();
    r.setSeed(seed);
    System.out.println("seed="+ seed);
    
    for (int i = 0; i < numRecords; i++) {
      int keySize = nextKeySize();
      int valueSize = nextValueSize();
      keys[i] = TestUtils.randomBytes(keySize, r);
      values[i] = TestUtils.randomBytes(valueSize, r);
      mKeys[i] = TestUtils.randomMemory(keySize, r);
      mValues[i] = TestUtils.randomMemory(valueSize, r);
      expires[i] = System.currentTimeMillis() + i; // To make sure that we have distinct expiration values
    }  
  }
  
  protected int loadBytes() {
    int count = 0;
    int sid = this.segment.getId();

    IndexFormat format = this.index != null? this.index.getIndexFormat(): null;
    int indexSize = this.index != null? format.indexEntrySize(): 0;
    long indexBuf = this.index != null? UnsafeAccess.malloc(indexSize): 0L;
    
    while(count < this.numRecords) {
      long expire = expires[count];
      byte[] key = keys[count];
      byte[] value = values[count];
      int size = Utils.kvSize(key.length, value.length);
      long offset = segment.append(key, 0, key.length, value, 0, value.length, expire);
      if (offset < 0) {
        break;
      }
      
      if (this.index != null) {
        format.writeIndex(0L, indexBuf, key, 0, key.length, value, 0, value.length, 
          sid, (int) offset, size, expire);
        index.insert(key, 0, key.length, indexBuf, indexSize);
      }
      count++;
    }    
    if (indexBuf > 0) {
      UnsafeAccess.free(indexBuf);
    }
    return count;
  }
  
  protected int loadMemory() {
    int count = 0;
    int sid = this.segment.getId();

    IndexFormat format = this.index != null? this.index.getIndexFormat(): null;
    int indexSize = this.index != null? format.indexEntrySize(): 0;
    long indexBuf = this.index != null? UnsafeAccess.malloc(indexSize): 0L;
   
    while(count < this.numRecords) {
      long expire = expires[count];
      byte[] key = keys[count];
      byte[] value = values[count];
      int keySize = key.length;
      int valueSize = value.length;
      int size = Utils.kvSize(key.length, value.length);
      long keyPtr = mKeys[count];
      long valuePtr = mValues[count];
      long offset = segment.append(keyPtr, keySize, valuePtr, valueSize, expire);

      if (offset < 0) {
        break;
      }
      
      if (this.index != null) {
        format.writeIndex(0L, indexBuf, keyPtr, keySize, valuePtr, valueSize, 
          sid, (int) offset, size, expire);
        index.insert(keyPtr, keySize, indexBuf, indexSize);
      }
      count++;
    }
    if (indexBuf > 0) {
      UnsafeAccess.free(indexBuf);
    }
    return count;
  }
  
  protected void verifyBytes(int num) {
    long ptr = segment.getAddress();
    
    for (int i = 0; i < num; i++) {
      byte[] key = keys[i];
      byte[] value = values[i];
      int kSize = Utils.readUVInt(ptr);
      int kSizeSize = Utils.sizeUVInt(kSize);
      int vSize = Utils.readUVInt(ptr + kSizeSize);
      int vSizeSize = Utils.sizeUVInt(vSize);
      assertEquals(key.length, kSize);
      assertEquals(value.length, vSize);
      assertTrue(Utils.compareTo(key, 0, key.length, ptr + kSizeSize + vSizeSize, kSize) == 0);
      assertTrue(Utils.compareTo(value, 0, value.length, ptr + kSizeSize + vSizeSize + kSize, vSize) == 0);
      ptr += kSize + vSize + kSizeSize + vSizeSize;
    }
  }
  
  protected void verifyScanner(SegmentScanner scanner, int num) throws IOException {
    int n = 0;
    while(scanner.hasNext()) {
      byte[] key = keys[n];
      byte[] value = values[n];
      long keyPtr = scanner.keyAddress();
      int keySize = scanner.keyLength();
      long valuePtr = scanner.valueAddress();
      int valueSize = scanner.valueLength();
      assertEquals(key.length, keySize);
      assertEquals(value.length, valueSize);
      assertTrue(Utils.compareTo(key, 0, key.length, keyPtr, keySize) == 0);
      assertTrue(Utils.compareTo(value, 0, value.length, valuePtr, valueSize) == 0);
      n++;
      scanner.next();
    }
    assertEquals(num, n);
    scanner.close();
  }
  
  protected void verifyScannerFile(SegmentScanner scanner, int num) throws IOException {
    int n = 0;
    
    byte[] keyBuffer = new byte[Utils.kvSize(maxKeySize, maxValueSize)];
    byte[] valueBuffer = new byte[Utils.kvSize(maxKeySize, maxValueSize)];
    while(scanner.hasNext()) {
      byte[] key = keys[n];
      byte[] value = values[n];
      
      //System.out.println(n+ " : " + Utils.kvSize(key.length, value.length));

      int keySize = scanner.keyLength();
      int valueSize = scanner.valueLength();
      assertEquals(key.length, keySize);
      assertEquals(value.length, valueSize);
      
      int size = scanner.getKey(keyBuffer, 0);
      assertEquals(size, keySize);
      
      size = scanner.getValue(valueBuffer, 0);
      assertEquals(size, valueSize);

      assertTrue(Utils.compareTo(key, 0, key.length, keyBuffer, 0, keySize) == 0);
      assertTrue(Utils.compareTo(value, 0, value.length, valueBuffer, 0, valueSize) == 0);
      n++;
      scanner.next();
    }
    assertEquals(num, n);
    scanner.close();
  }
  
  protected void verifyMemory(int num) {
    long ptr = segment.getAddress();
    
    for (int i = 0; i < num; i++) {
      byte[] key = keys[i];
      byte[] value = values[i];
      int kSize = Utils.readUVInt(ptr);
      int kSizeSize = Utils.sizeUVInt(kSize);
      int vSize = Utils.readUVInt(ptr + kSizeSize);
      int vSizeSize = Utils.sizeUVInt(vSize);
      long mKey = mKeys[i];
      long mValue = mValues[i];
      assertEquals(key.length, kSize);
      assertEquals(value.length, vSize);
      assertTrue(Utils.compareTo(mKey, kSize, ptr + kSizeSize + vSizeSize, kSize) == 0);
      assertTrue(Utils.compareTo(mValue, vSize, ptr + kSizeSize + vSizeSize + kSize, vSize) == 0);
      ptr += kSize + vSize + kSizeSize + vSizeSize;
    }
  }

  private int safeBufferSize() {
    int bufSize = Utils.kvSize(maxKeySize, maxValueSize);
    return (bufSize / blockSize + 1) * blockSize;
  }
  
  protected void verifyBytesWithReader(int num, DataReader reader, IOEngine engine) 
      throws IOException {
    IndexFormat format = index.getIndexFormat();
    int indexSize = format.indexEntrySize();
    long indexBuf = UnsafeAccess.malloc(indexSize);
    int bufSize = safeBufferSize();
    byte[] buf = new byte[bufSize];
    int sid = segment.getId();
    for (int i = 0; i < num; i++) {
      byte[] key = keys[i];
      byte[] value = values[i];
      long result = index.find(key, 0, key.length, false, indexBuf, indexSize);
      assertEquals(indexSize, (int) result);
      
      int offset = (int) format.getOffset(indexBuf);
      int size = format.getKeyValueSize(indexBuf);
      int expSize = Utils.kvSize(key.length, value.length);
      assertEquals(expSize, size);
      
      int read = reader.read(engine, key, 0, key.length, sid, offset, size, buf, 0);
      assertEquals(expSize, read);
      
      int keySize = Utils.readUVInt(buf, 0);
      int kSizeSize = Utils.sizeUVInt(keySize);
      int valueSize  = Utils.readUVInt(buf, kSizeSize);
      int vSizeSize = Utils.sizeUVInt(valueSize);
      assertEquals(key.length, keySize);
      assertEquals(value.length, valueSize);
      assertTrue(Utils.compareTo(key, 0, key.length, buf, kSizeSize + vSizeSize, keySize) == 0);
      assertTrue(Utils.compareTo(value, 0, value.length, buf, kSizeSize + vSizeSize + keySize, valueSize) == 0);
    }
    
    UnsafeAccess.free(indexBuf);
  }
  
  protected void verifyMemoryWithReader(int num, DataReader reader, IOEngine engine) throws IOException {
    
    IndexFormat format = index.getIndexFormat();
    int indexSize = format.indexEntrySize();
    long indexBuf = UnsafeAccess.malloc(indexSize);
    int bufSize = safeBufferSize();//.kvSize(maxKeySize, maxValueSize);
    byte[] buf = new byte[bufSize];
    
    int sid = segment.getId();
    for (int i = 0; i < num; i++) {

      int kSize = keys[i].length;
      int vSize = values[i].length;
      long keyPtr = mKeys[i];
      long valuePtr = mValues[i];
      long result = index.find(keyPtr, kSize, false, indexBuf, indexSize);
      assertEquals(indexSize, (int) result);
      
      int offset = (int) format.getOffset(indexBuf);
      int size = format.getKeyValueSize(indexBuf);
      int expSize = Utils.kvSize(kSize, vSize);
      assertEquals(expSize, size);
      
      int read = reader.read(engine, keyPtr, kSize, sid, offset, size, buf, 0);
      assertEquals(expSize, read);
      
      int keySize = Utils.readUVInt(buf, 0);
      int kSizeSize = Utils.sizeUVInt(keySize);
      int valueSize  = Utils.readUVInt(buf, kSizeSize);
      int vSizeSize = Utils.sizeUVInt(valueSize);
      assertEquals(kSize, keySize);
      assertEquals(vSize, valueSize);
      assertTrue(Utils.compareTo(buf, kSizeSize + vSizeSize, keySize, keyPtr, kSize) == 0);
      assertTrue(Utils.compareTo(buf, kSizeSize + vSizeSize + keySize, valueSize, valuePtr, vSize) == 0);
    }
    UnsafeAccess.free(indexBuf);
  }

  protected void verifyBytesBlock(int num, int blockSize) {
    long ptr = segment.getAddress();
    for (int i = 0; i < num; i++) {
      int blockDataSize = UnsafeAccess.toInt(ptr);
      long $ptr = ptr + META_SIZE;
      int count = 0;
      while ($ptr < ptr + blockDataSize + META_SIZE) {
        byte[] key = keys[i + count];
        byte[] value = values[i + count];
        int kSize = Utils.readUVInt($ptr);
        int kSizeSize = Utils.sizeUVInt(kSize);
        int vSize = Utils.readUVInt($ptr + kSizeSize);
        int vSizeSize = Utils.sizeUVInt(vSize);
        assertEquals(key.length, kSize);
        assertEquals(value.length, vSize);
        assertTrue(Utils.compareTo(key, 0, key.length, $ptr + kSizeSize + vSizeSize, kSize) == 0);
        assertTrue(
            Utils.compareTo(value, 0, value.length, $ptr + kSizeSize + vSizeSize + kSize, vSize)
                == 0);
        $ptr += kSize + vSize + kSizeSize + vSizeSize;
        count++;
      }
      i += count - 1;
      ptr += ((blockDataSize + META_SIZE - 1)/blockSize + 1) * blockSize;
    }
  }

  protected void verifyMemoryBlock(int num, int blockSize) {
    long ptr = segment.getAddress();
    for (int i = 0; i < num; i++) {
      int blockDataSize = UnsafeAccess.toInt(ptr);
      long $ptr = ptr + META_SIZE;
      int count = 0;

      while ($ptr < ptr + blockDataSize + META_SIZE) {
        byte[] key = keys[i + count];
        byte[] value = values[i + count];
        long mKey = mKeys[i + count];
        long mValue = mValues[i + count];
        int kSize = Utils.readUVInt($ptr);
        int kSizeSize = Utils.sizeUVInt(kSize);
        int vSize = Utils.readUVInt($ptr + kSizeSize);
        int vSizeSize = Utils.sizeUVInt(vSize);
        assertEquals(key.length, kSize);
        assertEquals(value.length, vSize);
        assertTrue(Utils.compareTo(mKey, key.length, $ptr + kSizeSize + vSizeSize, kSize) == 0);
        assertTrue(
            Utils.compareTo(mValue, value.length, $ptr + kSizeSize + vSizeSize + kSize, vSize)
                == 0);
        $ptr += kSize + vSize + kSizeSize + vSizeSize;
        count++;
      }
      i += count - 1;
      ptr += ((blockDataSize + META_SIZE - 1)/blockSize + 1) * blockSize;
    }
  }

  protected int nextKeySize() {
    int size = this.maxKeySize / 2 + r.nextInt(this.maxKeySize / 2);
    return size;
  }

  protected int nextValueSize() {
    int size = 1 + r.nextInt(this.maxValueSize - 1);
    return size;
  }
  
  RandomAccessFile saveToFile(Segment s) throws IOException {
    File f = File.createTempFile("segment", null);
    f.deleteOnExit();
    FileOutputStream fos = new FileOutputStream(f);
    s.save(fos);
    fos.close();
    RandomAccessFile raf = new RandomAccessFile(f, "r");
    return raf;
  }
}
