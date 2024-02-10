package com.carrot.cache;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.Before;

import com.carrot.cache.compression.CodecFactory;
import com.carrot.cache.compression.CompressionCodec;
import com.carrot.cache.compression.zstd.ZstdCompressionCodec;
import com.carrot.cache.index.CompactBaseIndexFormat;
import com.carrot.cache.io.CompressedBlockDataWriter;
import com.carrot.cache.io.CompressedBlockFileDataReader;
import com.carrot.cache.io.CompressedBlockMemoryDataReader;
import com.carrot.cache.util.CarrotConfig;
import com.carrot.cache.util.TestUtils;
import com.carrot.cache.util.UnsafeAccess;
import com.carrot.cache.util.Utils;


public class TestCompressedCacheMultithreadedZipfBase extends TestCacheMultithreadedZipfBase {

  protected boolean dictionaryEnabled = true;
  protected boolean asyncTrainingMode = false;
  protected int dictionarySize = 1 << 16;
  protected int compLevel = 3; 
  
  protected List<byte[]> bValues;
  protected List<Long> mValues;
  
  @After  
  public void tearDown() throws IOException {
    super.tearDown();
    cleanDictionaries();
    ZstdCompressionCodec.reset();
    CodecFactory.getInstance().clear();
    // Release memory
    mValues.stream().forEach(x -> UnsafeAccess.free(x));
  }
  
  @Before
  public void setUp() throws IOException, URISyntaxException {
    this.offheap = true;
    this.numRecords = 1000000;
    this.numIterations = this.numRecords;
    this.numThreads = 4;
    this.maxCacheSize = 100 * this.segmentSize;
    this.scavNumberThreads = 2;
    cleanDictionaries();
    bValues = TestUtils.loadGithubDataAsBytes();
    mValues = TestUtils.loadGithubDataAsMemory();
  }
  
  /**
   * Subclasses may override
   * @param b builder instance
   * @return builder instance
   */
  @Override
  protected Builder withAddedConfigurations(Builder b) {
       
    b.withDataWriter(CompressedBlockDataWriter.class.getName());
    b.withMemoryDataReader(CompressedBlockMemoryDataReader.class.getName());
    b.withFileDataReader(CompressedBlockFileDataReader.class.getName());
    b.withCacheCompressionDictionaryEnabled(dictionaryEnabled);
    b.withCacheCompressionEnabled(true);
    b.withTLSSupported(true);
    b.withCacheCompressionDictionaryTrainingAsync(asyncTrainingMode);
    b.withMainQueueIndexFormat(CompactBaseIndexFormat.class.getName());
    b.withCacheCompressionKeysEnabled(true);
    b.withCacheCompressionDictionarySize(dictionarySize);
    b.withCacheCompressionLevel(compLevel);
    
    try {
      initCodecs();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return b;
  }
  
  private byte[] getKey(int n) {
    return ("KEY:" + n).getBytes();
  }
  
  @Override
  protected final boolean loadBytesStream(int n) throws IOException {
   
    byte[] key = getKey(n);
    byte[] value = bValues.get(n % bValues.size());
    long expire = getExpire(n);
    boolean result = this.cache.put(key, value, expire);
    return result;
  }
  
  @Override
  protected final boolean verifyBytesStream(int n, byte[] buffer) throws IOException {
    
    byte[] key = getKey(n);
    byte[] value = bValues.get(n % bValues.size());
    long expSize = Utils.kvSize(key.length, value.length);
    long size = this.cache.getKeyValue(key, 0, key.length, false, buffer, 0);
    if (size < 0) {
      return false;
    }
    try {
      assertEquals(expSize, size);
      int kSize = Utils.readUVInt(buffer, 0);
      assertEquals(key.length, kSize);
      int kSizeSize = Utils.sizeUVInt(kSize);
      int vSize = Utils.readUVInt(buffer, kSizeSize);
      assertEquals(value.length, vSize);
      int vSizeSize = Utils.sizeUVInt(vSize);
      int off = kSizeSize + vSizeSize;
      assertTrue(Utils.compareTo(buffer, off, kSize, key, 0, key.length) == 0);
      off += kSize;
      assertTrue(Utils.compareTo(buffer, off, vSize, value, 0, value.length) == 0);
      
    } catch(AssertionError e) {
      return false;
    } 
    return true;
  }

  @Override
  protected final boolean loadMemoryStream(int n) throws IOException {
    
    byte[] key = getKey(n);
    int keySize = key.length;
    long keyPtr = TestUtils.copyToMemory(key);
    long valuePtr = mValues.get(n % mValues.size());
    int valueSize = bValues.get(n % bValues.size()).length;
    long expire = getExpire(n);
    boolean result = this.cache.put(keyPtr, keySize, valuePtr, valueSize, expire);
    UnsafeAccess.free(keyPtr);
    return result;
  }
  
  @Override
  protected final boolean verifyMemoryStream(int n, ByteBuffer buffer) throws IOException {

    byte[] key = getKey(n);
    int keySize = key.length;
    long keyPtr = TestUtils.copyToMemory(key);
    long valuePtr = mValues.get(n % mValues.size());
    int valueSize = bValues.get(n % bValues.size()).length;

    long expSize = Utils.kvSize(keySize, valueSize);
    long size = this.cache.getKeyValue(keyPtr, keySize, buffer);
    if (size < 0) {
      UnsafeAccess.free(keyPtr);
      return false;
    }
    try {
      assertEquals(expSize, size);
      int kSize = Utils.readUVInt(buffer);
      assertEquals(keySize, kSize);
      int kSizeSize = Utils.sizeUVInt(kSize);
      int off = kSizeSize;
      buffer.position(off);
      int vSize = Utils.readUVInt(buffer);
      assertEquals(valueSize, vSize);
      int vSizeSize = Utils.sizeUVInt(vSize);
      off += vSizeSize;
      buffer.position(off);
      assertTrue(Utils.compareTo(buffer, kSize, keyPtr, keySize) == 0);
      off += keySize;
      buffer.position(off);
      assertTrue(Utils.compareTo(buffer, vSize, valuePtr, valueSize) == 0);

    } catch (AssertionError e) {
      return false;
    } finally {
      UnsafeAccess.free(keyPtr);
      buffer.clear();
    }
    return true;
  }
  
  protected void cleanDictionaries() {
    cleanDictionaries(this.parentCacheName);
    cleanDictionaries(this.victimCacheName);
  }
  
  protected void initCodecs() throws IOException {
    initCodec(parentCacheName);
    initCodec(victimCacheName);
  }
  
  protected void cleanDictionaries(String cacheName) {
    // Clean up dictionaries
    CarrotConfig config = CarrotConfig.getInstance();
    String dictDir = config.getCacheDictionaryDir(cacheName);
    File dir = new File(dictDir);
    if (dir.exists()) {
      File[] files = dir.listFiles();
      Arrays.stream(files).forEach( x -> x.delete());
    }
  }
  
  protected void initCodec(String cacheName) throws IOException {
    CodecFactory factory = CodecFactory.getInstance();
    //factory.clear();
    CompressionCodec codec = factory.getCompressionCodecForCache(cacheName);
    if (codec == null) {
      factory.initCompressionCodecForCache(cacheName, null);
    }
  }
}
