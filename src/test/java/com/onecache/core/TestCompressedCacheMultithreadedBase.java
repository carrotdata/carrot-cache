package com.onecache.core;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.onecache.core.compression.CodecFactory;
import com.onecache.core.compression.CompressionCodec;
import com.onecache.core.compression.zstd.ZstdCompressionCodec;
import com.onecache.core.controllers.LRCRecyclingSelector;
import com.onecache.core.io.CompressedBlockBatchDataWriter;
import com.onecache.core.io.CompressedBlockFileDataReader;
import com.onecache.core.io.CompressedBlockMemoryDataReader;
import com.onecache.core.util.CacheConfig;
import com.onecache.core.util.TestUtils;
import com.onecache.core.util.UnsafeAccess;
import com.onecache.core.util.Utils;


public abstract class TestCompressedCacheMultithreadedBase {
  private static final Logger LOG = LoggerFactory.getLogger(TestCompressedCacheMultithreadedBase.class);

  protected String cacheName = "cache";
  
  protected boolean dictionaryEnabled = true;
  protected boolean asyncTrainingMode = false;
  protected int dictionarySize = 1 << 16;
  protected int compLevel = 3; 
  
  protected boolean offheap = false;
  protected int numRecords;
  protected int numThreads = 1;
  
  protected int segmentSize =  4 << 20;
  
  protected long maxCacheSize = 4 << 20;
  
  protected int scavNumberThreads = 1;
  
  protected int scavengerInterval = 10;
  
  protected float scavDumpBelowRatio = 0.5f;
  
  protected List<byte[]> bValues;
  protected List<Long> mValues;
  
  protected Cache cache;
  
  @After  
  public void tearDown() throws IOException {
    cache.printStats();
    cleanDictionaries();
    ZstdCompressionCodec.reset();
    CodecFactory.getInstance().clear();
    // Release memory
    mValues.stream().forEach(x -> UnsafeAccess.free(x));
    this.cache.dispose();
  }
  
  @Before
  public void setUp() throws IOException, URISyntaxException {
    this.offheap = true;
    this.numRecords = 10000000;
    this.numThreads = 4;
    this.maxCacheSize = 1000 * this.segmentSize;
    this.scavNumberThreads = 2;
    cleanDictionaries();
    bValues = TestUtils.loadGithubDataAsBytes();
    mValues = TestUtils.loadGithubDataAsMemory();
  }
  
  /**
   * Subclasses may override
   * @param b builder instance
   * @return builder instance
   * @throws IOException 
   */
  protected Cache createCache() throws IOException {
    // Data directory
    Path path = Files.createTempDirectory(null);
    File  dir = path.toFile();
    dir.deleteOnExit();
    String rootDir = dir.getAbsolutePath();
    
    Builder b = new Builder(cacheName);
    
    b.withDataWriter(CompressedBlockBatchDataWriter.class.getName());
    b.withMemoryDataReader(CompressedBlockMemoryDataReader.class.getName());
    b.withFileDataReader(CompressedBlockFileDataReader.class.getName());
    b.withCacheCompressionDictionaryEnabled(dictionaryEnabled);
    b.withCacheCompressionEnabled(true);
    b.withTLSSupported(true);
    b.withCacheCompressionKeysEnabled(true);
    b.withCacheCompressionDictionarySize(dictionarySize);
    b.withCacheCompressionLevel(compLevel);
    b.withCacheDataSegmentSize(segmentSize);
    b.withCacheCompressionDictionaryTrainingAsync(true);
    b.withCacheMaximumSize(maxCacheSize);
    b.withScavengerRunInterval(scavengerInterval);
    b.withScavengerDumpEntryBelowMin(scavDumpBelowRatio);
    b.withRecyclingSelector(LRCRecyclingSelector.class.getName());
    b.withCacheRootDir(rootDir);
    
    try {
      initCodecs();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    if (offheap) {
      return b.buildMemoryCache();
    } else {
      return b.buildDiskCache();
    }
  }
  
  @Test
  public void testLoadVerifyBytesMultithreaded() throws InterruptedException {
    Runnable r = () -> {
      try {
        testLoadVerifyBytes();
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    };
    Thread[] workers = new Thread[numThreads];
    for (int i = 0; i < numThreads; i++) {
      workers[i] = new Thread(r);
      //Thread.sleep(1000);
      workers[i].start();
    }
    for (int i = 0; i < numThreads; i++) {
      workers[i].join();
    }
  }
  
  @Test
  public void testLoadVerifyMemoryMultithreaded() throws InterruptedException {
    Runnable r = () -> {
      try {
        testLoadVerifyMemory();
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    };
    Thread[] workers = new Thread[numThreads];
    for (int i = 0; i < numThreads; i++) {
      workers[i] = new Thread(r);
      workers[i].start();
    }
    for (int i = 0; i < numThreads; i++) {
      workers[i].join();
    }
  }
  private void testLoadVerifyBytes() throws IOException {
    int loaded = loadBytes();
    verifyBytes(loaded);
  }
  
  private void testLoadVerifyMemory() throws IOException {
    int loaded = loadMemory();
    verifyMemory(loaded);
  }
  

  private byte[] getKey(int n) {
    return (Thread.currentThread() + ":" + n).getBytes();
  }
  
  protected final int loadBytes() throws IOException {
    int loaded = 0;
    long t1 = System.currentTimeMillis();
    for (int i = 0; i < numRecords; i++) {
      byte[] key = getKey(i);
      byte[] value = bValues.get(i % bValues.size());
      long expire = getExpire(i);
      boolean result = this.cache.put(key, value, expire);
      if (!result) {
        break;
      }
      loaded++;
    }
    long t2 = System.currentTimeMillis();
    LOG.info("{} loaded bytes {} in {}ms. RPS={}", Thread.currentThread().getName(), loaded, (t2 - t1), loaded * 1000L/(t2 - t1));
    return loaded;
  }
  
  protected final int verifyBytes(int loaded) throws IOException {

    byte[] buffer = new byte[10000];
    int verified = 0;
    int notFound = 0;
    long t1 = System.currentTimeMillis();
    for (int n = 0; n < loaded; n++) {
      byte[] key = getKey(n);
      byte[] value = bValues.get(n % bValues.size());
      long expSize = Utils.kvSize(key.length, value.length);
      long size = this.cache.getKeyValue(key, 0, key.length, false, buffer, 0);
      if (size < 0) {
        //LOG.error("not found={}", n);
        notFound++;
        continue;
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

      } catch (AssertionError e) {
        continue;
      }
      verified++;
    }
    long t2 = System.currentTimeMillis();
    LOG.info("{} verified bytes {} in {}ms, not found={} RPS={}", Thread.currentThread().getName(), 
      verified, t2 - t1, notFound, loaded * 1000L/(t2 - t1));

    return verified;
  }

  protected final int loadMemory() throws IOException {
    int loaded = 0;
    long t1 = System.currentTimeMillis();

    for (int n = 0; n < numRecords; n++) {
      byte[] key = getKey(n);
      int keySize = key.length;
      long keyPtr = TestUtils.copyToMemory(key);
      long valuePtr = mValues.get(n % mValues.size());
      int valueSize = bValues.get(n % bValues.size()).length;
      long expire = getExpire(n);
      boolean result = this.cache.put(keyPtr, keySize, valuePtr, valueSize, expire);
      UnsafeAccess.free(keyPtr);
      if (!result) break;
      loaded ++;
    }
    long t2 = System.currentTimeMillis();
    LOG.info("{} loaded memory {} in {}ms. RPS={}", Thread.currentThread().getName(), loaded, t2 - t1, loaded * 1000L/(t2 - t1));

    return loaded;
  }

  protected final int verifyMemory(int loaded) throws IOException {

    int verified = 0;
    int notFound = 0;
    byte[] buffer = new byte[10000];
    long t1 = System.currentTimeMillis();
    
    for (int n = 0; n < loaded; n++) {
      byte[] key = getKey(n);
      int keySize = key.length;
      long keyPtr = TestUtils.copyToMemory(key);
      long valuePtr = mValues.get(n % mValues.size());
      int valueSize = bValues.get(n % bValues.size()).length;

      long expSize = Utils.kvSize(keySize, valueSize);
      long size = this.cache.getKeyValue(keyPtr, keySize, buffer, 0);
      if (size < 0) {
        UnsafeAccess.free(keyPtr);
        notFound++;
        continue;
      }
      try {
        assertEquals(expSize, size);
        int kSize = Utils.readUVInt(buffer, 0);
        assertEquals(keySize, kSize);
        int kSizeSize = Utils.sizeUVInt(kSize);
        int off = kSizeSize;
        int vSize = Utils.readUVInt(buffer, off);
        assertEquals(valueSize, vSize);
        int vSizeSize = Utils.sizeUVInt(vSize);
        off += vSizeSize;
        assertTrue(Utils.compareTo(buffer, off, kSize, keyPtr, keySize) == 0);
        off += keySize;
        assertTrue(Utils.compareTo(buffer, off, vSize, valuePtr, valueSize) == 0);
        verified++;
      } catch (AssertionError e) {
        continue;
      } finally {
        UnsafeAccess.free(keyPtr);
      }
    }
    long t2 = System.currentTimeMillis();
    LOG.info("{} verified memory {} in {}ms, not found={} RPS={}", Thread.currentThread().getName(), 
      verified, t2 - t1, notFound, loaded * 1000L/(t2 - t1));

    return verified;
  }
  
  protected long getExpire(int n) {
    return System.currentTimeMillis() + 1000000L;
  }
  
  protected void cleanDictionaries() {
    cleanDictionaries(this.cacheName);
  }
  
  protected void initCodecs() throws IOException {
    initCodec(this.cacheName);
  }
  
  protected void cleanDictionaries(String cacheName) {
    // Clean up dictionaries
    CacheConfig config = CacheConfig.getInstance();
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
