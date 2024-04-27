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
package com.onecache.core.compression.zstd;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.luben.zstd.ZstdCompressCtx;
import com.github.luben.zstd.ZstdDecompressCtx;
import com.github.luben.zstd.ZstdDictCompress;
import com.github.luben.zstd.ZstdDictDecompress;
import com.github.luben.zstd.ZstdDictTrainer;
import com.onecache.core.compression.CompressionCodec;
import com.onecache.core.io.IOEngine;
import com.onecache.core.util.CacheConfig;
import com.onecache.core.util.UnsafeAccess;
import com.onecache.core.util.Utils;
/**
 * 
 * This class is a singleton for each cache instance, with each cache having 
 * its own codec. It makes extensive use of thread-local storage objects and, 
 * as a result, necessitates a fixed thread pool within the application.
 * Invariant: TLS is enabled, see CacheConfig::isCacheTLSSupported
 *
 */
public class ZstdCompressionCodec implements CompressionCodec {
  /** Logger */
  private static final Logger LOG = LoggerFactory.getLogger(ZstdCompressionCodec.class);
  private static int INIT_BUFFER_SIZE = 1 << 16; 
  
  static {
    
    INIT_BUFFER_SIZE = CacheConfig.getInstance().getCacheTLSInitialBufferSize(null);
    
  }
  
  private static ThreadLocal<byte[]> buffers = new ThreadLocal<byte[]>() {
    @Override
    protected byte[] initialValue() {
      return new byte[INIT_BUFFER_SIZE];
    }    
  };

  /**
   * Dictionary map. keys: cache names, values: map dictId -> data
   */
  static Map<String, Map<Integer, byte[]>> dictCacheMap = new ConcurrentHashMap<String, Map<Integer, byte[]>>();
  
  /**
   * Compression context objects. 
   * Map of maps: cache name -> {dictionaryId -> compression context}
   */
  static ThreadLocal<HashMap<String, HashMap<Integer, ZstdCompressCtx>>> compContextMap =
      new ThreadLocal<HashMap<String, HashMap<Integer, ZstdCompressCtx>>>() {

        @Override
        protected HashMap<String, HashMap<Integer, ZstdCompressCtx>>
            initialValue() {
          return new HashMap<String, HashMap<Integer,ZstdCompressCtx>>();
        }
    
  };
  
  /**
   * Decompression context objects. 
   * Map of maps: cache name -> {dictionaryId -> decompression context}
   */
  static ThreadLocal<HashMap<String, HashMap<Integer, ZstdDecompressCtx>>> decompContextMap =
      new ThreadLocal<HashMap<String, HashMap<Integer, ZstdDecompressCtx>>>() {

        @Override
        protected HashMap<String, HashMap<Integer, ZstdDecompressCtx>>
            initialValue() {
          return new HashMap<String, HashMap<Integer,ZstdDecompressCtx>>();
        }
  };
  
  /**
   * For testing
   */
  public static void reset() {
    dictCacheMap.clear();
    HashMap<String, HashMap<Integer, ZstdCompressCtx>> compContext = compContextMap.get();
    if (compContext != null) {
      for (HashMap<Integer, ZstdCompressCtx> v: compContext.values()) {
        for (ZstdCompressCtx ctxt: v.values()) {
          ctxt.reset();
        }
      }
    }
    compContextMap.set(new HashMap<String, HashMap<Integer,ZstdCompressCtx>>());
    HashMap<String, HashMap<Integer, ZstdDecompressCtx>> decompContext =decompContextMap.get();
    if (decompContext != null) {
      for (HashMap<Integer, ZstdDecompressCtx> v: decompContext.values()) {
        for (ZstdDecompressCtx ctxt: v.values()) {
          ctxt.reset();
        }
      }
    }
    decompContextMap.set(new HashMap<String, HashMap<Integer,ZstdDecompressCtx>>());
  }
  
  /**
   * Cache name
   */
  private String cacheName;
  
  /* Dictionary size in bytes */
  private int dictSize;
  
  /* Compression level */
  private int compLevel;
  
  /* Dictionary enabled */
  private boolean dictionaryEnabled;
  
  /* Current dictionary level (maximum) */
  private volatile int currentDictVersion;
  
  /* Training in progress */
  private volatile boolean trainingInProgress = false;
  
  /**
   * Finalizing training
   */
  private AtomicBoolean finalizingTraining = new AtomicBoolean(false);
  
  /* Current size of a training data */
  private AtomicInteger trainingDataSize;
  
  /* List of data pointers for training */
  private ConcurrentLinkedQueue<Long> trainingData;
  
  /* Is dictionary training in async mode*/
  private boolean trainingAsync;
  
  /**
   * Codec statistics
   */
  private Stats stats;
  
  @Override
  public int compress(long ptr, int len, int dictId) {
    byte[] buf = getBuffer(len);
    ZstdCompressCtx currentCtx = getCompressContext(dictId);    
    
    long startTime = System.nanoTime();
    int compressedSize = currentCtx.compressNativeByteArray(buf, 0, buf.length, ptr, len);
    long endTime = System.nanoTime();

    if (compressedSize >= len) {
      // do not copy
      return compressedSize;
    }
    UnsafeAccess.copy(buf, 0, ptr, compressedSize);
    this.stats.getCompressedRaw().addAndGet(len);
    this.stats.getCompressed().addAndGet(compressedSize);
    this.stats.getCompressionTime().addAndGet(endTime - startTime);
    // update statistics
    return compressedSize;
  }

  @Override
  public int compress(long ptr, int len, int dictId, long buffer, int bufferSize) {
    // sanity check?
    ZstdCompressCtx currentCtx = getCompressContext(dictId);    
    long startTime = System.nanoTime();
    int compressedSize = currentCtx.compressNativeNative(buffer, bufferSize, ptr, len);
    long endTime = System.nanoTime();
    
    this.stats.getCompressedRaw().addAndGet(len);
    this.stats.getCompressed().addAndGet(compressedSize);
    this.stats.getCompressionTime().addAndGet(endTime - startTime);
    // update statistics
    return compressedSize;
  }
  
  private ZstdCompressCtx getCompressContext(int dictId) {
    // compression context using current dictionary id
    HashMap<Integer, ZstdCompressCtx> ctxMap = compContextMap.get().get(this.cacheName);
    // This is thread local reference
    if (ctxMap == null) {
      ctxMap = new HashMap<Integer, ZstdCompressCtx>();
      Map<Integer, byte[]> dictMap = dictCacheMap.get(this.cacheName);
      if (dictMap != null) {
        for (Map.Entry<Integer, byte[]> e: dictMap.entrySet()) {
          ZstdDictCompress dictCompress = new ZstdDictCompress(e.getValue(), this.compLevel);
          ZstdCompressCtx compContext = new ZstdCompressCtx();
          compContext.loadDict(dictCompress);
          compContext.setLevel(this.compLevel);
          ctxMap.put(e.getKey(), compContext);
        }
      } else {
        dictCacheMap.putIfAbsent(cacheName, new HashMap<Integer, byte[]>());
      }
      compContextMap.get().put(this.cacheName, ctxMap);
      // Initialize dictionary id = 0 (no dictionary)
      initCompContext(0, null);
    }
    // Now check the current level
    ZstdCompressCtx currentCtxt = ctxMap.get(dictId);
    if (currentCtxt == null) {
      Map<Integer, byte[]> dictMap = dictCacheMap.get(this.cacheName);
      byte[] dict = dictMap.get(dictId);
      // SHOULD NOT BE NULL
      ZstdDictCompress dictCompress = new ZstdDictCompress(dict, this.compLevel);
      currentCtxt = new ZstdCompressCtx();
      currentCtxt.loadDict(dictCompress);
      currentCtxt.setLevel(this.compLevel);
      ctxMap.put(dictId, currentCtxt);
    }
    return currentCtxt;
  }
  
  @Override
  public int getCurrentDictionaryVersion() {
    return this.currentDictVersion;
  }

  private ZstdDecompressCtx getDecompressContext(int dictId) {
    // compression context using current dictionary id
    HashMap<Integer, ZstdDecompressCtx> ctxMap = decompContextMap.get().get(this.cacheName);
    // This is thread local reference
    if (ctxMap == null) {
      ctxMap = new HashMap<Integer, ZstdDecompressCtx>();
      Map<Integer, byte[]> dictMap = dictCacheMap.get(this.cacheName);
      if (dictMap != null) {
        for (Map.Entry<Integer, byte[]> e: dictMap.entrySet()) {
          ZstdDictDecompress dictDecompress = new ZstdDictDecompress(e.getValue());
          ZstdDecompressCtx decompContext = new ZstdDecompressCtx();
          decompContext.loadDict(dictDecompress);
          ctxMap.put(e.getKey(), decompContext);
        }
      } else {
        dictCacheMap.putIfAbsent(cacheName, new HashMap<Integer, byte[]>());
      }
      decompContextMap.get().put(this.cacheName, ctxMap);
      // Initialize dictionary id = 0 (no dictionary)
      initDecompContext(0, null);
    }
    // Now check the current level
    ZstdDecompressCtx currentCtxt = ctxMap.get(dictId);
    if (currentCtxt == null) {
      Map<Integer, byte[]> dictMap = dictCacheMap.get(this.cacheName);
      byte[] dict = dictMap.get(dictId);
      if (dict == null) {
        return null; // dictionary not found
      }
      // SHOULD NOT BE NULL
      ZstdDictDecompress dictCompress = new ZstdDictDecompress(dict);
      currentCtxt = new ZstdDecompressCtx();
      currentCtxt.loadDict(dictCompress);
      ctxMap.put(this.currentDictVersion, currentCtxt);
    }
    return currentCtxt;
  }
  
  static int counter = 0;
  
  @Override
  public int decompress(long ptr, int size, byte[] buffer, int dictId) {
    
    long startTime = System.nanoTime();
    ZstdDecompressCtx currentCtxt = getDecompressContext(dictId);
    if (currentCtxt == null) {
      return 0;
    }
    int decompressedSize = 0;
    try {
      decompressedSize= currentCtxt.decompressNativeByteArray(buffer, 0, buffer.length, ptr, size);
    } catch(Throwable t) {
      return 0; // failed
    }
    long endTime = System.nanoTime();
    this.stats.getDecompressedRaw().addAndGet(decompressedSize);
    this.stats.getDecompressed().addAndGet(size);
    this.stats.getDecompressionTime().addAndGet(endTime - startTime);
    return decompressedSize;
  }

  @Override
  public int decompress(byte[] src, int srcOffset, int srcSize, byte[] buffer, int dictId) {
    long startTime = System.nanoTime();
  
    ZstdDecompressCtx currentCtxt = getDecompressContext(dictId);
    if (currentCtxt == null) {
      return 0;
    }
    int decompressedSize = 0;
    try {
      decompressedSize = currentCtxt.decompressByteArray(buffer, 0, buffer.length, src, srcOffset, srcSize);
    } catch (Throwable t) {
      return 0;
    }
    long endTime = System.nanoTime();
    this.stats.getDecompressedRaw().addAndGet(decompressedSize);
    this.stats.getDecompressed().addAndGet(srcSize);
    this.stats.getDecompressionTime().addAndGet(endTime - startTime);
    return decompressedSize;
  }
  
  @Override
  public int decompress(long ptr, int size, long buffer, int bufSize, int dictId) {
    long startTime = System.nanoTime();
    ZstdDecompressCtx currentCtxt = getDecompressContext(dictId);
    if (currentCtxt == null) {
      return 0;
    }
    int decompressedSize = 0;
    try {
      decompressedSize = currentCtxt.decompressNativeNative(buffer, bufSize, ptr, size);
    } catch (Throwable t) {
      return 0;
    }
    long endTime = System.nanoTime();
    this.stats.getDecompressedRaw().addAndGet(decompressedSize);
    this.stats.getDecompressed().addAndGet(size);
    this.stats.getDecompressionTime().addAndGet(endTime - startTime);
    return decompressedSize;
  }
  
  @Override
  public void init(String cacheName) throws IOException {
    CacheConfig config = CacheConfig.getInstance();
    this.cacheName = cacheName;
    this.dictSize = config.getCacheCompressionDictionarySize(cacheName);
    this.compLevel = config.getCacheCompressionLevel(cacheName);
    this.dictionaryEnabled = config.isCacheCompressionDictionaryEnabled(cacheName);
    this.trainingAsync = config.isCacheCompressionDictionaryTrainingAsync(cacheName);
    String dictDir = config.getCacheDictionaryDir(cacheName);
    File dir = new File(dictDir);
    this.stats = new Stats(compLevel, dictSize, Type.ZSTD);
    if (!dir.exists()) {
      boolean result = dir.mkdirs();
      // nothing to load
      if (!result) {
        throw new IOException(String.format("Failed to create dictionary directory: %s", dictDir));
      }
      return;
    }
    loadDictionaries(dir);
  }

  private byte[] getBuffer(int required) {
    byte[] buf = buffers.get();
    //FIXME: this is not safe
    if (buf == null || buf.length < 2 * required) {
      buf = new byte[2 * required];
      buffers.set(buf);
    }
    return buf;
  }
  
  private int getIdFromName(String name) {
    String strId = name.split("\\.")[1];
    int id = Integer.parseInt(strId);
    return id;
  }
  
  private void loadDictionaries(File dir) throws IOException {
    int maxId = 0;
    HashMap<Integer, byte[]> map = 
        new HashMap<Integer, byte[]>();
    dictCacheMap.put(cacheName,  map);
    File[] list = dir.listFiles();
    for (File f: list) {
        byte[] data = Files.readAllBytes(Path.of(f.toURI()));
        String name = f.getName();
        int id = getIdFromName(name);
        if (id > maxId) {
          maxId = id;
        }
        map.put(id, data);
    }
    this.currentDictVersion = maxId;   
  }

  private void saveDictionary(int id, byte[] data) throws IOException {
    CacheConfig config = CacheConfig.getInstance();
    String dictDir = config.getCacheDictionaryDir(cacheName);
    File dir = new File(dictDir);
    String name = makeDictFileName(id);
    File dictFile = new File(dir, name);
    FileOutputStream fos = new FileOutputStream(dictFile);
    fos.write(data);
    fos.close();
  }
  
  private String makeDictFileName(int id) {
    return "dict." + id;
  }
  
  private void initCompContext(int id, byte[] dict) {
    ZstdCompressCtx compContext = new ZstdCompressCtx();
    compContext.setLevel(this.compLevel);
    HashMap<Integer, ZstdCompressCtx> ctxMap = compContextMap.get().get(this.cacheName);
    if (ctxMap == null) {
      ctxMap = new HashMap<Integer, ZstdCompressCtx> ();
      compContextMap.get().put(this.cacheName, ctxMap);
    }
    if (dict == null) {
      id = 0;
    } else {
      ZstdDictCompress dictCompress = new ZstdDictCompress(dict, this.compLevel);
      compContext.loadDict(dictCompress);
    }
    ctxMap.put(id, compContext);
  }
  
  private void initDecompContext(int id, byte[] dict) {
    ZstdDecompressCtx decompContext = new ZstdDecompressCtx();
    HashMap<Integer, ZstdDecompressCtx> ctxMap = decompContextMap.get().get(this.cacheName);
    if (ctxMap == null) {
      ctxMap = new HashMap<Integer, ZstdDecompressCtx> ();
      decompContextMap.get().put(this.cacheName, ctxMap);
    }
    if (dict == null) {
      id = 0;
    } else {
      ZstdDictDecompress dictCompress = new ZstdDictDecompress(dict);
      decompContext.loadDict(dictCompress);
    }
    ctxMap.put(id, decompContext);
  }
  
  @Override
  public void save(OutputStream os) throws IOException {
    stats.save(os);
  }

  @Override
  public void load(InputStream is) throws IOException {
    stats.load(is);
  }

  private synchronized void startTraining() {
    if (this.trainingInProgress) return;
    LOG.debug("Start training");
    this.trainingDataSize = new AtomicInteger();
    this.trainingData = new ConcurrentLinkedQueue<Long>();
    this.trainingInProgress = true;
  }

  @Override
  public synchronized void addTrainingData(byte[]... data) {
    if (!trainingInProgress || finalizingTraining.get()) {
      return;
    }
    for(byte[] b: data) {
      long ptr = UnsafeAccess.malloc(b.length + Utils.SIZEOF_INT);
      UnsafeAccess.copy(b, 0, ptr + Utils.SIZEOF_INT, b.length);
      UnsafeAccess.putInt(ptr, b.length);
      this.trainingData.add(ptr);
      this.trainingDataSize.addAndGet(b.length);
      checkFinishTraining();
    }
  }

  @Override
  public synchronized void addTrainingData(byte[] data, int off, int len) {
    if (!trainingInProgress || finalizingTraining.get()) {
      return;
    }
    long ptr = UnsafeAccess.malloc(len + Utils.SIZEOF_INT);
    UnsafeAccess.copy(data, 0, ptr + Utils.SIZEOF_INT, len);
    UnsafeAccess.putInt(ptr, len);
    this.trainingData.add(ptr);
    this.trainingDataSize.addAndGet(len);
    checkFinishTraining();
  }

  @Override
  public synchronized void addTrainingData(long ptr, int size) {
    if (!trainingInProgress || finalizingTraining.get()) {
      return;
    }
    long $ptr = UnsafeAccess.malloc(size + Utils.SIZEOF_INT);
    UnsafeAccess.copy(ptr, $ptr + Utils.SIZEOF_INT, size);
    UnsafeAccess.putInt($ptr,  size);
    this.trainingData.add($ptr);
    this.trainingDataSize.addAndGet(size);
    checkFinishTraining();
  }
  
  @Override
  public boolean isTrainingRequired() {
    if (!this.dictionaryEnabled) {
      return false;
    }
    boolean required = this.currentDictVersion == 0;
    if (required && !this.trainingInProgress) {
      startTraining();
    }
    return required;
  }

  @Override
  public int getRecommendedTrainingDataSize() {
    return 100 * this.dictSize;
  }
  
  private void checkFinishTraining() {
    if (this.dictionaryEnabled && this.trainingDataSize.get() >= getRecommendedTrainingDataSize()) {
      finishTraining();
    }
  }
  
  @SuppressWarnings("unused")
  private void finishTraining() {
    if (! finalizingTraining.compareAndSet(false, true)) {
      return;
    }

    Runnable r = () -> {
      byte[] dict;
      ZstdDictTrainer trainer = new ZstdDictTrainer(this.trainingDataSize.get(), this.dictSize);
      LOG.debug("Start training");
      for (Long ptr: this.trainingData) {
        int size = UnsafeAccess.toInt(ptr);
        byte[] data = new byte[size];
        UnsafeAccess.copy(ptr + Utils.SIZEOF_INT, data, 0, size);
        trainer.addSample(data);
      }
      long start = System.currentTimeMillis();
      dict = trainer.trainSamples();
      long end = System.currentTimeMillis();
      Map<Integer, byte[]> dictMap = dictCacheMap.get(this.cacheName);
      dictMap.put(this.currentDictVersion + 1, dict);
      this.currentDictVersion++;
      // Deallocate resources
      this.trainingDataSize.set(0);
      while(!this.trainingData.isEmpty()) {
        long ptr = this.trainingData.poll();
        UnsafeAccess.free(ptr);
      }
      this.trainingInProgress = false;
      this.finalizingTraining.set(false);
      try {
        saveDictionary(this.currentDictVersion, dict);
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
      LOG.debug("Finished training in () ms", System.currentTimeMillis() - start);
    };
    if (this.trainingAsync) {
      // Run training session
      new Thread(r).start();
    } else {
      r.run();
    }
  }

  @Override
  public Stats getStats() {
    return this.stats;
  }

}
