package com.carrot.cache.compression.zstd;

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
import java.util.concurrent.atomic.AtomicInteger;

import com.carrot.cache.compression.CompressionCodec;
import com.carrot.cache.util.CarrotConfig;
import com.carrot.cache.util.UnsafeAccess;
import com.carrot.cache.util.Utils;
import com.github.luben.zstd.ZstdCompressCtx;
import com.github.luben.zstd.ZstdDecompressCtx;
import com.github.luben.zstd.ZstdDictCompress;
import com.github.luben.zstd.ZstdDictDecompress;
import com.github.luben.zstd.ZstdDictTrainer;
/**
 * 
 * This class is a singleton for each cache instance, with each cache having 
 * its own codec. It makes extensive use of thread-local storage objects and, 
 * as a result, necessitates a fixed thread pool within the application.
 * Invariant: TLS is enabled, see CacheConfig::isCacheTLSSupported
 *
 */
public class ZstdCompressionCodec implements CompressionCodec {
  
  private static int INIT_BUFFER_SIZE = 1 << 16; 
  
  static {
    
    INIT_BUFFER_SIZE = CarrotConfig.getInstance().getCacheTLSInitialBufferSize(null);
    
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
  private volatile boolean finalizingTraining = false;
  
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

    if (compressedSize > len) {
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
    int decompressedSize = currentCtxt.decompressNativeByteArray(buffer, 0, buffer.length, ptr, size);
    long endTime = System.nanoTime();
    //*DEBUG*/ System.out.println("counter=" + (counter ++) + " decomp time=" + (endTime - startTime) + " decomp size=" + decompressedSize) ;
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
    int decompressedSize = currentCtxt.decompressByteArray(buffer, 0, buffer.length, src, srcOffset, srcSize);
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
    int decompressedSize = currentCtxt.decompressNativeNative(buffer, bufSize, ptr, size);
    long endTime = System.nanoTime();
    this.stats.getDecompressedRaw().addAndGet(decompressedSize);
    this.stats.getDecompressed().addAndGet(size);
    this.stats.getDecompressionTime().addAndGet(endTime - startTime);
    return decompressedSize;
  }
  
  @Override
  public void init(String cacheName) throws IOException {
    CarrotConfig config = CarrotConfig.getInstance();
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
    CarrotConfig config = CarrotConfig.getInstance();
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
    /*DEBUG*/ System.out.println("START TRAINING");
    if (this.trainingInProgress) return;
    this.trainingDataSize = new AtomicInteger();
    this.trainingData = new ConcurrentLinkedQueue<Long>();
    this.trainingInProgress = true;
  }

  @Override
  public synchronized void addTrainingData(byte[]... data) {
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
    long ptr = UnsafeAccess.malloc(len + Utils.SIZEOF_INT);
    UnsafeAccess.copy(data, 0, ptr + Utils.SIZEOF_INT, len);
    UnsafeAccess.putInt(ptr, len);
    this.trainingData.add(ptr);
    this.trainingDataSize.addAndGet(len);
    checkFinishTraining();
  }

  @Override
  public synchronized void addTrainingData(long ptr, int size) {
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
    if (this.finalizingTraining) return;
    this.finalizingTraining = true;
    /*DEBUG*/ System.out.println("FINISH TRAINING");

    Runnable r = () -> {
      byte[] dict;
      ZstdDictTrainer trainer = new ZstdDictTrainer(this.trainingDataSize.get(), this.dictSize);
      
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
      this.trainingDataSize = null;
      this.trainingData.stream().forEach(x -> UnsafeAccess.free(x));
      this.trainingData = null;
      this.trainingInProgress = false;
      this.finalizingTraining = false;
      try {
        saveDictionary(this.currentDictVersion, dict);
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    };
    if (this.trainingAsync) {
      // Run training session
      new Thread(r).start();
    } else {
      long start = System.currentTimeMillis();
      r.run();
      long end = System.currentTimeMillis();
      /*DEBUG*/ System.out.println("TIME=" + (end -start)+"ms");
    }
  }

  @Override
  public Stats getStats() {
    return this.stats;
  }

}
