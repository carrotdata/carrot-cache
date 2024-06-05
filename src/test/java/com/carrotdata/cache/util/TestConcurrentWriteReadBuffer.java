package com.carrotdata.cache.util;

import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class TestConcurrentWriteReadBuffer {
  private static final Logger LOG = LoggerFactory.getLogger(TestConcurrentWriteReadBuffer.class);

  ConcurrentWriteReadBuffer buffer;
  Long2LongHashMap cache;
  Long2LongHashMap mainStorage;

  int numThreads = 4;
  static int pageSize = 4096;

  int n = 10_000_000;

  int keySize = 32;
  int valSize = 250;

  int evacuated = 0;

  static ThreadLocal<long[]> keysTLS = new ThreadLocal<long[]>();

  static ThreadLocal<long[]> valuesTLS = new ThreadLocal<long[]>();

  static ThreadLocal<Long> memoryBuffer = new ThreadLocal<Long>() {
    @Override
    protected Long initialValue() {
      return UnsafeAccess.malloc(pageSize);
    }
  };

  // ConcurrentHashMap<Long,Long> chmCache = new ConcurrentHashMap<Long, Long>();
  // Map<Long, Long> chmCache = Collections.synchronizedMap(new HashMap<Long, Long>());

  @Before
  public void setUp() {
    buffer = new ConcurrentWriteReadBuffer((x, y, z) -> evacuate(x, y, z), pageSize, numThreads);
    cache = new Long2LongHashMap(numThreads * 100_000, true);
    mainStorage = new Long2LongHashMap(2 * numThreads * n, true);
    mainStorage.setDeallocator(x -> deallocate(x));
  }

  @After
  public void tearDown() {
    // buffer.dispose();
    // cache.dispose();
    // mainStorage.dispose();
  }

  @Test
  public void testPutGet() throws InterruptedException {
    Runnable r = () -> putGet();

    Thread[] threads = new Thread[numThreads];
    for (int i = 0; i < numThreads; i++) {
      threads[i] = new Thread(r);
      threads[i].start();
    }

    for (int i = 0; i < numThreads; i++) {
      threads[i].join();
    }
  }

  private void putGet() {
    generateData();
    loadData();
    verifyData();
    disposeThreadLocal();

  }

  synchronized void printKeys(long[] keys) {
    LOG.info("{}", Thread.currentThread());
    for (int i = 0; i < n; i++) {
      LOG.info("{}", keys[i]);
    }
    LOG.info("");
  }

  private void deallocate(long v) {
    UnsafeAccess.free(v);
  }

  private void disposeThreadLocal() {
    long[] keys = keysTLS.get();
    if (keys != null) {
      Arrays.stream(keys).forEach(x -> UnsafeAccess.free(x));
    }
    keysTLS.set(null);
    long[] values = valuesTLS.get();
    if (values != null) {
      Arrays.stream(values).forEach(x -> UnsafeAccess.free(x));
    }
    long ptr = memoryBuffer.get();
    if (ptr != 0) {
      UnsafeAccess.free(ptr);
    }
  }

  private void generateData() {
    long start = System.currentTimeMillis();
    long[] keys = new long[n];
    long[] values = new long[n];
    for (int i = 0; i < n; i++) {
      keys[i] = TestUtils.randomMemory(keySize);
      values[i] = TestUtils.randomMemory(valSize);
    }
    keysTLS.set(keys);
    valuesTLS.set(values);
    long end = System.currentTimeMillis();
    LOG.info("{} generated {} in {}ms", Thread.currentThread().getName(), n, end - start);

  }

  AtomicLong totalEvacuatedTime = new AtomicLong();
  AtomicInteger failedCHM = new AtomicInteger();
  AtomicInteger failed = new AtomicInteger();

  private void evacuate(int slot, long ptr, int size) {
    int off = 0;
    long start = System.nanoTime();

    while (off < size) {
      boolean lastRecord; // last record in a slot is not in cache yet
      int keySize = Utils.readUVInt(ptr + off);
      int keySizeSize = Utils.sizeUVInt(keySize);
      int valueSize = Utils.readUVInt(ptr + off + keySizeSize);
      int valueSizeSize = Utils.sizeUVInt(valueSize);
      int requiredSize = valueSize + keySize + keySizeSize + valueSizeSize + Utils.SIZEOF_LONG;
      lastRecord = off + requiredSize == size;
      long key = hash(ptr + off + keySizeSize + valueSizeSize, keySize);

      long $ptr = UnsafeAccess.malloc(requiredSize);
      UnsafeAccess.copy(ptr + off, $ptr, requiredSize);
      long val = mainStorage.put(key, $ptr);
      if (val > 0) {
        boolean b = equalsKeys(ptr + off + keySizeSize + valueSizeSize, keySize, val);
        LOG.info("{} Put key hash={} found hash={}  value={} slot={} off={} keys eq=%b",
          Thread.currentThread().getName(), key, keyHash(val), val, slot, off, b);

      }
      if (!lastRecord) {
        long res = cache.delete(key);
        if (res == 0) {
          failed.incrementAndGet();
        }
      }
      off += requiredSize;
    }
    totalEvacuatedTime.addAndGet(System.nanoTime() - start);

  }

  private void loadData() {
    long[] keys = keysTLS.get();
    long[] values = valuesTLS.get();
    long start = System.currentTimeMillis();
    for (int i = 0; i < n; i++) {
      long keyPtr = keys[i];
      long valPtr = values[i];
      long v = buffer.append(keyPtr, keySize, valPtr, valSize, 0);
      long key = hash(keyPtr, keySize);
      // chmCache.put(key, v);
      long ret = cache.put(key, v);

      assertTrue(ret == 0);

    }
    long end = System.currentTimeMillis();
    LOG.info("{} loaded {} in {}ms evacuation time={}ms, failedCHM reads={} failed reads={}",
      Thread.currentThread().getName(), n, end - start, totalEvacuatedTime.get() / 1_000_000,
      failedCHM.get(), failed.get());

  }

  private void verifyData() {
    long[] keys = keysTLS.get();
    long[] values = valuesTLS.get();
    long start = System.currentTimeMillis();
    for (int i = 0; i < n; i++) {
      long keyPtr = keys[i];
      long valPtr = values[i];
      long ptr = getKeyValueBlob(keyPtr, keySize);
      if (ptr <= 0) {
        long found = cache.search(hash(keyPtr, keySize));
        if (found > 0) {
          LOG.error("{} i={} verify failed, hash={} run={} slot={} off={}", Thread.currentThread(),
            i, hash(keyPtr, keySize), ConcurrentWriteReadBuffer.getWriteRun(found),
            ConcurrentWriteReadBuffer.getSlotNumber(found),
            ConcurrentWriteReadBuffer.getOffset(found));
        } else {
          LOG.error("{} i={} verify failed, hash={} founf={}", Thread.currentThread(), i,
            hash(keyPtr, keySize), found);
        }
        continue;
      }
      boolean result = equalsKeys(keyPtr, keySize, ptr);
      if (!result) {
        LOG.error("Expected key hash={} size={}", hash(keyPtr, keySize), keySize);
        int kSize = Utils.readUVInt(ptr);
        int kSizeSize = Utils.sizeUVInt(kSize);
        ptr += kSizeSize;
        int vSize = Utils.readUVInt(ptr);
        int vSizeSize = Utils.sizeUVInt(vSize);
        ptr += vSizeSize;
        LOG.error("Found key hash={} size={}", hash(ptr, kSize), kSize);

      }
      assertTrue(result);
      assertTrue(equalsValues(valPtr, valSize, ptr));
    }
    long end = System.currentTimeMillis();
    LOG.info("{} verified {} in {}ms", Thread.currentThread().getName(), n, end - start);
  }

  private long getKeyValueBlob(long keyPtr, int keySize) {
    long key = hash(keyPtr, keySize);
    long value = mainStorage.get(key);
    if (value != 0) {
      if (!equalsKeys(keyPtr, keySize, value)) {
        LOG.error("Failed read from mainStorage");
      }
      return value;
    }
    long index = cache.get(key);
    value = index > 0 ? buffer.getAddressByIndex(index) : 0;
    if (value > 0) {
      if (!equalsKeys(keyPtr, keySize, value)) {
        LOG.error("Failed read from cache");
      }
      return value;
    }
    long start = System.nanoTime();
    long timeout = 10_000_000;
    if (value <= 0) {
      // wait until we get result from main
      while ((value = mainStorage.get(key)) <= 0) {
        if (System.nanoTime() - start > timeout) {
          break;
        }
      }
      // LOG.error("Waited for {} ns, value={}", System.nanoTime() - start, value);
    }
    if (value > 0) {
      if (!equalsKeys(keyPtr, keySize, value)) {
        LOG.error("Failed read from mainStorage after wait");
      }
      return value;
    }
    return value;
  }

  private boolean equalsKeys(long keyPtr, int keySize, long blob) {
    int kSize = Utils.readUVInt(blob);
    int kSizeSize = Utils.sizeUVInt(kSize);
    blob += kSizeSize;
    int vSize = Utils.readUVInt(blob);
    int vSizeSize = Utils.sizeUVInt(vSize);
    blob += vSizeSize;
    return Utils.compareTo(keyPtr, keySize, blob, kSize) == 0;
  }

  private boolean equalsValues(long valuePtr, int valueSize, long blob) {
    int kSize = Utils.readUVInt(blob);
    int kSizeSize = Utils.sizeUVInt(kSize);
    blob += kSizeSize;
    int vSize = Utils.readUVInt(blob);
    int vSizeSize = Utils.sizeUVInt(vSize);
    blob += vSizeSize;
    return Utils.compareTo(valuePtr, valueSize, blob + kSize, vSize) == 0;
  }

  private long keyHash(long blob) {
    int kSize = Utils.readUVInt(blob);
    int kSizeSize = Utils.sizeUVInt(kSize);
    blob += kSizeSize;
    int vSize = Utils.readUVInt(blob);
    int vSizeSize = Utils.sizeUVInt(vSize);
    blob += vSizeSize;
    return hash(blob, keySize);
  }

  private long hash(long ptr, int size) {
    long h = Utils.hash64(ptr, size, 137);
    return Utils.squirrel3(h);
  }

}
