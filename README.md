# Carrot Cache (C2) Framework
The project goal is to modernize data caching and to allow users to build custom caching solutions by providing pluggable components to the C2 framework. C2 is 100% Java solution, which extensively utilizes java off heap memory and virtually has no impact on Java GC.

## Features

- Multiple modes of operations: Only RAM, Only SSD, Hybrid mode (RAM -> SSD) are supported. 
- Highly configurable (customizable):  Cache Admission policy (important for SSD), eviction policy  and throughput controller are three major policies which can be replaced by users. Other customizable components : memory index formats, internal GC recycling selector, data writers/data readers. 
- ML (Machine Learning) ready. Custom cache admission and eviction policies can utilize sophisticated ML models, trained to a customer specific workloads.   
- **CacheGuard (tm)** protected. It is the combination of a Cache Admission Policy and scan resistant cache eviction algorithm - significantly reduces SSD wearing and increases its longevity.  
- **Low SSD DWA** (device write amplification) and CLWA (cache level write amplification), which can be controlled via desired sustained write througput setting. One can set desired sustained write throughput which is safe for a particular SSD device and the system will adjust its parameters dynamically to meet this requirement. **Estimates for 75% SSD used space DLWA = 1.1, 100% - 1.8**. So, even if SSD is almost full, writing to it does not incur significant DLWA. For example, in some commercially available products SSD DLWA can be as high as 10 (random writes of data by blocks of 1MB size).
- **Very low cached item overhead in RAM: 6-16 bytes for both RAM and SSD and with expiration support**. The overhead depends on a particular index format. Several index formats are provided out of box, both: with and without expiration support.  
- Very low meta overhead in RAM. Example: Keeping 1M data items in the **C2** requires less than 1MB of Java heap and less than 10MB of Java offheap memory for meta.
- Several eviction algorithms available out of the box: Segmented LRU (default), LRU, FIFO. Segmented LRU is scan resistent algorithms. Eviction policy is pluggable and customers can provide their own implementation.  
- **Scalable**. Multiple TBs of storage is supported - up to **256TB** with only 16 bytes per cached item in RAM overhead, per single cache instance.
- Efficient eviction of expired cached items (for appliaction which require eviction support). 
- Highly configurable (over 50 parameters). 
- **Warm restart**. This allows cache data to survive full server's reboot. Saving and loading data is very fast and depends only on disk available I/O throughput (GBs per sec).

Features which are not implemented yet but are being planned (TBI): 

- Concurrent save - will allow to save cache data and meta on demand concurrently with normal cache operation. Similar to BGSAVE in Redis.
- W-LFU (Window least Frequently Used) eviction policy plus some additional ones.
- **AutoConfiguration** mode allows to find optimal parameters for a particular workload, incuding cache size (offline mode). 
- **Shadow Mode** allows to make quick decisions real-time on optimal cache sizing. It is important when workload varies by time of day.
- Periodic cache backup.  
- Memchached API compatible (Add-On TBI)
- Rolling restart in cluster mode (Add-On TBI)
- Fast scale up - scale down in a cluster mode (Add-On TBI)

## Building prerequisits

- Java 11
- Maven 3.x
- Git client

To build:
```
git clone https://github.com/VladRodionov/carrot-cache.git
cd carrot-cache
mvn install -DskipTests
```

To run unit tests:
```mvn surefire:test```

## How to use

### Create in-memory cache

```
 protected  Cache createInMemoryCache(String cacheName) throws IOException{
    // Data directory is needed even for in-memory cache, this is where 
    // data from memory can be saved to
    Path dataDirPath = Files.createTempDirectory(null);
    Path String dataDir = dataDirPath.toFile().getAbsolutePath();
    // Snapshot directory contains saved meta information
    snapshotDirPath = Files.createTempDirectory(null);
    String snapshotDir = snapshotDirPath.toFile().getAbsolutePath();
    
    Cache.Builder builder = new Cache.Builder(cacheName);
    
    builder
      .withCacheDataSegmentSize(19726336); // 16MB
      .withCacheMaximumSize(1024 * 19726336) // 16GB 
      .withScavengerRunInterval(10) // in seconds
      .withCacheEvictionPolicy(LRUEvictionPolicy.class.getName())
      .withRecyclingSelector(MinAliveRecyclingSelector.class.getName())
      .withSnapshotDir(snapshotDir)
      .withDataDir(dataDir)
      .withMainQueueIndexFormat(CompactBaseWithExpireIndexFormat.class.getName()) // This index format supports cache expiration 
      .withAdmissionController(ExpirationAwareAdmissionController.class.getName()); // This controller does some smart things :)
      return builder.buildMemoryCache();
  }
```

### Create disk-based cache

```
 protected  Cache createDiskCache(String cacheName) throws IOException{
    
    Path dataDirPath = Files.createTempDirectory(null);
    String dataDir = dataDirPath.toFile().getAbsolutePath();
    // Snapshot directory contains saved meta information
    Path snapshotDirPath = Files.createTempDirectory(null);
    String snapshotDir = snapshotDirPath.toFile().getAbsolutePath();
    
    Cache.Builder builder = new Cache.Builder(cacheName);
    
    builder
      .withCacheDataSegmentSize(78905344); // 64MB
      .withCacheMaximumSize(10 * 1024 * 78905344) // 640GB 
      .withScavengerRunInterval(10) // in seconds
      .withCacheEvictionPolicy(LRUEvictionPolicy.class.getName()) // Specify eviction policy
      .withRecyclingSelector(MinAliveRecyclingSelector.class.getName()) // Specify recycling selector type
      .withSnapshotDir(snapshotDir)
      .withDataDir(dataDir)
      .withMainQueueIndexFormat(CompactBaseWithExpireIndexFormat.class.getName()); // This index format supports cache expiration 
      .withAdmissionController(ExpirationAwareAdmissionController.class.getName()); // Specify cache admission controller
      return builder.buildDiskCache();
  }
```

### Create hybrid cache (RAM -> SSD)

```
protected Cache createHybridCache(String ramCacheName, String diskCacheName) throws IOException {
  Cache ramCache = createInMemoryCache(ramCacheName);
  Cache diskCache = createDiskCache(diskCacheName);
  ramCache.setVictimCache(diskCache);
  return ramCache;
}
```

### Cache configuration

At minimum you need to provide the maximum cache size, the data segment size (if you do not like default - 4MB), the data directory and the snapshot directory names, all other parameters wiil be default ones. It is a good idea to read ```com.carrot.cache.util.CacheConfig``` class, which contains all configuration parameters with annotations and default values.

### Simple code example

```
Cache cache = createInMemoryCache("ram1");

byte[] key1 = "key1".getBytes();
byte[] value1 = "value1".getBytes();

// Put key - value without expiration time
cache.put(key1, value1, 0);

byte[] key2 = "key2".getBytes();
byte[] value2 = "value2".getBytes();

// Put key - value with expiration time 1 minute
cache.put(key2, value2, System.currentTimeMillis() + 60 * 1000);

byte[] buffer = new byte[value2.length];

int size = cache.get(key2, 0, key2.length, buffer, 0);
String result = new String(buffer, 0, size);

System.out.printf("Value for key %s is %s", key2, result);

```

### Important - core dumps are possible

... because the code has direct access to the native memory via ```sun.misc.Unsafe``` class and this is the alpha-version.
To debug possible core dumps you need to activate the debug mode in the memory allocator

```
  UnsafeAccess.setMallocDebugEnabled(true);
```

This will prevents core dumps and will throw exception on memory corruption. 

To track the potential memory leaks, for example an allocations of a size 64, you need additionally to enable the allocations stack track 
monitoring

```
UnsafeAccess.setMallocDebugStackTraceEnabled(true);
UnsafeAccess.setStackTraceRecordingFilter(x -> x == 64);
UnsafeAccess.setStackTraceRecordingLimit(100); // record first 100 allocations only

```

Bear in mind that allocations tracking is expensive and slows down the appliaction by factor 20-30. To check the code on memory leaks you need to enable the debug mode (described above) and use the provided API to print the memory allocator statistics:

```
UnsafeAccess.mallocStats.printStats();
```

Happy using and testing, folks.

Best regards,
Vladimir Rodionov

You can reach me easily at
vladrodionov@gmail.com








