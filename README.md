# Carrot Cache (CC)
The Carrot Cache (CC) project aims to modernize data caching and enable users to build custom caching solutions through pluggable components within the CC framework. CC is a 100% Java solution that extensively utilizes off-heap memory, minimizing the impact on Java garbage collection (GC).

## Features

- **Multiple Modes of Operation**: Supports RAM-only, SSD-only, and hybrid modes (RAM -> SSD).
- **Highly Configurable**: Users can customize cache admission policies (important for SSD), promotion policies (from victim cache back to the parent cache), eviction policies, and throughput controllers. Additional customizable components include memory index formats, internal GC recycling selectors, data writers, and data readers.
- **AI/ML Ready**: Custom cache admission and eviction policies can leverage sophisticated machine learning models tailored to specific workloads.
- **CacheGuard** Protected: Combines a cache admission policy with a scan-resistant cache eviction algorithm, significantly reducing SSD wear and increasing longevity.
- **Low SSD Write Amplification (DWA) and Cache Level Write Amplification (CLWA)**: Controlled via a sustained write throughput setting. Users can set a desired sustained write throughput safe for a particular SSD device, with the system dynamically adjusting parameters to meet this requirement. Estimates for 75% SSD used space DLWA = 1.1, and 100% = 1.8. Even with nearly full SSDs, writing does not incur significant DLWA.
- **Low RAM Overhead for Cached Items**: Overhead ranges from 6-16 bytes per item for both RAM and SSD, including expiration support. The overhead depends on the index format used. Several index formats, both with and without expiration support, are provided out of the box.
- **Low Meta Overhead in RAM**: Example: Managing 1M data items in CC requires less than 1MB of Java heap and less than 10MB of Java off-heap memory for metadata.
- **Multiple Eviction Algorithms**: Available out of the box, including Segmented LRU (default), LRU, and FIFO. Segmented LRU is a scan-resistant algorithm. Eviction policies are pluggable, allowing customers to implement their own.
- **Scalability**: Supports multiple terabytes of storage, up to 256TB, with only 16 bytes of RAM overhead per cached item for disk storage.
- **Efficient Expired Item Eviction**: Designed for applications requiring expiration support.
- **Highly Configurable**: Over 50 configurable parameters.
- **Warm Restart**: Allows cache data to survive a full server reboot. Data saving and loading are very fast, dependent only on available disk I/O throughput (GBs per second).
- **Compression**: CC can compress and decompress both keys and values in real-time using pluggable compression codecs, significantly reducing memory usage. Currently supports Zstd with dictionary.
- **Memcached API Compatible**: Supports all store, retrieval, and miscellaneous commands (text protocol only).

## Features in Development (TBI)
- **Replication for HA**
- **Concurrent Save**: Allows concurrent saving of cache data and metadata on demand, similar to Redis's BGSAVE.
- **AutoConfiguration Mode**: Finds optimal parameters for specific workloads, including cache size, in offline mode.
- **Shadow Mode**: Enables real-time decision-making on optimal cache sizing, important for workloads that vary by time of day.
- **Periodic Cache Backup**
- **Rolling Restart** in Cluster Mode (Add-On TBI)
- **Fast Scale Up/Down** in Cluster Mode (Add-On TBI)

## Building prerequisits

- Java 11+ (JVM supported versions are 11-17, no support for 21+ yet)
- Maven 3.x
- Git client

To build:
```
git clone https://github.com/VladRodionov/onecache-core.git
cd onecache-core
mvn install -DskipTests
```

To run unit tests:
```mvn surefire:test```

## How to use

### Create in-memory cache

```
import com.carrotdata.cache.*;

 protected  Cache createInMemoryCache(String cacheName) throws IOException{
    // Data directory is needed even for in-memory cache, this is where 
    // data from memory can be saved to
    Path dataDirPath = Files.createTempDirectory(null);
    Path String dataDir = dataDirPath.toFile().getAbsolutePath();
    // Snapshot directory contains saved meta information
    snapshotDirPath = Files.createTempDirectory(null);
    String snapshotDir = snapshotDirPath.toFile().getAbsolutePath();
    
    Builder builder = new Builder(cacheName);
    
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
import com.carrotdata.cache.*;
 protected  Cache createDiskCache(String cacheName) throws IOException{
    
    Path dataDirPath = Files.createTempDirectory(null);
    String dataDir = dataDirPath.toFile().getAbsolutePath();
    // Snapshot directory contains saved meta information
    Path snapshotDirPath = Files.createTempDirectory(null);
    String snapshotDir = snapshotDirPath.toFile().getAbsolutePath();
    
    Builder builder = new Builder(cacheName);
    
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

At minimum you need to provide the maximum cache size, the data segment size (if you do not like default - 4MB), the data directory and the snapshot directory names, all other parameters wiil be default ones. It is a good idea to read ```com.carrotdata.cache.util.CacheConfig``` class, which contains all configuration parameters with annotations and default values.

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

... because the code has direct access to the native memory via ```sun.misc.Unsafe``` class and this is the pre beta-version.
To debug possible core dumps you need to activate the debug mode in the memory allocator

```
  UnsafeAccess.setMallocDebugEnabled(true);
```

This will prevents core dumps and will throw exception on memory corruption, but wiil runs significantly slower, use this only for small scale tests. 

To track a potential memory leaks, for example an allocations of a size 64, you need additionally to enable the allocations stack track 
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
Carrot Data, Inc.

You can reach me easily at
vlad@trycarrots.io








