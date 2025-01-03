
# Carrot Cache (CC)

[Documentation](https://github.com/carrotdata/carrot-cache/wiki/Overview) 🥕🥕 [Forum & Discussions](https://github.com/carrotdata/carrot-cache/discussions)

The Carrot Cache (CC) project aims to modernize data caching and enable users to build custom caching solutions through pluggable components within the CC framework. CC is a 100% Java solution that extensively utilizes off-heap memory, minimizing the impact on Java garbage collection (GC).

## Features

- **Multiple Modes of Operation**: Supports RAM-only, SSD-only, hybrid modes (RAM -> SSD), and tandem mode (RAM -> compressed RAM).
- **Highly Configurable**: Users can customize cache admission policies (important for SSD), promotion policies (from victim cache back to the parent cache), eviction policies, and throughput controllers. Additional customizable components include memory index formats, internal GC recycling selectors, data writers, and data readers.
- **AI/ML Ready**: Custom cache admission and eviction policies can leverage sophisticated machine learning models tailored to specific workloads.
- **CacheGuard Protected**: Combines a cache admission policy with a scan-resistant cache eviction algorithm, significantly reducing SSD wear and increasing longevity.
- **Low SSD Write Amplification (DWA) and Cache Level Write Amplification (CLWA)**: Controlled via a sustained write throughput setting. Users can set a desired sustained write throughput safe for a particular SSD device, with the system dynamically adjusting parameters to meet this requirement. Estimates for 75% SSD used space DLWA = 1.1, and 100% = 1.8. Even with nearly full SSDs, writing does not incur significant DLWA.
- **Low RAM Overhead for Cached Items**: Overhead ranges from 6-20 bytes per item for both RAM and SSD, including expiration support. The overhead depends on the index format used. Several index formats, both with and without expiration support, are provided out of the box.
- **Low Meta Overhead in RAM**: Example: Managing 1M data items in CC requires less than 1MB of Java heap and less than 10MB of Java off-heap memory for metadata.
- **Multiple Eviction Algorithms**: Available out of the box, including Segmented LRU (default), LRU, and FIFO. Segmented LRU is a scan-resistant algorithm. Eviction policies are pluggable, allowing customers to implement their own.
- **Scalability**: Supports multiple terabytes of storage, up to 256TB, with only 11 bytes of RAM overhead per cached item for disk storage.
- **Efficient Expired Item Eviction**: Designed for applications requiring expiration support.
- **Highly Configurable**: Over 50 configurable parameters.
- **Warm Restart**: Allows cache data to survive a full server reboot. Data saving and loading are very fast, dependent only on available disk I/O throughput (GBs per second).
- **Compression**: CC can compress and decompress both keys and values in real-time using pluggable compression codecs, significantly reducing memory usage. Currently supports Zstd with dictionary.
- **Memcached API Compatible**: Supports all store, retrieval, and miscellaneous commands (text protocol only).

## Download

You can download ```Carrot Cache``` Jar - file with dependencies from the latest [Releases](https://github.com/carrotdata/carrot-cache/releases) page. Supported platforms:
- MacOS Sonoma (x86_64, aarch64)
- Linux (amd64, aarch64, glibc 2.31+)

If your platform is not supported you can build binaries from the source code.

## Building Prerequisites

- Java 11+
- Maven 3.x
- Git client

## How to Build

Carrot Cache binaries already support the following platforms: macOS (x86_64, aarch64) and Linux (x86_64, aarch64). If your platform is not supported, you can build the binary package locally.

### Build and Install Locally `zstd-jni` Package

You will find instructions here: [zstd-jni package](https://github.com/carrotdata/zstd-jni)

### Build Carrot Cache

```bash
git clone https://github.com/carrotdata/carrot-cache.git
cd carrot-cache
mvn install -DskipTests
```

After the build is complete, the binaries (JAR file) will be in the `target` directory.

To run unit tests:

```bash
mvn surefire:test
```

## How to Use

### Create In-Memory Cache

```java
import com.carrotdata.cache.*;

protected Cache createInMemoryCache(String cacheName) throws IOException {
    // Data directory is needed even for in-memory cache; this is where 
    // data from memory can be saved to
    Path dataDirPath = Files.createTempDirectory(null);
    String dataDir = dataDirPath.toFile().getAbsolutePath();
    
    Builder builder = new Builder(cacheName);
    
    builder
      .withCacheDataSegmentSize(16_777_216)  // 16MB
      .withCacheMaximumSize(8_589_934_592)  // 8GB 
      .withRecyclingSelector(MinAliveRecyclingSelector.class.getName())
      .withDataDir(dataDir)
      .withMainQueueIndexFormat(SubCompactBaseWithExpireIndexFormat.class.getName())  // This index format supports cache expiration 
      .withAdmissionController(ExpirationAwareAdmissionController.class.getName());  // This controller does some smart things :)
    return builder.buildMemoryCache();
}
```

### Create Disk-Based Cache

```java
import com.carrotdata.cache.*;

protected Cache createDiskCache(String cacheName) throws IOException {
    Path dataDirPath = Files.createTempDirectory(null);
    String dataDir = dataDirPath.toFile().getAbsolutePath();
    
    Builder builder = new Builder(cacheName);
    
    builder
      .withCacheDataSegmentSize(67_108_864)  // 64MB
      .withCacheMaximumSize(687_194_767_360)  // 640GB 
      .withRecyclingSelector(MinAliveRecyclingSelector.class.getName())  // Specify recycling selector type
      .withDataDir(dataDir)
      .withMainQueueIndexFormat(SubCompactBaseWithExpireIndexFormat.class.getName())  // This index format supports cache expiration 
      .withAdmissionController(ExpirationAwareAdmissionController.class.getName());  // Specify cache admission controller
    return builder.buildDiskCache();
}
```

### Create Hybrid Cache (RAM -> SSD)

```java
protected Cache createHybridCache(String ramCacheName, String diskCacheName) throws IOException {
    Cache ramCache = createInMemoryCache(ramCacheName);
    Cache diskCache = createDiskCache(diskCacheName);
    ramCache.setVictimCache(diskCache);
    return ramCache;
}
```

### Create Tandem Cache (RAM -> Compressed RAM)

```java
import com.carrotdata.cache.*;

protected Cache createInMemoryCompressedCache(String cacheName) throws IOException {
    // Data directory is needed even for in-memory cache; this is where 
    // data from memory can be saved to
    Path dataDirPath = Files.createTempDirectory(null);
    String dataDir = dataDirPath.toFile().getAbsolutePath();
    
    Builder builder = new Builder(cacheName);
    
    builder
      .withCacheDataSegmentSize(16_777_216)  // 16MB
      .withCacheMaximumSize(34_359_738_368)  // 32GB 
      .withCacheCompressionEnabled(true)  // Enable compression
      .withRecyclingSelector(MinAliveRecyclingSelector.class.getName())
      .withDataDir(dataDir)
      .withMainQueueIndexFormat(SubCompactBaseWithExpireIndexFormat.class.getName())  // This index format supports cache expiration 
      .withAdmissionController(ExpirationAwareAdmissionController.class.getName());  // This controller does some smart things :)
    return builder.buildMemoryCache();
}

protected Cache createTandemCache(String ramCacheName, String ramCompressedCacheName) throws IOException {
    Cache ramCache = createInMemoryCache(ramCacheName);
    Cache compCache = createInMemoryCompressedCache(ramCompressedCacheName);
    ramCache.setVictimCache(compCache);
    return ramCache;
}
```

### Cache Configuration

At a minimum, you need to provide the maximum cache size, the data segment size (if you do not like the default - 4MB), and the data directory. All other parameters will use their default values. It is a good idea to read the `com.carrotdata.cache.util.CacheConfig` class, which contains all configuration parameters with annotations and default values.

### Simple Code Example

```java
Cache cache = createInMemoryCache("ram1");

byte[] key1 = "key1".getBytes();
byte[] value1 = "value1".getBytes();

// Put key-value without expiration time
cache.put(key1, value1, 0);

byte[] key2 = "key2".getBytes();
byte[] value2 = "value2".getBytes();

// Put key-value with expiration time of 1 minute
cache.put(key2, value2, System.currentTimeMillis() + 60 * 1000);

byte[] buffer = new byte[value2.length];

int size = cache.get(key2, 0, key2.length, buffer, 0);
String result = new String(buffer, 0, size);



System.out.printf("Value for key %s is %s", key2, result);
```

### Important - Core Dumps Are Possible

... because the code has direct access to the native memory via the `sun.misc.Unsafe` class, and this is a beta version.

To debug possible core dumps, you need to activate the debug mode in the memory allocator:

```java
UnsafeAccess.setMallocDebugEnabled(true);
```

This will prevent core dumps and will throw an exception on memory corruption, but it will run significantly slower. Use this only for small-scale tests.

To track potential memory leaks, such as allocations of size 64, you need to additionally enable the allocations stack trace monitoring:

```java
UnsafeAccess.setMallocDebugStackTraceEnabled(true);
UnsafeAccess.setStackTraceRecordingFilter(x -> x == 64);
UnsafeAccess.setStackTraceRecordingLimit(100);  // Record first 100 allocations only
```

Bear in mind that allocations tracking is expensive and slows down the application by a factor of 20-30. To check the code for memory leaks, you need to enable debug mode (as described above) and use the provided API to print the memory allocator statistics:

```java
UnsafeAccess.mallocStats.printStats();
```


 
Contact: vlad@trycarrots.io

Copyright (c) Carrot Data, Inc. 2024

