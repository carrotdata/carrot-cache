# Carrot Cache (C2) Framework
The project goal is to modernize data caching and to allow users to build custom caching solutions by providing pluggable components to the C2 framework. C2 is 100% Java solution, which extensively utilizes java off heap memory and virually has no impact on Java GC.

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
```git clone ```
```mvn install -DskipTests```

To run unit tests:
```mvn surefire:test```







