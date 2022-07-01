# CHLib (CacheHouse Library)
The project goal is to modernize data caching and to allow users to build custom caching solutions by providing pluggable components to the CHLib framework. CHLib is 100% Java solution, which extensively utilizes java off heap memory and virually has no impact on Java GC.

## Features

- Multiple modes: Only RAM, Only SSD, Hybrid mode (RAM -> SSD) are supported. 
- Highly configurable (customizable):  Cache Admission policy (important for SSD) and eviction policy are two major policies which can be replaced by users. Other customizable components : memory index formats, internal GC recycling selector, data writers/data readers. 
- ML (Machine Learning) ready. Custom cache admission and eviction policies can utilize sophisticated ML models.    
- **CacheGuard (tm)** is the combination of a Cache Admission Policy and scan resistant cache eviction implementaion - significantly reduces SSD wearing.  
- Low SSD DWA (device write amplification) and CLWA (cache level write amplification), which can be controlled via desired sustained write througput settings. One can set desired sustained write throughput which is safe for a particular SSD device and the system will adjust its parameters dynamically to meet this requirement. **Estimates for 75% SSD used space DLWA = 1.1, 100% - 1.8**. So, even if SSD is almost full, writing to it does not incur significant DLWA. For example, in some commercially available products SSD DLWA can be as high as 10 (random writes of data by blocks of 1MB size).
- Very low cached item overhead: its 2 bytes only in a compact mode (RAM with expiration) and 8-20 bytes in a normal mode (for both RAM and SSD and with expiration support).
- Very low meta overhead in RAM. Example: Keeping 1M data items in the CHLib requires less than 1MB of Java heap and less than 10MB of Java offheap memory for meta.
- Several eviction algorithms availble out of box: Segmented LRU (default), LRU, FIFO, 2Q, W-LFU (Window Least Frequently used). Segmented LRU, 2Q, W-LFU are scan resistent algorithms.  
- Scalable in both RAM and disk. Multiple TBs of storage is supported (up to **256TB** in a normal mode with 20 bytes per cached item in RAM overhead).
- Efficient eviction of expired cached items (for appliaction which require eviction support).
- Highly configurable (over 40 parameters). 
- **AutoConfiguration** mode allows to find optimal parameters for a particular workload, incuding cache size (offline mode). 
- **Shadow Mode** allows to make quick decisions real-time on optimal cache sizing. It is important when workload varies by time of day.
- Warm restart and periodic cache backup. 
- Memchached API compatible (Add-On TBI)
- Rolling restart in cluster mode (Add-On TBI)
- Fast scale up - scale down in a cluster mode (Add-On TBI)


TBI - To Be Implemented

Timeline:
07-31-22 - beta1



