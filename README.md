# C2 - Carrot cache
In-Out-Process Java cache (L1/L2 off-heap, scalable, ZeroGC) with full SSD support

## Features

- Hybrid mode (RAM -> SSD) is supported. Actually, multi - layer caching is supported and is not limited only to two layers. 
- Configurable (customizable) admission (for SSD) and eviction policies.
- Low DWA (device write amplification) and CLWA (cache level write amplification), which can be controlled via desired sustained write througput settings. One can set desired sustained write throughput which is safe for a particular SSD device and the system will adjust its parameters dynamically to meet this requirement.
- Very low cached item overhead: its 2 bytes only in a compact mode (RAM with expiration) and 8 bytes in a normal mode (for both RAM and SSD and with expiration support).
- Very low meta overhead in RAM.
- Scan resistent Segmented LRU eviction algorithm.  
- Scalable in both RAM and disk. Multiple TBs of storage is supported (up to 16TB in a optimized mode with 8 bytes per cached item in RAM overhead).
- Efficient eviction of expired cached items.
- Warm restart and periodic cache backup. 
- Memchached API compatible (TBI)
- Rolling restart in cluster mode (TBI)
- Fast scale up - scale down in a cluster mode (TBI)


TBI - To Be Implemented

