# Carrot cache
In-Out-Process Java cache (L1/L2 off-heap, scalable, ZeroGC) with full SSD support

## Features

- Hybrid mode (RAM -> SSD) is supported. Actually, multi - layer caching is supported and is not limited only to two layers. 
- Configurable (customizable) admission (for SSD) and eviction policies.
- Low DWA (device write amplification) which can be controlled via desired sustained write througput settings. One can set desired sustained write throughput which is safe for a particular SSD device and the system will adjust its parameters accordingly to meet this requirement.
- Very low cached item overhead: its 2 bytes only in a compact mode (RAM with expiration) and 8 bytes in a normal mode (for both RAM and SSD and with expiration support).
- Very low meta overhead in RAM.
- Default eviction policy - Segmented LRU is scan resistent.  
- Scalable in both RAM and disk. Multiple TBs of storage is supported (up to 16TB in a optimized mode with 8 bytes per cached item in RAM overhead).


