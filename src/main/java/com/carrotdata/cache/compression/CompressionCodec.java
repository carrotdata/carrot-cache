/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package com.carrotdata.cache.compression;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.concurrent.atomic.AtomicLong;

import com.carrotdata.cache.compression.zstd.ZstdCompressionCodec;
import com.carrotdata.cache.util.Persistent;
import com.carrotdata.cache.util.Utils;

/**
 * Compression codec. Implementations must be thread-safe.
 */
public interface CompressionCodec extends Persistent {

  static enum Type {
    ZSTD {
      @Override
      public CompressionCodec newCodec() {
        return new ZstdCompressionCodec();
      }
    },

    NONE {
      @Override
      public CompressionCodec newCodec() {
        throw new RuntimeException("codec type is undefined");
      }
    };

    public abstract CompressionCodec newCodec();
  }

  static class Stats implements Persistent {

    /**
     * Compressed raw size (total)
     */
    private AtomicLong compressedRaw = new AtomicLong();
    /**
     * Compressed size (total)
     */
    private AtomicLong compressed = new AtomicLong();

    /**
     * Decompressed raw size (total)
     */
    private AtomicLong decompressedRaw = new AtomicLong();

    /**
     * Decompressed size
     */
    private AtomicLong decompressed = new AtomicLong();
    /**
     * Compression time (nanoseconds)
     */
    private AtomicLong compressionTime = new AtomicLong();

    /**
     * Decompression time (nanoseconds)
     */
    private AtomicLong decompressionTime = new AtomicLong();

    /**
     * Compression level
     */
    private int compressionLevel;

    /**
     * Dictionary size
     */
    private int dictSize;

    /**
     * Codec type
     */
    private Type codecType;

    Stats() {
    }

    /**
     * Default constructor
     * @param level compression level
     * @param dictSize dictionary size
     * @param type codec type
     */
    public Stats(int level, int dictSize, Type type) {
      this.compressionLevel = level;
      this.dictSize = dictSize;
      this.codecType = type;
    }

    /**
     * Get compression level
     * @return compression level
     */
    public int getCompressionLevel() {
      return this.compressionLevel;
    }

    /**
     * Get dictionary size
     * @return dictionary size
     */
    public int getDictionarySize() {
      return this.dictSize;
    }

    /**
     * Get codec type
     * @return codec type
     */
    public Type getCodecType() {
      return this.codecType;
    }

    /**
     * Get total size of raw data compressed
     * @return size
     */
    public long getCompressedRawSize() {
      return getCompressedRaw().get();
    }

    /**
     * Get total size of compressed data
     * @return size
     */
    public long getCompressedSize() {
      return getCompressed().get();
    }

    /**
     * Get compression ratio
     * @return compression ratio
     */
    public double getCompressionRatio() {
      if (getCompressed().get() == 0) return 0.;
      return (double) getCompressedRaw().get() / getCompressed().get();
    }

    /**
     * Get compression throughput (bytes per second)
     * @return compression throughput
     */
    public double getCompressionThroughput() {
      long time = getCompressionTime().get(); // in nanoseconds
      if (time == 0) return 0.;
      long total = getCompressedRaw().get();
      return (double) total * 1000000000 / time;
    }

    /**
     * Get total size of raw data compressed
     * @return size
     */
    public long getDecompressedRawSize() {
      return getDecompressedRaw().get();
    }

    /**
     * Get total size of data for decompression
     * @return size
     */
    public long getDecompressedSize() {
      return getDecompressed().get();
    }

    /**
     * Get decompression ratio
     * @return decompression ratio
     */
    public double getDecompressionRatio() {
      if (getDecompressed().get() == 0) return 0.;
      return (double) getDecompressedRaw().get() / getDecompressed().get();
    }

    /**
     * Get decompression throughput (bytes per second)
     * @return compression throughput
     */
    public double getDecompressionThroughput() {
      long time = getDecompressionTime().get(); // in nanoseconds
      if (time == 0) return 0.;
      long total = getDecompressedRaw().get();
      return (double) total * 1000000000 / time;
    }

    @Override
    public void save(OutputStream os) throws IOException {
      DataOutputStream dos = Utils.toDataOutputStream(os);
      dos.writeInt(compressionLevel);
      dos.writeInt(dictSize);
      dos.writeInt(codecType.ordinal());
      dos.writeLong(getCompressedRaw().get());
      dos.writeLong(getCompressed().get());
      dos.writeLong(getDecompressedRaw().get());
      dos.writeLong(getDecompressed().get());
      dos.writeLong(getCompressionTime().get());
      dos.writeLong(getDecompressionTime().get());
    }

    @Override
    public void load(InputStream is) throws IOException {
      DataInputStream dis = Utils.toDataInputStream(is);
      this.compressionLevel = dis.readInt();
      this.dictSize = dis.readInt();
      this.codecType = Type.values()[dis.readInt()];
      this.setCompressedRaw(new AtomicLong(dis.readLong()));
      this.setCompressed(new AtomicLong(dis.readLong()));
      this.setDecompressedRaw(new AtomicLong(dis.readLong()));
      this.setDecompressed(new AtomicLong(dis.readLong()));
      this.setCompressionTime(new AtomicLong(dis.readLong()));
      this.setDecompressionTime(new AtomicLong(dis.readLong()));
    }

    public AtomicLong getDecompressedRaw() {
      return decompressedRaw;
    }

    public void setDecompressedRaw(AtomicLong decompressedRaw) {
      this.decompressedRaw = decompressedRaw;
    }

    public AtomicLong getDecompressed() {
      return decompressed;
    }

    public void setDecompressed(AtomicLong decompressed) {
      this.decompressed = decompressed;
    }

    public AtomicLong getDecompressionTime() {
      return decompressionTime;
    }

    public void setDecompressionTime(AtomicLong decompressionTime) {
      this.decompressionTime = decompressionTime;
    }

    public AtomicLong getCompressedRaw() {
      return compressedRaw;
    }

    public void setCompressedRaw(AtomicLong compressedRaw) {
      this.compressedRaw = compressedRaw;
    }

    public AtomicLong getCompressed() {
      return compressed;
    }

    public void setCompressed(AtomicLong compressed) {
      this.compressed = compressed;
    }

    public AtomicLong getCompressionTime() {
      return compressionTime;
    }

    public void setCompressionTime(AtomicLong compressionTime) {
      this.compressionTime = compressionTime;
    }

  }

  public final static int SIZE_OFFSET = 0;

  public final static int DICT_VER_OFFSET = Utils.SIZEOF_INT;

  public final static int COMP_SIZE_OFFSET = 2 * Utils.SIZEOF_INT;

  public final static int COMP_META_SIZE = 3 * Utils.SIZEOF_INT;

  /**
   * Compress in - place
   * @param ptr address block start (excluding meta)
   * @param len data size
   * @param dictId id of dictionary to use
   * @return compressed data size
   */
  public int compress(long ptr, int len, int dictId);

  /**
   * Compress to a side buffer
   * @param ptr address block start (excluding meta)
   * @param len data size
   * @param dictId id of dictionary to use
   * @param buffer memory address to compress to
   * @param bufferSize buffer size
   * @return compressed data size
   */
  public int compress(long ptr, int len, int dictId, long buffer, int bufferSize);

  /**
   * Decompress to a buffer, buffer size is larger or equals to decompressed size
   * @param ptr address of a compressed block in memory (excluding meta)
   * @param size compressed size
   * @param buffer buffer to decompress to (must be able to accommodate decompressed data)
   * @param dictId dictionary ID (optional)
   * @return decompressed size (0 if dictionary id not found)
   */
  public int decompress(long ptr, int size, byte[] buffer, int dictId);

  /**
   * Decompress to a buffer, buffer size is larger or equals to decompressed size
   * @param ptr address of a compressed block in memory (excluding meta)
   * @param size compressed size
   * @param buffer buffer address to decompress to (must be able to accommodate decompressed data)
   * @param bufferSize buffer size
   * @param dictId dictionary ID (optional)
   * @return decompressed size (0 if dictionary id not found)
   */
  public int decompress(long ptr, int size, long buffer, int bufferSize, int dictId);

  /**
   * Decompress to a buffer, buffer size is larger or equals to decompressed size
   * @param src source buffer
   * @param srcOffset offset in the source buffer
   * @param srcSize source size
   * @param buffer buffer to decompress
   * @param dictId dictionary version to use
   * @return decompressed size (0 if dictionary not found)
   */
  public int decompress(byte[] src, int srcOffset, int srcSize, byte[] buffer, int dictId);

  /**
   * Initialize manager for a given cache
   * @param cacheName cache name
   */
  public void init(String cacheName) throws IOException;

  /**
   * Support dictionary compression
   * @return true or false
   */
  public default boolean supportDictionary() {
    return true;
  }

  /**
   * Get codec type
   * @return
   */
  public default Type getCodecType() {
    return Type.ZSTD;
  }

  /**
   * Get dictionary size
   * @return
   */

  public default int getDictionarySize() {
    return 0;
  }

  /**
   * Get compression level
   * @return compression level
   */
  public default int getCompressionLevel() {
    return 0;
  }

  /**
   * Get current dictionary version (0 - no dictionary yet)
   * @return version
   */
  public default int getCurrentDictionaryVersion() {
    return 0;
  }

  /**
   * Is training required
   * @return true or false
   */
  public boolean isTrainingRequired();

  /**
   * Add data to training session
   * @param data
   */
  public void addTrainingData(byte[]... data);

  /**
   * Add training data
   * @param data data
   * @param off data offset
   * @param len data size
   */
  public void addTrainingData(byte[] data, int off, int len);

  /**
   * Add data to training session
   * @param ptr address
   * @param size size
   */
  public void addTrainingData(long ptr, int size);

  /**
   * Get recommended training data size (minimum 100 x dictionary size)
   * @return data size
   */
  public int getRecommendedTrainingDataSize();

  /**
   * Get statistics
   * @return statistics
   */
  public Stats getStats();

}
