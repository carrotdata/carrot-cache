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
package com.carrotdata.cache.util;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Concurrent (circular) memory buffer for writes and reads. Every write has a maximum size known in
 * advance - page size. It requires maximum number of concurrent threads to be known in advance as
 * well. Writes are lock - free, reads are lock - free and wait free. To implement wait free and
 * lock free reads we set size of an internal memory buffer to 2 * max_threads * 2 * page_size and
 * if distance between read slot and current write slot exceeds maximum number of threads we fail
 * the read. The reason is two-fold: this gives us guarantee that read cell won't overwritten by
 * writes during read operation and the requested data should have been evacuated already by thus
 * time, so a caller can call other storage for a data Limitations: page size <=32K (can be
 * increased by a few line code changes), maximum number of threads <= 16K ()
 */

public class ConcurrentWriteReadBuffer {

  public static interface DataEvacuator {
    public void evacuate(int slot, long memPtr, int size);
  }

  /**
   * Memory buffer
   */
  final private long memory;

  /*
   * Size of a page
   */
  final private int pageSize;

  /*
   * Size of a slot (x2 of page)
   */
  final private int slotSize;

  /*
   * Number of slots
   */
  final private int numSlots;

  // Atomic and volatiles section

  /*
   * Write pass number (starts with 1)
   */
  public volatile int writeRun = 1;

  /*
   * Current slot we are writing into (0 - based)
   */
  public volatile short writeSlotNumber;

  /*
   * Write offset in a current write slot
   */
  private AtomicInteger slotWriteOffset = new AtomicInteger(0);

  /*
   * Confirmed write offset
   */
  private volatile int confirmedWriteOffset = 0;

  private AtomicLong ticketRequested = new AtomicLong();

  private volatile int ticketConfirmed = 0;

  /**
   * Data evacuator
   */
  private DataEvacuator evacuator;

  public ConcurrentWriteReadBuffer(DataEvacuator de, int pageSize, int maxConcurrentThreads) {
    this.pageSize = pageSize;
    this.numSlots = 2 * maxConcurrentThreads;
    this.slotSize = 2 * pageSize;
    this.memory = UnsafeAccess.mallocZeroed(this.numSlots * this.slotSize);
    this.evacuator = de;
  }

  /**
   * Get page size - maximum of blob size
   * @return size
   */
  public int getPageSize() {
    return this.pageSize;
  }

  /**
   * Write key-value and expire to the buffer
   * @param keyPtr key address
   * @param keySize key size
   * @param valuePtr value address
   * @param valueSize value size
   * @param expire expiration time
   * @return index of a record (4 bytes - write run number, 2 bytes slot number, 2 bytes offset in a
   *         slot
   */
  public long append(long keyPtr, int keySize, long valuePtr, int valueSize, long expire) {
    int requiredSize = keySize + valueSize + Utils.sizeUVInt(keySize) + Utils.sizeUVInt(valueSize)
        + Utils.SIZEOF_LONG;
    if (requiredSize > this.pageSize) {
      throw new IllegalArgumentException("required serialized size exceeds page size");
    }

    int off = slotWriteOffset.getAndAdd(requiredSize);
    if (off >= this.pageSize) {
      // Wait until next write slot will become available and we get correct write offset
      while ((off = slotWriteOffset.getAndAdd(requiredSize)) >= this.pageSize)
        ;
    }

    // TODO writeSlotNumber, slotWriteOffset and confirmedWriteOffset
    // MUST be set all atomically in single operation

    // At this point, both: writeSlotNumber and writeRun are fixed
    // (they will be changed either by this thread or after this
    // method exits)

    short writeSlotNumber = this.writeSlotNumber;
    int writeRun = this.writeRun;

    // We have writeRun and offset to write
    long address = this.memory + (long) writeSlotNumber * this.slotSize + off;
    // Copy data with size info
    int keySizeSize = Utils.writeUVInt(address, keySize);
    address += keySizeSize;
    int valueSizeSize = Utils.writeUVInt(address, valueSize);
    address += valueSizeSize;
    UnsafeAccess.copy(keyPtr, address, keySize);
    address += keySize;
    UnsafeAccess.copy(valuePtr, address, valueSize);
    address += valueSize;
    UnsafeAccess.putLong(address, expire);
    // Spin - wait until all threads catch up
    while (confirmedWriteOffset < off)
      ;

    confirmedWriteOffset += requiredSize;

    // Build return index
    long value = (long) writeRun << 32 | (long) writeSlotNumber << 16 | (long) off;

    // Now check if we need to start new slot
    if (off + requiredSize >= this.pageSize) {

      // Advance write slot number
      if (writeSlotNumber == numSlots - 1) {
        // Advance write run number
        if (writeRun == Integer.MAX_VALUE) {
          this.writeRun = 1;
        } else {
          this.writeRun = writeRun + 1;
        }
      }
      // Resume all other threads pending
      // NO this triple MUST be one atomic op OR?

      this.writeSlotNumber = (short) ((writeSlotNumber + 1) % this.numSlots);
      confirmedWriteOffset = 0;
      // The last one is slotWriteOffset - it cancels all other threads busy loops at the beginning
      // of
      // this method. Because writeSlotNumber and confirmedWriteOffset are both volatile, their
      // updated values will be visible to all other threads after busy loop ends immediately.
      slotWriteOffset.set(0);

      // Now evacuate full slot
      long ticket = ticketRequested.getAndIncrement();
      evacuate(writeSlotNumber, off + requiredSize);
      while (ticketConfirmed < ticket)
        ;
      ticketConfirmed++;
      // For last record we return 0
      return 0;
    }
    return value;
  }

  private void evacuate(short slotNumber, int size) {
    if (this.evacuator != null) {
      long address = this.memory + slotNumber * this.slotSize;
      this.evacuator.evacuate(slotNumber, address, size);
    }
  }

  public int read(long index, long buffer, int bufferSize) {
    // TODO: sanity check for index
    int writeRun = (int) ((index >>> 32) & 0xffffffff);
    int curWriteRun = this.writeRun;
    short slotNumber = (short) ((index >>> 16) & 0xffff);
    short off = (short) (index & 0xffff);

    if (curWriteRun >= writeRun) {
      long dist =
          (long) (curWriteRun - writeRun) * this.numSlots + this.writeSlotNumber - slotNumber;
      if (dist >= this.numSlots / 2 /* this is the maximum number of concurrent threads */) {
        // We wont check this distant write because it should have been evacuated already
        return -1; // NOT FOUND
      }
    } else {
      long dist = (long) (Integer.MAX_VALUE - (writeRun - curWriteRun)) * this.numSlots
          + this.writeSlotNumber - slotNumber;
      if (dist >= this.numSlots / 2 /* this is the maximum number of concurrent threads */) {
        // We wont check this distant write because it should have been evacuated already
        return -1; // NOT FOUND
      }
    }
    // From this point reads are safe, because under no circumstances
    // current write slot can reach this read slot - our number
    // of slots is double of maximum concurrent threads
    long address = this.memory + (long) slotNumber * this.slotSize + off;
    int keySize = Utils.readUVInt(address);
    int keySizeSize = Utils.sizeUVInt(keySize);
    address += keySizeSize;
    int valueSize = Utils.readUVInt(address);
    int valueSizeSize = Utils.sizeUVInt(valueSize);
    int requiredSize = keySize + valueSize + keySizeSize + valueSizeSize + Utils.SIZEOF_LONG;
    if (requiredSize > bufferSize) {
      return requiredSize;
    }
    UnsafeAccess.copy(address, buffer, requiredSize);
    return requiredSize;
  }

  /**
   * Return address of a k-v record or -1. This call avoid double memory copy and allows to copy
   * data directly from this cache storage to some output buffer
   * @param index record's index
   * @return address in memory or -1 (not found)
   */
  public long getAddressByIndex(long index) {
    int writeRun = (int) ((index >>> 32) & 0xffffffff);

    int curWriteRun = this.writeRun;
    short slotNumber = (short) ((index >>> 16) & 0xffff);
    short off = (short) (index & 0xffff);

    if (curWriteRun >= writeRun) {
      long dist =
          (long) (curWriteRun - writeRun) * this.numSlots + this.writeSlotNumber - slotNumber;
      if (dist >= this.numSlots / 2 /* this is the maximum number of concurrent threads */) {
        // We wont check this distant write because it should have been evacuated already
        return -1; // NOT FOUND
      }
    } else {
      long dist = (long) (Integer.MAX_VALUE - (writeRun - curWriteRun)) * this.numSlots
          + this.writeSlotNumber - slotNumber;
      if (dist >= this.numSlots / 2 /* this is the maximum number of concurrent threads */) {
        // We wont check this distant write because it should have been evacuated already
        return -1; // NOT FOUND
      }
    }
    // From this point reads are safe, because under no circumstances
    // current write slot can reach this read slot - our number
    // of slots is double of maximum concurrent threads
    long address = this.memory + (long) slotNumber * this.slotSize + off;
    if (address < this.memory || address >= this.memory + this.numSlots * this.slotSize) {
      throw new IllegalArgumentException("Illegal memory access");
    }
    return address;

  }

  /**
   * Utility methods
   */

  public static final int getWriteRun(long index) {
    return (int) (index >>> 32 & 0xffffffff);
  }

  public static final short getSlotNumber(long index) {
    return (short) (index >>> 16 & 0xffff);
  }

  public static final short getOffset(long index) {
    return (short) (index & 0xffff);
  }

  /**
   * Used for testing
   */
  public void dispose() {
    UnsafeAccess.free(memory);
  }
}
