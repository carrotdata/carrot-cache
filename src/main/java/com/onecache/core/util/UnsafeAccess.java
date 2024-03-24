/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.onecache.core.util;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.logging.log4j.Logger;

import com.onecache.core.util.RangeTree.Range;

import org.apache.logging.log4j.LogManager;

import sun.misc.Unsafe;

public final class UnsafeAccess {

  public static boolean debug = false;

  public static final class MallocStats {

    public static interface TraceFilter {
      public boolean recordAllocationStackTrace(int size);
    }
    /*
     * Number of memory allocations
     */
    public AtomicLong allocEvents = new AtomicLong();
    /*
     * Number of memory free events
     */
    public AtomicLong freeEvents = new AtomicLong();
    /*
     * Total allocated memory
     */
    public AtomicLong allocated = new AtomicLong();
    /** Total freed memory */
    public AtomicLong freed = new AtomicLong();
    /** Allocation map */
    private RangeTree allocMap = new RangeTree();

    /** Is stack trace record enabled */
    private boolean stackTraceRecordingEnabled = false;

    /** Stack trace filter */
    private TraceFilter filter = null;

    /** Trace recorder maximum record number */
    private int strLimit = Integer.MAX_VALUE; // default - no limit

    /** Stack traces map (allocation address -> stack trace) */
    private Map<Long, String> stackTraceMap =
        Collections.synchronizedMap(new HashMap<Long, String>());

    /**
     * Is stack trace recording enabled
     *
     * @return true, false
     */
    public boolean isStackTraceRecordingEnabled() {
      return this.stackTraceRecordingEnabled;
    }

    /**
     * Set STR enabled
     *
     * @param b
     */
    public void setStackTraceRecordingEnabled(boolean b) {
      this.stackTraceRecordingEnabled = b;
    }

    /**
     * Set STR filter
     *
     * @param f filter
     */
    public void setStackTraceRecordingFilter(TraceFilter f) {
      this.filter = f;
    }

    /**
     * Get STR filter
     *
     * @return filter
     */
    public TraceFilter getStackTraceRecordingFilter() {
      return this.filter;
    }

    /**
     * Set stack trace recording limit
     *
     * @param limit maximum number of records to keep
     */
    public void setStackTraceRecordingLimit(int limit) {
      this.strLimit = limit;
    }

    /**
     * Get STR limit
     *
     * @return limit
     */
    public int getStackTraceRecordingLimit() {
      return this.strLimit;
    }

    /**
     * Returns total number of memory allocations
     *
     * @return number
     */
    public long getAllocEventNumber() {
      return allocEvents.get();
    }

    /**
     * Returns total number of free events
     *
     * @return number
     */
    public long getFreeEventNumber() {
      return freeEvents.get();
    }

    /**
     * Records memory allocation event
     *
     * @param address memory address
     * @param alloced memory size
     */
    public void allocEvent(long address, long alloced) {
      if (!UnsafeAccess.debug) return;
      allocEvents.incrementAndGet();
      allocated.addAndGet(alloced);

      allocMap.delete(address);
      Range r = allocMap.add(new Range(address, (int) alloced));
      if (r != null) {
        System.err.println("Allocation collision [" + r.start + "," + r.size + "]");
      }
      if (isStackTraceRecordingEnabled()) {
        if (stackTraceMap.size() < strLimit) {
          if (filter != null && !filter.recordAllocationStackTrace((int) alloced)) {
            return;
          }
          stackTraceMap.put(address, stackTrace());
        }
      }
    }

    private String stackTrace() {
      PrintStream errStream = System.err;
      ByteArrayOutputStream baos = new ByteArrayOutputStream();
      PrintStream ps = new PrintStream(baos);
      System.setErr(ps);
      Thread.dumpStack();
      System.setErr(errStream);
      String s = baos.toString();
      return s;
    }

    @SuppressWarnings("unused")
    private void dumpIfAlloced(String str, long address, int value, long alloced) {
      if (alloced == value) {
        System.out.println(str + address + " size=" + alloced);
        Thread.dumpStack();
      }
    }

    /**
     * Records memory re-allocation event
     *
     * @param address memory address
     * @param alloced memory size
     */
    public void reallocEvent(long address, long alloced) {
      if (!UnsafeAccess.debug) return;
      Range r = allocMap.delete(address);
      allocMap.add(new Range(address, (int) alloced));
      allocated.addAndGet(alloced);
      freed.addAndGet(r.size);
    }
    /**
     * Records memory free event
     *
     * @param address memory address
     */
    public void freeEvent(long address) {
      if (!UnsafeAccess.debug) return;
      Range mem = allocMap.delete(address);
      if (mem == null) {
        System.out.println("FATAL: not found address " + address);
        Thread.dumpStack();
        System.exit(-1);
      }

      freed.addAndGet(mem.size);
      freeEvents.incrementAndGet();
      if (isStackTraceRecordingEnabled()) {
        stackTraceMap.remove(address);
      }
    }

    /**
     * Checks if we access valid memory
     *
     * @param address memory address
     * @param size memory size
     */
    public void checkAllocation(long address, int size) {
      if (!UnsafeAccess.debug) return;

      if (!allocMap.inside(address, size)) {
        System.out.println(Thread.currentThread().getName() + ": Memory corruption: address=" + address + " size=" + size);
        Thread.dumpStack();
        System.exit(-1);
      }
    }

    /*
     * Prints memory allocation statistics
     */
    public void printStats() {
      printStats(true);
    }

    /**
     * Prints memory allocation statistics
     *
     * @param printOrphans if true - print all orphan allocations
     */
    public void printStats(boolean printOrphans) {
      if (!UnsafeAccess.debug) return;

      System.out.println("\nMalloc stats:");
      System.out.println("allocations          =" + allocEvents.get());
      System.out.println("allocated memory     =" + allocated.get());
      System.out.println("deallocations        =" + freeEvents.get());
      System.out.println("deallocated memory   =" + freed.get());
      System.out.println("leaked (current)     =" + (allocated.get() - freed.get()));
      System.out.println("Orphaned allocations =" + (allocMap.size()));
      if (allocMap.size() > 0 && printOrphans) {
        System.out.println("Orphaned allocation sizes:");
        for (Map.Entry<Range, Range> entry : allocMap.entrySet()) {
          System.out.println(entry.getKey().start + " size=" + entry.getValue().size);
          if (isStackTraceRecordingEnabled()) {
            String strace = stackTraceMap.get(entry.getKey().start);
            if (strace != null) {
              System.out.println(strace);
            }
          }
        }
      }
      System.out.println();
    }
    
    public void clear() {
      /*
       * Number of memory allocations
       */
       allocEvents = new AtomicLong();
      /*
       * Number of memory free events
       */
      freeEvents = new AtomicLong();
      /*
       * Total allocated memory
       */
      allocated = new AtomicLong();
      /** Total freed memory */
      freed = new AtomicLong();
      /** Allocation map */
      allocMap = new RangeTree();
      /** Stack trace filter */
      filter = null;
      /** Trace recorder maximum record number */
      strLimit = Integer.MAX_VALUE; // default - no limit
      /** Stack traces map (allocation address -> stack trace) */
      stackTraceMap =
          Collections.synchronizedMap(new HashMap<Long, String>());

    }
  }

  /** Memory allocator statistics */
  public static MallocStats mallocStats = new MallocStats();

  /** Logger */
  private static final Logger LOG = LogManager.getLogger(UnsafeAccess.class);

  /** The great UNSAFE */
  public static final Unsafe theUnsafe;

  /** Allocation failed */
  public static final long MALLOC_FAILED = -1;

  /** The offset to the first element in a byte array. */
  public static final long BYTE_ARRAY_BASE_OFFSET;

  /** Sets platform byte order */
  static final boolean littleEndian = ByteOrder.nativeOrder().equals(ByteOrder.LITTLE_ENDIAN);

  /*
   * This number limits the number of bytes to copy per call to Unsafe's
   * copyMemory method. A limit is imposed to allow for safepoint polling
   * during a large copy
   */
  static final long UNSAFE_COPY_THRESHOLD = 1024L * 1024L;

  static {
    theUnsafe =
        (Unsafe)
            AccessController.doPrivileged(
                new PrivilegedAction<Object>() {
                  @Override
                  public Object run() {
                    try {
                      Field f = Unsafe.class.getDeclaredField("theUnsafe");
                      f.setAccessible(true);
                      return f.get(null);
                    } catch (Throwable e) {
                      LOG.warn("sun.misc.Unsafe is not accessible", e);
                    }
                    return null;
                  }
                });

    if (theUnsafe != null) {
      BYTE_ARRAY_BASE_OFFSET = theUnsafe.arrayBaseOffset(byte[].class);
    } else {
      BYTE_ARRAY_BASE_OFFSET = -1;
    }
  }

  /** Method handler for DirectByteBuffer::address method */
  static Method addressMethod;

  /** Private constructor */
  private UnsafeAccess() {}

  /** Malloc stats methods */

  /**
   * Set debug mode enabled/disabled
   *
   * @param b
   */
  public static void setMallocDebugEnabled(boolean b) {
    UnsafeAccess.debug = b;
  }

  /**
   * Is malloc debug enabled
   *
   * @return enabled/disabled
   */
  public static boolean isMallocDebugEnabled() {
    return UnsafeAccess.debug;
  }

  /**
   * Set STR enabled
   *
   * @param b true or false
   */
  public static void setMallocDebugStackTraceEnabled(boolean b) {
    mallocStats.setStackTraceRecordingEnabled(b);
  }

  /**
   * Is STR enabled
   *
   * @return true or false
   */
  public static boolean isMallocDebugStackTraceEnabled() {
    return mallocStats.isStackTraceRecordingEnabled();
  }

  /**
   * Set STR filter
   *
   * @param f filter
   */
  public static void setStackTraceRecordingFilter(MallocStats.TraceFilter f) {
    mallocStats.setStackTraceRecordingFilter(f);
  }

  /**
   * Get STR filter
   *
   * @return filter
   */
  public static MallocStats.TraceFilter getStackTraceRecordingFilter() {
    return mallocStats.getStackTraceRecordingFilter();
  }

  /**
   * Set STR limit
   *
   * @param limit
   */
  public static void setStackTraceRecordingLimit(int limit) {
    mallocStats.setStackTraceRecordingLimit(limit);
  }

  /**
   * Get STR limit
   *
   * @return limit
   */
  public static int getStackTraceRecordingLimit() {
    return mallocStats.getStackTraceRecordingLimit();
  }

  /**
   * Get memory address for direct byte buffer
   *
   * @param buf direct byte buffer
   * @return address of a memory buffer or -1 (if not direct or not accessible)
   */
  public static long address(ByteBuffer buf) {
    if (!buf.isDirect()) {
      return -1;
    }

    try {
      if (addressMethod == null) {
        synchronized (UnsafeAccess.class) {
          if (addressMethod == null) {
            Class<? extends ByteBuffer> B = buf.getClass();
            addressMethod = B.getDeclaredMethod("address");
            addressMethod.setAccessible(true);
          }
        }
      }
      Object address = addressMethod.invoke(buf);
      if (address == null) return -1;
      return (Long) address;
    } catch (Throwable e) {
      LOG.warn("java.nio.DirectByteBuffer is not accessible", e);
    }
    return -1;
  }

  // APIs to read primitive data from a byte[] using Unsafe way
  /**
   * Converts a byte array to a short value considering it was written in big-endian format.
   *
   * @param bytes byte array
   * @param offset offset into array
   * @return the short value
   */
  public static short toShort(byte[] bytes, int offset) {
    if (littleEndian) {
      return Short.reverseBytes(theUnsafe.getShort(bytes, offset + BYTE_ARRAY_BASE_OFFSET));
    } else {
      return theUnsafe.getShort(bytes, offset + BYTE_ARRAY_BASE_OFFSET);
    }
  }

  /**
   * Reads short value at a memory address
   *
   * @param addr memory address
   * @return short value
   */
  public static short toShort(long addr) {
    if (UnsafeAccess.debug) {
      mallocStats.checkAllocation(addr, 2);
    }
    if (littleEndian) {
      return Short.reverseBytes(theUnsafe.getShort(addr));
    } else {
      return theUnsafe.getShort(addr);
    }
  }

  /** Bit manipulation routines */

  /**
   * TODO: test Get offset of a first bit set in a long value (8 bytes)
   *
   * @param addr address to read value from
   * @return offset, or -1 if no bits set
   */
  public static int firstBitSetLong(long addr) {

    long value = theUnsafe.getLong(addr);
    if (value == 0) return -1;
    if (littleEndian) {
      value = Long.reverseBytes(value);
    }
    return Long.numberOfLeadingZeros(value);
  }

  /**
   * Get offset of a first bit unset in a long value (8 bytes)
   *
   * @param addr address to read value from
   * @return offset, or -1 if no bits unset
   */
  public static int firstBitUnSetLong(long addr) {

    long value = theUnsafe.getLong(addr);
    if (value == 0xffffffffffffffffL) return -1;
    if (littleEndian) {
      value = Long.reverseBytes(value);
    }
    value = ~value;
    return Long.numberOfLeadingZeros(value);
  }

  /**
   * Get offset of a first bit set in a integer value (4 bytes)
   *
   * @param addr address to read value from
   * @return offset, or -1 if
   */
  public static int firstBitSetInt(long addr) {

    int value = theUnsafe.getInt(addr);
    if (value == 0) return -1;
    if (littleEndian) {
      value = Integer.reverseBytes(value);
    }
    return Integer.numberOfLeadingZeros(value);
  }

  /**
   * Get offset of a first bit unset in a integer value (4 bytes)
   *
   * @param addr address to read value from
   * @return offset of first '0', or -1 if not found
   */
  public static int firstBitUnSetInt(long addr) {

    int value = theUnsafe.getInt(addr);
    if (value == 0xffffffff) return -1;
    if (littleEndian) {
      value = Integer.reverseBytes(value);
    }
    value = ~value;
    return Integer.numberOfLeadingZeros(value);
  }

  /**
   * Get offset of a first bit set in a byte value (1 byte)
   *
   * @param addr address to read value from
   * @return offset of first '1', or -1 if not found
   */
  public static int firstBitSetByte(long addr) {
    byte value = theUnsafe.getByte(addr);
    if (value == 0) return -1;
    return Integer.numberOfLeadingZeros(Byte.toUnsignedInt(value)) - 24;
  }

  /**
   * Get offset of a first bit unset in a byte value (1 byte)
   *
   * @param addr address to read value from
   * @return offset of first '0', or -1 if not found
   */
  public static int firstBitUnSetByte(long addr) {
    byte value = theUnsafe.getByte(addr);
    if (value == (byte) 0xff) return -1;
    // TODO: test it
    value = (byte) ~value;
    return Integer.numberOfLeadingZeros(Byte.toUnsignedInt(value)) - 24;
  }

  /**
   * Get offset of a first bit set in a short value (2 bytes)
   *
   * @param addr address to read value from
   * @return offset of first '1', or -1 if not found
   */
  public static int firstBitSetShort(long addr) {
    short value = theUnsafe.getShort(addr);
    if (value == 0) return -1;
    if (littleEndian) {
      value = Short.reverseBytes(value);
    }
    return Integer.numberOfLeadingZeros(Short.toUnsignedInt(value)) - 16;
  }

  /**
   * Get offset of a first bit unset in a short value (2 bytes)
   *
   * @param addr address to read value from
   * @return offset of first '0', or -1 if not found
   */
  public static int firstBitUnSetShort(long addr) {
    short value = theUnsafe.getShort(addr);
    if (value == (short) 0xffff) return -1;
    if (littleEndian) {
      value = Short.reverseBytes(value);
    }
    value = (short) ~value;
    return Integer.numberOfLeadingZeros(Short.toUnsignedInt(value)) - 16;
  }

  /**
   * Converts a byte array to an int value considering it was written in big-endian format.
   *
   * @param bytes byte array
   * @param offset offset into array
   * @return the int value
   */
  public static int toInt(byte[] bytes, int offset) {
    if (littleEndian) {
      return Integer.reverseBytes(theUnsafe.getInt(bytes, offset + BYTE_ARRAY_BASE_OFFSET));
    } else {
      return theUnsafe.getInt(bytes, offset + BYTE_ARRAY_BASE_OFFSET);
    }
  }

  /**
   * Converts a byte array to a long value considering it was written in big-endian format.
   *
   * @param bytes byte array
   * @param offset offset into array
   * @return the long value
   */
  public static long toLong(byte[] bytes, int offset) {
    if (littleEndian) {
      return Long.reverseBytes(theUnsafe.getLong(bytes, offset + BYTE_ARRAY_BASE_OFFSET));
    } else {
      return theUnsafe.getLong(bytes, offset + BYTE_ARRAY_BASE_OFFSET);
    }
  }

  // APIs to write primitive data to a byte[] using Unsafe way
  /**
   * Put a short value out to the specified byte array position in big-endian format.
   *
   * @param bytes the byte array
   * @param offset position in the array
   * @param val short to write out
   * @return incremented offset
   */
  public static int putShort(byte[] bytes, int offset, short val) {
    if (littleEndian) {
      val = Short.reverseBytes(val);
    }
    theUnsafe.putShort(bytes, offset + BYTE_ARRAY_BASE_OFFSET, val);
    return offset + Bytes.SIZEOF_SHORT;
  }

  /**
   * Put a short value out to the specified address in a big-endian format.
   *
   * @param addr memory address
   * @param val short to write out
   */
  public static void putShort(long addr, short val) {
    if (UnsafeAccess.debug) {
      mallocStats.checkAllocation(addr, 2);
    }

    if (littleEndian) {
      val = Short.reverseBytes(val);
    }
    theUnsafe.putShort(addr, val);
  }

  /**
   * Put a byte value out to the specified byte array position.
   *
   * @param bytes byte array
   * @param offset offset
   * @param val value
   * @return new offset
   */
  public static int putByte(byte[] bytes, int offset, byte val) {
    theUnsafe.putByte(bytes, offset + BYTE_ARRAY_BASE_OFFSET, val);
    return offset + Bytes.SIZEOF_BYTE;
  }

  /**
   * Put a byte value to a specified memory address
   *
   * @param addr memory address
   * @param val byte value
   */
  public static void putByte(long addr, byte val) {
    if(UnsafeAccess.debug) {
      mallocStats.checkAllocation(addr, 1);
    }
    theUnsafe.putByte(addr, val);
  }

  /**
   * Put an int value out to the specified byte array position in big-endian format.
   *
   * @param bytes the byte array
   * @param offset position in the array
   * @param val int to write out
   * @return incremented offset
   */
  public static int putInt(byte[] bytes, int offset, int val) {
    if (littleEndian) {
      val = Integer.reverseBytes(val);
    }
    theUnsafe.putInt(bytes, offset + BYTE_ARRAY_BASE_OFFSET, val);
    return offset + Bytes.SIZEOF_INT;
  }

  /**
   * Put an integer value to a specified memory address
   *
   * @param addr memory address
   * @param val integer value
   */
  public static void putInt(long addr, int val) {
    if (UnsafeAccess.debug) {
      mallocStats.checkAllocation(addr, 4);
    }

    if (littleEndian) {
      val = Integer.reverseBytes(val);
    }
    theUnsafe.putInt(addr, val);
  }
  /**
   * Put a long value out to the specified byte array position in big-endian format.
   *
   * @param bytes the byte array
   * @param offset position in the array
   * @param val long to write out
   * @return incremented offset
   */
  public static int putLong(byte[] bytes, int offset, long val) {
    if (littleEndian) {
      val = Long.reverseBytes(val);
    }
    theUnsafe.putLong(bytes, offset + BYTE_ARRAY_BASE_OFFSET, val);
    return offset + Bytes.SIZEOF_LONG;
  }

  /**
   * Put a long value to a specified memory address
   *
   * @param addr memory address
   * @param val byte value
   */
  public static void putLong(long addr, long val) {
    if (UnsafeAccess.debug) {
      mallocStats.checkAllocation(addr, 8);
    }

    if (littleEndian) {
      val = Long.reverseBytes(val);
    }
    theUnsafe.putLong(addr, val);
  }

  /**
   * Reads a short value at the given Object's offset considering it was written in big-endian
   * format.
   *
   * @param ref
   * @param offset
   * @return short value at offset
   */
  public static short toShort(Object ref, long offset) {
    if (littleEndian) {
      return Short.reverseBytes(theUnsafe.getShort(ref, offset));
    }
    return theUnsafe.getShort(ref, offset);
  }

  /**
   * Reads a int value at the given Object's offset considering it was written in big-endian format.
   *
   * @param ref
   * @param offset
   * @return int value at offset
   */
  public static int toInt(Object ref, long offset) {
    if (littleEndian) {
      return Integer.reverseBytes(theUnsafe.getInt(ref, offset));
    }
    return theUnsafe.getInt(ref, offset);
  }

  /**
   * Reads integer value at a given address
   *
   * @param addr memory address
   * @return integer value
   */
  public static int toInt(long addr) {
    if (UnsafeAccess.debug) {
      mallocStats.checkAllocation(addr, 4);
    }

    if (littleEndian) {
      return Integer.reverseBytes(theUnsafe.getInt(addr));
    } else {
      return theUnsafe.getInt(addr);
    }
  }

  /**
   * Reads a long value at the given Object's offset considering it was written in big-endian
   * format.
   *
   * @param ref
   * @param offset
   * @return long value at offset
   */
  public static long toLong(Object ref, long offset) {
    if (littleEndian) {
      return Long.reverseBytes(theUnsafe.getLong(ref, offset));
    }
    return theUnsafe.getLong(ref, offset);
  }

  /**
   * Reads a long value at the given memory address considering it was written in big-endian format.
   *
   * @param addr memory address
   * @return long value
   */
  public static long toLong(long addr) {
    if (UnsafeAccess.debug) {
      mallocStats.checkAllocation(addr, Utils.SIZEOF_LONG);
    }

    if (littleEndian) {
      return Long.reverseBytes(theUnsafe.getLong(addr));
    } else {
      return theUnsafe.getLong(addr);
    }
  }

  /*
   *  APIs to copy data. This will be direct memory location copy and will be much faster
   */

  /**
   * Copy from a memory to a byte buffer
   *
   * @param src memory address
   * @param dst byte buffer
   * @param len number of bytes to copy
   */
  public static void copy(long src, ByteBuffer dst, int len) {

    int pos = dst.position();
    if (dst.capacity() - pos < len) {
      throw new BufferOverflowException();
    }
    if (dst.isDirect()) {
      long addr = address(dst);
      addr += pos;
      copy_no_dst_check(src, addr, len);
    } else {
      byte[] buf = dst.array();
      copy(src, buf, pos, len);
    }
    dst.position(pos + len);
  }

  /**
   * Copy from a byte buffer to a direct memory
   *
   * @param src byte buffer
   * @param ptr memory destination
   * @param len number of bytes to copy
   */
  public static void copy(ByteBuffer src, long ptr, int len) {
    if (src.remaining() < len) {
      throw new BufferOverflowException();
    }
    int pos = src.position();
    if (src.isDirect()) {
      long addr = address(src);
      addr += pos;
      copy_no_src_check(addr, ptr, len);
    } else {
      byte[] buf = src.array();
      copy(buf, pos, ptr, len);
    }
    src.position(pos + len);
  }
  /**
   * Copies the bytes from given array's offset to length part into the given buffer.
   *
   * @param src source array
   * @param srcOffset offset at source
   * @param dest destination byte buffer
   * @param destOffset offset at destination
   * @param length bytes to copy
   */
  public static void copy(byte[] src, int srcOffset, ByteBuffer dest, int destOffset, int length) {
    long destAddress = destOffset;
    Object destBase = null;
    if (dest.isDirect()) {
      destAddress = destAddress + address(dest);
    } else {
      destAddress = destAddress + BYTE_ARRAY_BASE_OFFSET + dest.arrayOffset();
      destBase = dest.array();
    }
    long srcAddress = srcOffset + BYTE_ARRAY_BASE_OFFSET;
    unsafeCopy(src, srcAddress, destBase, destAddress, length);
  }

  /**
   * Copy data from a byte array to a specified memory address
   *
   * @param src source byte array
   * @param srcOffset byte array offset
   * @param address memory address
   * @param length number of bytes to copy
   */
  public static void copy(byte[] src, int srcOffset, long address, int length) {
    if (UnsafeAccess.debug) {
      mallocStats.checkAllocation(address, length);
    }

    Object destBase = null;
    long srcAddress = srcOffset + BYTE_ARRAY_BASE_OFFSET;
    unsafeCopy(src, srcAddress, destBase, address, length);
  }

  /**
   * Copy data between two byte arrays
   *
   * @param src source array
   * @param srcOffset offset in a source
   * @param dst destination array
   * @param dstOffset offset in a destination
   * @param length number of bytes to copy
   */
  public static void copy(byte[] src, int srcOffset, byte[] dst, int dstOffset, int length) {
    long srcAddress = srcOffset + BYTE_ARRAY_BASE_OFFSET;
    long dstAddress = dstOffset + BYTE_ARRAY_BASE_OFFSET;
    unsafeCopy(src, srcAddress, dst, dstAddress, length);
  }

  /**
   * Copy data from a memory to a byte array
   *
   * @param src source memory address
   * @param dest destination array
   * @param off offset in a destination array
   * @param length number of bytes to copy
   */
  public static void copy(long src, byte[] dest, int off, int length) {
    if (UnsafeAccess.debug) {
      mallocStats.checkAllocation(src, length);
    }

    Object srcBase = null;
    long dstOffset = off + BYTE_ARRAY_BASE_OFFSET;
    unsafeCopy(srcBase, src, dest, dstOffset, length);
  }

  /**
   * Copy data in memory
   *
   * @param src source address
   * @param dst destination address
   * @param len number of bytes to copy
   */
  public static void copy(long src, long dst, long len) {
    if(UnsafeAccess.debug) {
      mallocStats.checkAllocation(src, (int) len);
      mallocStats.checkAllocation(dst, (int) len);
    }

    while (len > 0) {
      long size = (len > UNSAFE_COPY_THRESHOLD) ? UNSAFE_COPY_THRESHOLD : len;
      theUnsafe.copyMemory(src, dst, size);
      len -= size;
      src += size;
      dst += size;
    }
  }

  /**
   * Copy data in memory (no src check in a debug memory mode)
   *
   * @param src source address
   * @param dst destination address
   * @param len number of bytes to copy
   */
  public static void copy_no_src_check(long src, long dst, long len) {
    if (UnsafeAccess.debug) {
      mallocStats.checkAllocation(dst, (int) len);
    }

    while (len > 0) {
      long size = (len > UNSAFE_COPY_THRESHOLD) ? UNSAFE_COPY_THRESHOLD : len;
      theUnsafe.copyMemory(src, dst, size);
      len -= size;
      src += size;
      dst += size;
    }
  }

  /**
   * Copy data in memory (no dst check in a debug memory mode)
   *
   * @param src source address
   * @param dst destination address
   * @param len number of bytes to copy
   */
  public static void copy_no_dst_check(long src, long dst, long len) {
    if (UnsafeAccess.debug) {
      mallocStats.checkAllocation(src, (int) len);
    }
    while (len > 0) {
      long size = (len > UNSAFE_COPY_THRESHOLD) ? UNSAFE_COPY_THRESHOLD : len;
      theUnsafe.copyMemory(src, dst, size);
      len -= size;
      src += size;
      dst += size;
    }
  }

  /**
   * Unsafe copy
   *
   * @param src source
   * @param srcAddr source object address
   * @param dst destination object
   * @param destAddr destination object address
   * @param len number of bytes to copy
   */
  private static void unsafeCopy(Object src, long srcAddr, Object dst, long destAddr, long len) {
    while (len > 0) {
      long size = (len > UNSAFE_COPY_THRESHOLD) ? UNSAFE_COPY_THRESHOLD : len;
      theUnsafe.copyMemory(src, srcAddr, dst, destAddr, size);
      len -= size;
      srcAddr += size;
      destAddr += size;
    }
  }

  /**
   * Returns current allocated memory size
   *
   * @return allocated memory size
   */
  public static long getAllocatedMemory() {
    return mallocStats.allocated.get() - mallocStats.freed.get();
  }

  /**
   * Allocate native memory and copy array
   *
   * @param arr array to copy
   * @param off offset
   * @param len length
   * @return address
   */
  public static long allocAndCopy(byte[] arr, int off, int len) {
    long ptr = malloc(len);
    copy(arr, off, ptr, len);
    return ptr;
  }

  /**
   * Allocate native memory and copy string
   *
   * @param s string
   * @param off offset
   * @param len length
   * @return address
   */
  public static long allocAndCopy(String s, int off, int len) {
    return allocAndCopy(s.getBytes(), off, len);
  }

  /**
   * Allocate native memory and copy source
   *
   * @param src source data
   * @param size size of a source data
   * @return address
   */
  public static long allocAndCopy(long src, int size) {
    long ptr = malloc(size);
    copy(src, ptr, size);
    return ptr;
  }

  /*
   *  APIs to add primitives to BBs
   */
  /**
   * Put a short value out to the specified BB position in big-endian format.
   *
   * @param buf the byte buffer
   * @param offset position in the buffer
   * @param val short to write out
   * @return incremented offset
   */
  public static int putShort(ByteBuffer buf, int offset, short val) {
    if (littleEndian) {
      val = Short.reverseBytes(val);
    }
    if (buf.isDirect()) {
      long addr = address(buf);
      theUnsafe.putShort(addr + offset, val);
    } else {
      theUnsafe.putShort(buf.array(), BYTE_ARRAY_BASE_OFFSET + buf.arrayOffset() + offset, val);
    }
    return offset + Bytes.SIZEOF_SHORT;
  }

  /**
   * Put an integer value out to the specified BB position in big-endian format.
   *
   * @param buf the byte buffer
   * @param offset position in the buffer
   * @param val integer to write out
   * @return incremented offset
   */
  public static int putInt(ByteBuffer buf, int offset, int val) {
    if (littleEndian) {
      val = Integer.reverseBytes(val);
    }
    if (buf.isDirect()) {
      long addr = address(buf);
      theUnsafe.putInt(addr + offset, val);
    } else {
      theUnsafe.putInt(buf.array(), BYTE_ARRAY_BASE_OFFSET + buf.arrayOffset() + offset, val);
    }
    return offset + Bytes.SIZEOF_INT;
  }

  /**
   * Put a long value out to the specified BB position in big-endian format.
   *
   * @param buf the byte buffer
   * @param offset position in the buffer
   * @param val long to write out
   * @return incremented offset
   */
  public static int putLong(ByteBuffer buf, int offset, long val) {
    if (littleEndian) {
      val = Long.reverseBytes(val);
    }
    if (buf.isDirect()) {
      long addr = address(buf);
      theUnsafe.putLong(addr + offset, val);
    } else {
      theUnsafe.putLong(buf.array(), BYTE_ARRAY_BASE_OFFSET + buf.arrayOffset() + offset, val);
    }
    return offset + Bytes.SIZEOF_LONG;
  }
  /**
   * Put a byte value out to the specified BB position in big-endian format.
   *
   * @param buf the byte buffer
   * @param offset position in the buffer
   * @param b byte to write out
   * @return incremented offset
   */
  public static int putByte(ByteBuffer buf, int offset, byte b) {
    // buf.position(offset);
    if (buf.isDirect()) {
      long addr = address(buf);
      theUnsafe.putByte(addr + offset, b);
    } else {
      theUnsafe.putByte(buf.array(), BYTE_ARRAY_BASE_OFFSET + buf.arrayOffset() + offset, b);
    }
    return offset + 1;
  }

  /**
   * Returns the byte at the given offset of the object
   *
   * @param addr
   * @return the byte at the given offset
   */
  public static byte toByte(long addr) {
    if (UnsafeAccess.debug) {
      mallocStats.checkAllocation(addr, 1);
    }

    return theUnsafe.getByte(addr);
  }

  /**
   * Allocate memory
   *
   * @param size size of a memory to allocate
   * @return memory pointer
   */
  public static long malloc(long size) {
    long address = theUnsafe.allocateMemory(size);
    mallocStats.allocEvent(address, size);
    return address;
  }

  /**
   * Allocate memory zeroed
   *
   * @param size size of amemory to allocate
   * @return memory pointer
   */
  public static long mallocZeroed(long size) {
    long address = theUnsafe.allocateMemory(size);
    theUnsafe.setMemory(address, size, (byte) 0);
    mallocStats.allocEvent(address, size);
    return address;
  }

  /** Print memory allocation statistics */
  public static void mallocStats() {
    mallocStats.printStats();
  }

  /**
   * Reallocate memory
   *
   * @param ptr memory address
   * @param newSize new size
   * @return memory address
   */
  public static long realloc(long ptr, long newSize) {
    long pptr = theUnsafe.reallocateMemory(ptr, newSize);
    if (pptr != ptr) {
      mallocStats.freeEvent(ptr);
      mallocStats.allocEvent(pptr, newSize);
    } else {
      mallocStats.reallocEvent(pptr, newSize);
    }
    return pptr;
  }

  /** Reallocate memory zeroed */
  public static long reallocZeroed(long ptr, long oldSize, long newSize) {
    long addr = theUnsafe.reallocateMemory(ptr, newSize);
    theUnsafe.setMemory(addr + oldSize, newSize - oldSize, (byte) 0);
    if (addr != ptr) {
      mallocStats.freeEvent(ptr);
      mallocStats.allocEvent(addr, newSize);
    } else {
      mallocStats.reallocEvent(addr, newSize);
    }

    return addr;
  }

  public static void setMemory(long ptr, long size, byte v) {
    theUnsafe.setMemory(ptr, size, v);
  }
  /**
   * Free memory
   *
   * @param ptr memory pointer
   */
  public static void free(long ptr) {
    mallocStats.freeEvent(ptr);
    theUnsafe.freeMemory(ptr);
  }

  /** Load fence command */
  public static void loadFence() {
    theUnsafe.loadFence();
  }

  /** Store fence command */
  public static void storeFence() {
    theUnsafe.storeFence();
  }

  /** Full fence command */
  public static void fullFence() {
    theUnsafe.fullFence();
  }
}
