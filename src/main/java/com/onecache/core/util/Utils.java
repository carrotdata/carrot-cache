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

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.Random;


import sun.misc.Unsafe;

public class Utils {

  public static final int SIZEOF_LONG = 8;
  public static final int SIZEOF_DOUBLE = 8;

  public static final int SIZEOF_INT = 4;
  public static final int SIZEOF_FLOAT = 4;

  public static final int SIZEOF_SHORT = 2;
  public static final int SIZEOF_BYTE = 1;

  public static final int BITS_PER_BYTE = 8;
  
  public static int javaVersion = getJavaVersion();

  static Class<Thread> clz = Thread.class;
  static Method onSpinWait;
  
  static {
    try {
      onSpinWait = clz.getDeclaredMethod("onSpinWait");
    } catch (NoSuchMethodException | SecurityException e) {      
    }
  }
  /**
   * Returns true if x1 is less than x2, when both values are treated as unsigned long. <br>
   * Both values are passed as is read by Unsafe. When platform is Little Endian, have <br>
   * to convert to corresponding Big Endian value and then do compare. We do all writes <br>
   * in Big Endian format.
   */
  static boolean lessThanUnsignedLong(long x1, long x2) {
    if (UnsafeAccess.littleEndian) {
      x1 = Long.reverseBytes(x1);
      x2 = Long.reverseBytes(x2);
    }
    return (x1 + Long.MIN_VALUE) < (x2 + Long.MIN_VALUE);
  }

  /**
   * Returns true if x1 is less than x2, when both values are treated as unsigned int. Both values
   * are passed as is read by Unsafe. When platform is Little Endian, have to convert to
   * corresponding Big Endian value and then do compare. We do all writes in Big Endian format.
   */
  static boolean lessThanUnsignedInt(int x1, int x2) {
    if (UnsafeAccess.littleEndian) {
      x1 = Integer.reverseBytes(x1);
      x2 = Integer.reverseBytes(x2);
    }
    return (x1 & 0xffffffffL) < (x2 & 0xffffffffL);
  }

  /**
   * Returns true if x1 is less than x2, when both values are treated as unsigned short. Both values
   * are passed as is read by Unsafe. When platform is Little Endian, have to convert to
   * corresponding Big Endian value and then do compare. We do all writes in Big Endian format.
   */
  static boolean lessThanUnsignedShort(short x1, short x2) {
    if (UnsafeAccess.littleEndian) {
      x1 = Short.reverseBytes(x1);
      x2 = Short.reverseBytes(x2);
    }
    return (x1 & 0xffff) < (x2 & 0xffff);
  }

  /**
   * Lexicographically compares byte buffer and array 
   * @param buffer byte buffer
   * @param len length
   * @param buffer2 array
   * @param bufOffset2 offset
   * @param len2 length
   * @return  0 if equal, &lt; 0 if left is less than right, etc.
   */
  public static int compareTo(ByteBuffer buffer, int len, byte[] buffer2, int bufOffset2, int len2) {
    int off = buffer.position();
    if (buffer.hasArray()) {
      byte[] arr = buffer.array();
      return compareTo(arr, off, len, buffer2, bufOffset2, len2);
    } else {
      long address = UnsafeAccess.address(buffer);
      return -compareTo(buffer2, bufOffset2, len2, address + off, len);
    }
  }
  
  /**
   * Lexicographically compares byte buffer and memory 
   * @param buffer byte buffer
   * @param len length
   * @param ptr memory address
   * @param len2 length
   * @return  0 if equal, &lt; 0 if left is less than right, etc.
   */
  public static int compareTo(ByteBuffer buffer, int len, long ptr,  int len2) {
    int off = buffer.position();
    if (buffer.hasArray()) {
      byte[] arr = buffer.array();
      return compareTo(arr, off, len, ptr, len2);
    } else {
      long address = UnsafeAccess.address(buffer);
      return compareTo(address + off, len, ptr, len2);
    }
  }
  
  /**
   * Lexicographically compare two arrays.
   *
   * @param buffer1 left operand
   * @param buffer2 right operand
   * @param offset1 Where to start comparing in the left buffer
   * @param offset2 Where to start comparing in the right buffer
   * @param length1 How much to compare from the left buffer
   * @param length2 How much to compare from the right buffer
   * @return 0 if equal, &lt; 0 if left is less than right, etc.
   */
  public static int compareTo(
      byte[] buffer1, int offset1, int length1, byte[] buffer2, int offset2, int length2) {

    Unsafe theUnsafe = UnsafeAccess.theUnsafe;
    // Short circuit equal case
    if (buffer1 == buffer2 && offset1 == offset2 && length1 == length2) {
      return 0;
    }
    final int minLength = Math.min(length1, length2);
    final int minWords = minLength / SIZEOF_LONG;
    final long offset1Adj = offset1 + UnsafeAccess.BYTE_ARRAY_BASE_OFFSET;
    final long offset2Adj = offset2 + UnsafeAccess.BYTE_ARRAY_BASE_OFFSET;

    /*
     * Compare 8 bytes at a time. Benchmarking shows comparing 8 bytes at a time is no slower than
     * comparing 4 bytes at a time even on 32-bit. On the other hand, it is substantially faster on
     * 64-bit.
     */
    // This is the end offset of long parts.
    int j = minWords << 3; // Same as minWords * SIZEOF_LONG
    for (int i = 0; i < j; i += SIZEOF_LONG) {
      long lw = theUnsafe.getLong(buffer1, offset1Adj + (long) i);
      long rw = theUnsafe.getLong(buffer2, offset2Adj + (long) i);
      long diff = lw ^ rw;
      if (diff != 0) {
        return lessThanUnsignedLong(lw, rw) ? -1 : 1;
      }
    }
    int offset = j;

    if (minLength - offset >= SIZEOF_INT) {
      int il = theUnsafe.getInt(buffer1, offset1Adj + offset);
      int ir = theUnsafe.getInt(buffer2, offset2Adj + offset);
      if (il != ir) {
        return lessThanUnsignedInt(il, ir) ? -1 : 1;
      }
      offset += SIZEOF_INT;
    }
    if (minLength - offset >= SIZEOF_SHORT) {
      short sl = theUnsafe.getShort(buffer1, offset1Adj + offset);
      short sr = theUnsafe.getShort(buffer2, offset2Adj + offset);
      if (sl != sr) {
        return lessThanUnsignedShort(sl, sr) ? -1 : 1;
      }
      offset += SIZEOF_SHORT;
    }
    if (minLength - offset == 1) {
      int a = (buffer1[(int) (offset1 + offset)] & 0xff);
      int b = (buffer2[(int) (offset2 + offset)] & 0xff);
      if (a != b) {
        return a - b;
      }
    }
    return length1 - length2;
  }

  /**
   * Lexicographically compare array and native memory.
   *
   * @param buffer1 left operand
   * @param address right operand - native
   * @param offset1 Where to start comparing in the left buffer
   * @param length1 How much to compare from the left buffer
   * @param length2 How much to compare from the right buffer
   * @return 0 if equal, &lt; 0 if left is less than right, etc.
   */
  public static int compareTo(byte[] buffer1, int offset1, int length1, long address, int length2) {

    if (UnsafeAccess.debug) {
      UnsafeAccess.mallocStats.checkAllocation(address, length2);
    }
    Unsafe theUnsafe = UnsafeAccess.theUnsafe;

    final int minLength = Math.min(length1, length2);
    final int minWords = minLength / SIZEOF_LONG;
    final long offset1Adj = offset1 + UnsafeAccess.BYTE_ARRAY_BASE_OFFSET;

    /*
     * Compare 8 bytes at a time. Benchmarking shows comparing 8 bytes at a time is no slower than
     * comparing 4 bytes at a time even on 32-bit. On the other hand, it is substantially faster on
     * 64-bit.
     */
    // This is the end offset of long parts.
    int j = minWords << 3; // Same as minWords * SIZEOF_LONG
    for (int i = 0; i < j; i += SIZEOF_LONG) {
      long lw = theUnsafe.getLong(buffer1, offset1Adj + (long) i);
      long rw = theUnsafe.getLong(address + (long) i);
      long diff = lw ^ rw;
      if (diff != 0) {
        return lessThanUnsignedLong(lw, rw) ? -1 : 1;
      }
    }
    int offset = j;

    if (minLength - offset >= SIZEOF_INT) {
      int il = theUnsafe.getInt(buffer1, offset1Adj + offset);
      int ir = theUnsafe.getInt(address + offset);
      if (il != ir) {
        return lessThanUnsignedInt(il, ir) ? -1 : 1;
      }
      offset += SIZEOF_INT;
    }
    if (minLength - offset >= SIZEOF_SHORT) {
      short sl = theUnsafe.getShort(buffer1, offset1Adj + offset);
      short sr = theUnsafe.getShort(address + offset);
      if (sl != sr) {
        return lessThanUnsignedShort(sl, sr) ? -1 : 1;
      }
      offset += SIZEOF_SHORT;
    }
    if (minLength - offset == 1) {
      int a = (buffer1[(int) (offset1 + offset)] & 0xff);
      int b = theUnsafe.getByte(address + offset) & 0xff;
      if (a != b) {
        return a - b;
      }
    }
    return length1 - length2;
  }

  /**
   * Lexicographically compare two native memory pointers.
   *
   * @param address1 first pointer
   * @param length1 length
   * @param address2 second pointer
   * @param length2 length
   * @return 0 if equal,&lt; 0 if left is less than right, etc.
   */
  public static int compareTo(long address1, int length1, long address2, int length2) {
    if(UnsafeAccess.debug) {
      UnsafeAccess.mallocStats.checkAllocation(address1, length1);
      UnsafeAccess.mallocStats.checkAllocation(address2, length2);
    }

    Unsafe theUnsafe = UnsafeAccess.theUnsafe;

    final int minLength = Math.min(length1, length2);
    final int minWords = minLength / SIZEOF_LONG;

    /*
     * Compare 8 bytes at a time. Benchmarking shows comparing 8 bytes at a time is no slower than
     * comparing 4 bytes at a time even on 32-bit. On the other hand, it is substantially faster on
     * 64-bit.
     */
    // This is the end offset of long parts.
    int j = minWords << 3; // Same as minWords * SIZEOF_LONG
    for (int i = 0; i < j; i += SIZEOF_LONG) {
      long lw = theUnsafe.getLong(address1 + (long) i);
      long rw = theUnsafe.getLong(address2 + (long) i);
      long diff = lw ^ rw;
      if (diff != 0) {
        return lessThanUnsignedLong(lw, rw) ? -1 : 1;
      }
    }
    int offset = j;

    if (minLength - offset >= SIZEOF_INT) {
      int il = theUnsafe.getInt(address1 + offset);
      int ir = theUnsafe.getInt(address2 + offset);
      if (il != ir) {
        return lessThanUnsignedInt(il, ir) ? -1 : 1;
      }
      offset += SIZEOF_INT;
    }
    if (minLength - offset >= SIZEOF_SHORT) {
      short sl = theUnsafe.getShort(address1 + offset);
      short sr = theUnsafe.getShort(address2 + offset);
      if (sl != sr) {
        return lessThanUnsignedShort(sl, sr) ? -1 : 1;
      }
      offset += SIZEOF_SHORT;
    }
    if (minLength - offset == 1) {
      int a = theUnsafe.getByte(address1 + offset) & 0xff;
      int b = theUnsafe.getByte(address2 + offset) & 0xff;
      if (a != b) {
        return a - b;
      }
    }
    return length1 - length2;
  }

  /**
   * TODO: THIS METHOD IS UNSAFE??? CHECK IT Read unsigned VarInt
   *
   * @param ptr address to read from
   * @return int value
   */
  public static int readUVInt(long ptr) {
    int v1 = UnsafeAccess.toByte(ptr) & 0xff;

    int cont = v1 >>> 7; // either 0 or 1
    ptr += cont;
    v1 &= 0x7f; // set 8th bit 0
    int v2 = (byte) (UnsafeAccess.toByte(ptr) * cont) & 0xff;
    cont = v2 >>> 7;
    ptr += cont;
    v2 &= 0x7f;
    int v3 = (byte) (UnsafeAccess.toByte(ptr) * cont) & 0xff;
    cont = v3 >>> 7;
    ptr += cont;
    v3 &= 0x7f;
    int v4 = (byte) (UnsafeAccess.toByte(ptr) * cont) & 0xff;
    v4 &= 0x7f;
    return v1 + (v2 << 7) + (v3 << 14) + (v4 << 21);
  }

  /**
   * Read unsigned VarInt from a byte buffer
   * @param buf byte buffer
   * @param off offset
   * @return value
   */
  public static int readUVInt(byte[] buf, int off) {
    int v1 = buf[off] & 0xff;
    int cont = v1 >>> 7; // either 0 or 1
    off += cont;
    v1 &= 0x7f; // set 8th bit 0
    int v2 = (byte) (buf[off] * cont) & 0xff;
    cont = v2 >>> 7;
    off += cont;
    v2 &= 0x7f;
    int v3 = (byte) (buf[off] * cont) & 0xff;
    cont = v3 >>> 7;
    off += cont;
    v3 &= 0x7f;
    int v4 = (byte) (buf[off] * cont) & 0xff;
    v4 &= 0x7f;
    return v1 + (v2 << 7) + (v3 << 14) + (v4 << 21);
  }
  
  /**
   * Read unsigned VarInt from a byte buffer
   * @param buf byte buffer
   * @return value
   */
  public static int readUVInt(ByteBuffer buf) {
    int off = buf.position();
    int v1 = buf.get(off) & 0xff;
    int cont = v1 >>> 7; // either 0 or 1
    off += cont;
    v1 &= 0x7f; // set 8th bit 0
    int v2 = (byte) (buf.get(off) * cont) & 0xff;
    cont = v2 >>> 7;
    off += cont;
    v2 &= 0x7f;
    int v3 = (byte) (buf.get(off) * cont) & 0xff;
    cont = v3 >>> 7;
    off += cont;
    v3 &= 0x7f;
    int v4 = (byte) (buf.get(off) * cont) & 0xff;
    v4 &= 0x7f;
    return v1 + (v2 << 7) + (v3 << 14) + (v4 << 21);
  }
  
  /**
   * Returns size of unsigned variable integer in bytes
   * The maximum positive value is 256M
   * To support large values we need to change read/write code
   * 
   * @param value
   * @return size in bytes
   */
  public static int sizeUVInt(int value) {
    if (value < v1) {
      return 1;
    } else if (value < v2) {
      return 2;
    } else if (value < v3) {
      return 3;
    } else if (value < v4) {
      return 4;
    }
    return 0;
  }

  static final int v1 = 1 << 7;
  static final int v2 = 1 << 14;
  static final int v3 = 1 << 21;
  static final int v4 = 1 << 28;
  /**
   * Writes unsigned variable integer
   *
   * @param ptr address to write to
   * @param value
   * @return number of bytes written
   */
  public static int writeUVInt(long ptr, int value) {

    if (value < v1) {
      UnsafeAccess.putByte(ptr, (byte) value);
      return 1;
    } else if (value < v2) {
      UnsafeAccess.putByte(ptr, (byte) ((value & 0xff) | 0x80));
      UnsafeAccess.putByte(ptr + 1, (byte) (value >>> 7));
      return 2;
    } else if (value < v3) {
      UnsafeAccess.putByte(ptr, (byte) ((value & 0xff) | 0x80));
      UnsafeAccess.putByte(ptr + 1, (byte) ((value >>> 7) | 0x80));
      UnsafeAccess.putByte(ptr + 2, (byte) (value >>> 14));
      return 3;
    } else if (value < v4) {
      UnsafeAccess.putByte(ptr, (byte) ((value & 0xff) | 0x80));
      UnsafeAccess.putByte(ptr + 1, (byte) ((value >>> 7) | 0x80));
      UnsafeAccess.putByte(ptr + 2, (byte) ((value >>> 14) | 0x80));
      UnsafeAccess.putByte(ptr + 3, (byte) (value >>> 21));
      return 4;
    }
    return 0;
  }


  /**
   * Murmur3hash implementation with native pointer.
   *
   * @param ptr the address of memory
   * @param len the length of memory
   * @param seed the seed
   * @return hash value
   */
  public static int murmurHash(long ptr, int len, int seed) {
    Unsafe unsafe = UnsafeAccess.theUnsafe;

    final int m = 0x5bd1e995;
    final int r = 24;
    final int length = len;
    int h = seed ^ length;

    final int len_4 = length >> 2;
    for (int i = 0; i < len_4; i++) {
      int i_4 = i << 2;
      int k = unsafe.getByte(ptr + i_4 + 3) & 0xff;
      k = k << 8;
      k = k | (unsafe.getByte(ptr + i_4 + 2) & 0xff);
      k = k << 8;
      k = k | (unsafe.getByte(ptr + i_4 + 1) & 0xff);
      k = k << 8;
      k = k | (unsafe.getByte(ptr + i_4) & 0xff);
      k *= m;
      k ^= k >>> r;
      k *= m;
      h *= m;
      h ^= k;
    }
    // avoid calculating modulo
    int len_m = len_4 << 2;
    int left = length - len_m;

    if (left != 0) {
      if (left >= 3) {
        h ^= (unsafe.getByte(ptr + length - 3) & 0xff) << 16;
      }
      if (left >= 2) {
        h ^= (unsafe.getByte(ptr + length - 2) & 0xff) << 8;
      }
      if (left >= 1) {
        h ^= (unsafe.getByte(ptr + length - 1) & 0xff);
      }

      h *= m;
    }

    h ^= h >>> 13;
    h *= m;
    h ^= h >>> 15;

    // This is a stupid thinh I have ever stuck upon
    if (h == Integer.MIN_VALUE) h = -(Integer.MIN_VALUE + 1);
    return h;
  }

  /**
   * Murmur3hash implementation.
   *
   * @param buf byte buffer
   * @param off offset
   * @param len length
   * @param seed the seed
   * @return hash value
   */
  public static int murmurHash(byte[] buf, int off, int len, int seed) {

    final int m = 0x5bd1e995;
    final int r = 24;
    final int length = len;
    int h = seed ^ length;

    final int len_4 = length >> 2;
    for (int i = 0; i < len_4; i++) {
      int i_4 = i << 2;
      int k = buf[off + i_4 + 3] & 0xff;
      k = k << 8;
      k = k | (buf[off + i_4 + 2] & 0xff);
      k = k << 8;
      k = k | (buf[off + i_4 + 1] & 0xff);
      k = k << 8;
      k = k | (buf[off + i_4] & 0xff);
      k *= m;
      k ^= k >>> r;
      k *= m;
      h *= m;
      h ^= k;
    }
    // avoid calculating modulo
    int len_m = len_4 << 2;
    int left = length - len_m;

    if (left != 0) {
      if (left >= 3) {
        h ^= (buf[off + length - 3] & 0xff) << 16;
      }
      if (left >= 2) {
        h ^= (buf[off + length - 2] & 0xff) << 8;
      }
      if (left >= 1) {
        h ^= (buf[off + length - 1] & 0xff);
      }

      h *= m;
    }

    h ^= h >>> 13;
    h *= m;
    h ^= h >>> 15;

    // This is a most stupid thing I have ever stuck upon
    if (h == Integer.MIN_VALUE) h = -(Integer.MIN_VALUE + 1);
    return h;
  }



  /**
   * Hash 8 bytes hash
   *
   * @param buf buffer
   * @param off offset
   * @param len length
   * @return hash number as a long
   */
  public static long hash64(byte[] buf, int off, int len) {
    return hash64(buf, off, len, 6754);
  }

  /**
   * Hash 8 bytes hash
   *
   * @param ptr pointer
   * @param len length
   * @return hash number as a long
   */
  public static long hash64(long ptr, int len) {
    return hash64(ptr, len, 6754);
  }



  // Constants for 128-bit variant
  private static final long C1 = 0x87c37b91114253d5L;
  private static final long C2 = 0x4cf5ad432745937fL;
  private static final int R1 = 31;
  private static final int R2 = 27;
  private static final int M = 5;
  private static final int N1 = 0x52dce729;
  
  public static long hash64(final byte[] data, final int offset, final int length, final int seed) {
      // ************
      // Note: This fails to apply masking using 0xffffffffL to the seed.
      // ************
      long hash = seed;
      final int nblocks = length >> 3;

      // body
      for (int i = 0; i < nblocks; i++) {
          final int index = offset + (i << 3);
          long k = getLittleEndianLong(data, index);

          // mix functions
          k *= C1;
          k = Long.rotateLeft(k, R1);
          k *= C2;
          hash ^= k;
          hash = Long.rotateLeft(hash, R2) * M + N1;
      }

      // tail
      long k1 = 0;
      final int index = offset + (nblocks << 3);
      switch (offset + length - index) {
      case 7:
          k1 ^= ((long) data[index + 6] & 0xff) << 48;
      case 6:
          k1 ^= ((long) data[index + 5] & 0xff) << 40;
      case 5:
          k1 ^= ((long) data[index + 4] & 0xff) << 32;
      case 4:
          k1 ^= ((long) data[index + 3] & 0xff) << 24;
      case 3:
          k1 ^= ((long) data[index + 2] & 0xff) << 16;
      case 2:
          k1 ^= ((long) data[index + 1] & 0xff) << 8;
      case 1:
          k1 ^= ((long) data[index] & 0xff);
          k1 *= C1;
          k1 = Long.rotateLeft(k1, R1);
          k1 *= C2;
          hash ^= k1;
      }

      // finalization
      hash ^= length;
      hash = fmix64(hash);

      return hash;
  }
  
  public static long hash64(final long data, final int length, final int seed) {
    // ************
    // Note: This fails to apply masking using 0xffffffffL to the seed.
    // ************
    long hash = seed;
    final int nblocks = length >> 3;

    // body
    for (int i = 0; i < nblocks; i++) {
        final int index = (i << 3);
        long k = UnsafeAccess.toLong(data + index);
        k = Long.reverseBytes(k);
        // mix functions
        k *= C1;
        k = Long.rotateLeft(k, R1);
        k *= C2;
        hash ^= k;
        hash = Long.rotateLeft(hash, R2) * M + N1;
    }

    // tail
    long k1 = 0;
    final int index =  (nblocks << 3);
    switch (length - index) {
    case 7:
        k1 ^= ((long) UnsafeAccess.toByte(data + index + 6) & 0xff) << 48;
    case 6:
        k1 ^= ((long) UnsafeAccess.toByte(data + index + 5) & 0xff) << 40;
    case 5:
        k1 ^= ((long) UnsafeAccess.toByte(data + index + 4) & 0xff) << 32;
    case 4:
        k1 ^= ((long) UnsafeAccess.toByte(data + index + 3) & 0xff) << 24;
    case 3:
        k1 ^= ((long) UnsafeAccess.toByte(data + index + 2) & 0xff) << 16;
    case 2:
        k1 ^= ((long) UnsafeAccess.toByte(data + index + 1) & 0xff) << 8;
    case 1:
        k1 ^= ((long) UnsafeAccess.toByte(data + index ) & 0xff);
        k1 *= C1;
        k1 = Long.rotateLeft(k1, R1);
        k1 *= C2;
        hash ^= k1;
    }

    // finalization
    hash ^= length;
    hash = fmix64(hash);

    return hash;
}
  
  /**
   * Gets the little-endian long from 8 bytes starting at the specified index.
   *
   * @param data The data
   * @param index The index
   * @return The little-endian long
   */
  public static long getLittleEndianLong(final byte[] data, final int index) {
      return (((long) data[index    ] & 0xff)      ) |
             (((long) data[index + 1] & 0xff) <<  8) |
             (((long) data[index + 2] & 0xff) << 16) |
             (((long) data[index + 3] & 0xff) << 24) |
             (((long) data[index + 4] & 0xff) << 32) |
             (((long) data[index + 5] & 0xff) << 40) |
             (((long) data[index + 6] & 0xff) << 48) |
             (((long) data[index + 7] & 0xff) << 56);
  }

  /**
   * Performs the final avalanche mix step of the 64-bit hash function {@code MurmurHash3_x64_128}.
   *
   * @param hash The current hash
   * @return The final hash
   */
  private static long fmix64(long hash) {
      hash ^= (hash >>> 33);
      hash *= 0xff51afd7ed558ccdL;
      hash ^= (hash >>> 33);
      hash *= 0xc4ceb9fe1a85ec53L;
      hash ^= (hash >>> 33);
      return hash;
  }
  /**
   * Read memory as byte array
   *
   * @param ptr address
   * @param size size of a memory
   * @return byte array
   */
  public static byte[] toBytes(long ptr, int size) {
    byte[] buf = new byte[size];
    UnsafeAccess.copy(ptr, buf, 0, size);
    return buf;
  }

  public static String toHexString(long ptr, int size) {
    byte[] buf = new byte[size];
    UnsafeAccess.copy(ptr, buf, 0, size);
    return toHex(buf);
  }
  
  private static final char[] HEX_CHARS = {
      '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f'
    };

  /** Convert a byte range into a hex string */
  public static String toHex(byte[] b, int offset, int length) {
    int numChars = length * 2;
    char[] ch = new char[numChars];
    for (int i = 0; i < numChars; i += 2) {
      byte d = b[offset + i / 2];
      ch[i] = HEX_CHARS[(d >> 4) & 0x0F];
      ch[i + 1] = HEX_CHARS[d & 0x0F];
    }
    return new String(ch);
  }

  /** Convert a byte array into a hex string */
  public static String toHex(byte[] b) {
    return toHex(b, 0, b.length);
  }

  public static String toHex(long ptr, int size) {
    byte[] bytes = new byte[size];
    UnsafeAccess.copy(ptr, bytes, 0, size);
    return toHex(bytes);
  }
  /**
   * Generates random alphanumeric string
   *
   * @param r random generator
   * @param size size of a string to generate
   * @return string
   */
  public static String getRandomStr(Random r, int size) {
    int start = 'A';
    int stop = 'Z';
    StringBuffer sb = new StringBuffer(size);
    for (int i = 0; i < size; i++) {
      int v = r.nextInt(stop - start) + start;
      sb.append((char) v);
    }
    return sb.toString();
  }

 
  /**
   * Fills memory area with random data
   *
   * @param ptr pointer
   * @param size size of a memory area
   */
  public static void fillRandom(long ptr, int size) {
    byte[] arr = new byte[size];
    Random r = new Random();
    r.nextBytes(arr);
    UnsafeAccess.copy(arr, size, ptr, size);
  }

 
  public static String toString(double d, int afterDecimalPoint) {
    String s = Double.toString(d);
    int index = s.indexOf('.');
    if (index < 0) return s;
    if (index + 3 >= s.length()) return s;
    return s.substring(0, index + afterDecimalPoint + 1);
  }

  public static String format(String s, int wide) {
    if (s.length() >= wide) return s;
    int slen = s.length();
    for (int i = 0; i < wide - slen; i++) {
      s = "0" + s;
    }
    return s;
  }
  
  /** 
   * 
   * The total size of a K-V pair in the storage
   *
   **/
  public static int kvSize(int keySize, int valSize) {
    return keySize + valSize + Utils.sizeUVInt(valSize) + Utils.sizeUVInt(keySize);
  }
  
  /**
   * Safe version (no overflow)
   * @param keySize
   * @param valSize
   * @return serialized size of k-v
   */
  public static long kvSizeL(int keySize, int valSize) {
    return (long) keySize + valSize + Utils.sizeUVInt(valSize) + Utils.sizeUVInt(keySize);
  }
  /**
   * Get value offset in serialized key-value record
   * @param keySize key size
   * @param valSize value size
   * @return value offset
   */
  public static int valueOffset(int keySize, int  valSize) {
    return sizeUVInt(keySize) + sizeUVInt(valSize) + keySize;
  }
  
  /**
   * Get value size from raw value size (value size + value length representation)
   * @param rawSize 
   * @return value size
   */
  public static int getValueSizeFromRawSize(int rawSize) {
    for (int i = 1; i <= 6; i++) {
      if ((Utils.sizeUVInt(rawSize - i) + rawSize - i)  == rawSize) {
        return rawSize - i;
      }
    }
    return -1;
  }
  /**
   * Converts output stream to data output stream
   * @param os output stream
   * @return data output stream
   */
  public static DataOutputStream toDataOutputStream(OutputStream os) {
    if (os instanceof DataOutputStream) {
      return (DataOutputStream) os;
    }
    return new DataOutputStream(os);
  }
  
  /**
   * Converts input stream to data input stream
   * @param is input stream
   * @return data input stream
   */
  public static DataInputStream toDataInputStream(InputStream is) {
    if (is instanceof DataInputStream) {
      return (DataInputStream) is;
    }
    return new DataInputStream(is);
  }

  /**
   * Required size for K-V pair
   * @param keyLength key length
   * @param valueLength value length
   * @return size
   */
  public static int requiredSize(int keyLength, int valueLength) {
    return sizeUVInt(keyLength) + sizeUVInt(valueLength) + keyLength + valueLength;
  }
  
  /**
   * Reads item size from a memory address
   * 
   * @param ptr memory address
   * @return size
   */
  public static int getItemSize(long ptr) {
    int kSize = Utils.readUVInt(ptr);
    int kSizeSize = Utils.sizeUVInt(kSize);
    int vSize = Utils.readUVInt(ptr + kSizeSize);
    int vSizeSize = Utils.sizeUVInt(vSize);
    return kSize + kSizeSize + vSize + vSizeSize;
  }
  
  /**
   * Reads item size from a data page
   * 
   * @param page data page
   * @param off offset
   * @return size
   */
  public static int getItemSize(byte[] page, int off) {
    int kSize = Utils.readUVInt(page, off);
    int kSizeSize = Utils.sizeUVInt(kSize);
    int vSize = Utils.readUVInt(page, off + kSizeSize);
    int vSizeSize = Utils.sizeUVInt(vSize);
    return kSize + kSizeSize + vSize + vSizeSize;
  }
  
 
  /**
   * Get item key size 
   * 
   * @param ptr memory address
   * @return key size
   */
  public static int getKeySize(long ptr) {
    int kSize = Utils.readUVInt(ptr);
    return kSize;
  }
  
  /**
   * Get item key size 
   * 
   * @param buffer byte buffer
   * @return key size
   */
  public static int getKeySize(ByteBuffer buffer) {
    int kSize = Utils.readUVInt(buffer);
    return kSize;
  }
  
  /**
   * Get item key size 
   * 
   * @param buf buffer
   * @param offset offset in the buffer
   * @return key size
   */
  public static int getKeySize(byte[] buf, int offset) {
    int kSize = Utils.readUVInt(buf, offset);
    return kSize;
  }
  
  /**
   * Get item key offset 
   * 
   * @param ptr memory address
   * @return key offset
   */
  public static int getKeyOffset(long ptr) {
    int kSize = Utils.readUVInt(ptr);
    int kSizeSize = Utils.sizeUVInt(kSize);
    int vSize = Utils.readUVInt(ptr + kSizeSize);
    int vSizeSize = Utils.sizeUVInt(vSize);
    return kSizeSize + vSizeSize;
  }
  
  /**
   * Get item key offset 
   * 
   * @param page data page
   * @param off offset in a page
   * @return key offset
   */
  public static int getKeyOffset(byte[] page, int off) {
    int kSize = Utils.readUVInt(page, off);
    int kSizeSize = Utils.sizeUVInt(kSize);
    int vSize = Utils.readUVInt(page, off + kSizeSize);
    int vSizeSize = Utils.sizeUVInt(vSize);
    return kSizeSize + vSizeSize;
  }
  
  /**
   * Reads item size from a byte buffer
   * 
   * @param buffer byte buffer
   * @return size
   */
  public static int getItemSize(ByteBuffer buffer) {
    int pos = buffer.position();
    int kSize = Utils.readUVInt(buffer);
    int kSizeSize = Utils.sizeUVInt(kSize);
    buffer.position(pos + kSizeSize);
    int vSize = Utils.readUVInt(buffer);
    int vSizeSize = Utils.sizeUVInt(vSize);
    buffer.position(pos);
    return kSize + kSizeSize + vSize + vSizeSize;
  }
  
  /**
   * Reads key offset from a byte buffer
   * 
   * @param buffer byte buffer
   * @return offset
   */
  public static int getKeyOffset(ByteBuffer buffer) {
    int pos = buffer.position();
    int kSize = Utils.readUVInt(buffer);
    int kSizeSize = Utils.sizeUVInt(kSize);
    buffer.position(pos + kSizeSize);
    int vSize = Utils.readUVInt(buffer);
    int vSizeSize = Utils.sizeUVInt(vSize);
    buffer.position(pos);
    return kSizeSize + vSizeSize;
  }
  
  /**
   * Reads value offset from a byte buffer
   * 
   * @param buffer byte buffer
   * @return value offset
   */
  public static int getValueOffset(ByteBuffer buffer) {
    int pos = buffer.position();
    int kSize = Utils.readUVInt(buffer);
    int kSizeSize = Utils.sizeUVInt(kSize);
    buffer.position(pos + kSizeSize);
    int vSize = Utils.readUVInt(buffer);
    int vSizeSize = Utils.sizeUVInt(vSize);
    buffer.position(pos);
    
    return kSizeSize + vSizeSize + kSize;
  }
  
  /**
   * Reads value offset from a byte array
   * 
   * @param buffer byte array
   * @param off array offset
   * @return value offset
   */
  public static int getValueOffset(byte[] buffer, int off) {
    int kSize = Utils.readUVInt(buffer, off);
    int kSizeSize = Utils.sizeUVInt(kSize);
    off += kSizeSize;
    int vSize = Utils.readUVInt(buffer, off);
    int vSizeSize = Utils.sizeUVInt(vSize);    
    return kSizeSize + vSizeSize + kSize;
  }
  
  /**
   * Reads value offset from a memory address
   * 
   * @param ptr memory address
   * @return value offset
   */
  public static int getValueOffset(long ptr) {
    int kSize = Utils.readUVInt(ptr);
    int kSizeSize = Utils.sizeUVInt(kSize);
    ptr += kSizeSize;
    int vSize = Utils.readUVInt(ptr);
    int vSizeSize = Utils.sizeUVInt(vSize);    
    return kSizeSize + vSizeSize + kSize;
  }
  
  /**
   * Reads value size from a byte array
   * 
   * @param buffer byte array
   * @param off array offset
   * @return value size
   */
  public static int getValueSize(byte[] buffer, int off) {
    int kSize = Utils.readUVInt(buffer, off);
    int kSizeSize = Utils.sizeUVInt(kSize);
    int vSize = Utils.readUVInt(buffer, off + kSizeSize);
    return vSize;
  }
  
  /**
   * Reads value size from a memory address
   * 
   * @param ptr memory address
   * @return value size
   */
  public static int getValueSize(long ptr) {
    int kSize = Utils.readUVInt(ptr);
    int kSizeSize = Utils.sizeUVInt(kSize);
    int vSize = Utils.readUVInt(ptr + kSizeSize);
    return vSize;
  }
  
  /**
   * Reads value size from a byte buffer
   * 
   * @param buffer byte buffer
   * @return size
   */
  public static int getValueSize(ByteBuffer buffer) {
    int pos = buffer.position();
    int kSize = Utils.readUVInt(buffer);
    int kSizeSize = Utils.sizeUVInt(kSize);
    buffer.position(pos + kSizeSize);
    int vSize = Utils.readUVInt(buffer);
    buffer.position(pos);
    return vSize;
  }
  
  public static int extractValue(ByteBuffer buffer) {
    int pos = buffer.position();
    int kSize = Utils.readUVInt(buffer);
    int kSizeSize = Utils.sizeUVInt(kSize);
    buffer.position(pos + kSizeSize);
    int vSize = Utils.readUVInt(buffer);
    int vSizeSize = Utils.sizeUVInt(vSize);
    buffer.position(pos);

    int toMove = kSizeSize + vSizeSize + kSize;

    if (buffer.hasArray()) {
      byte[] array = buffer.array();
      System.arraycopy(array, pos + toMove, array, pos, vSize);
    } else {
      long ptr = UnsafeAccess.address(buffer);
      ptr += pos;
      UnsafeAccess.copy(ptr + toMove, ptr, vSize);
    }
    return vSize;
  }
  
  public static int extractValueRange(ByteBuffer buffer, int rangeStart, int rangeSize) {
    int pos = buffer.position();
    int kSize = Utils.readUVInt(buffer);
    int kSizeSize = Utils.sizeUVInt(kSize);
    buffer.position(pos + kSizeSize);
    int vSize = Utils.readUVInt(buffer);
    int vSizeSize = Utils.sizeUVInt(vSize);
    buffer.position(pos);

    int toMove = kSizeSize + vSizeSize + kSize + rangeStart;

    if (buffer.hasArray()) {
      byte[] array = buffer.array();
      System.arraycopy(array, pos + toMove, array, pos, rangeSize);
    } else {
      long ptr = UnsafeAccess.address(buffer);
      ptr += pos;
      UnsafeAccess.copy(ptr + toMove, ptr, rangeSize);
    }
    return rangeSize;
  }
  
  public static int extractValue(byte[] buffer, int off) {
    int pos = off;
    int kSize = Utils.readUVInt(buffer, pos);
    int kSizeSize = Utils.sizeUVInt(kSize);
    pos += kSizeSize;
    int vSize = Utils.readUVInt(buffer, pos);
    int vSizeSize = Utils.sizeUVInt(vSize);   
    int toMove = kSizeSize + vSizeSize + kSize;
    System.arraycopy(buffer, off + toMove, buffer, off, vSize);
    return vSize;
  }
  
  public static int extractValue(long buffer) {
    int pos = 0;
    int kSize = Utils.readUVInt(buffer + pos);
    int kSizeSize = Utils.sizeUVInt(kSize);
    pos += kSizeSize;
    int vSize = Utils.readUVInt(buffer + pos);
    int vSizeSize = Utils.sizeUVInt(vSize);   
    int toMove = kSizeSize + vSizeSize + kSize;
    UnsafeAccess.copy(buffer + toMove, buffer, vSize);
    return vSize;
  }
  
  public static int extractValueRange(byte[] buffer, int off, int rangeStart, int rangeSize) {
    int pos = off;
    int kSize = Utils.readUVInt(buffer, pos);
    int kSizeSize = Utils.sizeUVInt(kSize);
    pos += kSizeSize;
    int vSize = Utils.readUVInt(buffer, pos);
    int vSizeSize = Utils.sizeUVInt(vSize);   
    int toMove = kSizeSize + vSizeSize + kSize + rangeStart;
    System.arraycopy(buffer, off + toMove, buffer, off, rangeSize);
    return rangeSize;
  }
  
  public static int getJavaVersion() {
    String version = System.getProperty("java.version");
    if(version.startsWith("1.")) {
        version = version.substring(2, 3);
    } else {
        int dot = version.indexOf(".");
        if(dot != -1) { version = version.substring(0, dot); }
    } return Integer.parseInt(version);
  }
  
  
  public static void onSpinWait(long waitNs) {
    
    long start = System.nanoTime();
    
    while(System.nanoTime() - start < waitNs) {
      if (onSpinWait != null) {
        try {
          onSpinWait.invoke(null);
        } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
          e.printStackTrace();
        }
      } 
    }
  }
  
  public static long strToLong(byte[] buf, int off, int len) {
    long v = 0;
    int sign = 1;
    if (buf[off] == (byte) '-') {
      sign = -1;
      off++; len--;
    }
    for (int i = off; i < off + len; i++) {
      int d = buf[i] - (byte) '0';
      if (d < 0 || d > 9) {
        throw new NumberFormatException();
      }
      v = v * 10 + d; 
    }
    return sign * v;
  }
  
  
  public static int longToStr(byte[] buf, int off, long value) {
    int numDigits = 0;
    int sign = Long.signum(value);
    long v = sign * value;
    do { numDigits++; v /= 10; } while (v > 0);
    if (buf.length - off < numDigits + (sign < 0? 1: 0)) {
      throw new ArrayIndexOutOfBoundsException(numDigits);
    }
    
    if (sign < 0) {
      buf[off++] = (byte) '-';
    }
    value = sign * value;
    for (int i = 0; i < numDigits; i++) {
      int d = (int)(value % 10);
      buf[off + numDigits - 1 - i] = (byte)(d + '0');
      value /= 10;
    }
    return numDigits + (sign < 0? 1: 0);
  }
  
  public static long strToLongDirect(long ptr, int len) {
    long v = 0;
    int sign = 1;
    if (UnsafeAccess.toByte(ptr) == (byte) '-') {
      sign = -1;
      ptr++; len--;
    }
    for (int i = 0; i < len; i++) {
      int d = UnsafeAccess.toByte(ptr + i) - (byte) '0';
      if (d < 0 || d > 9) {
        throw new NumberFormatException();
      }
      v = v * 10 + d; 
    }
    return sign * v;
  }
  
  
  public static int longToStrDirect(long ptr, int avail, long value) {
    int numDigits = 0;
    int sign = Long.signum(value);
    long v = sign * value;
    do { numDigits++; v /= 10; } while (v > 0);
    if (avail < numDigits + + (sign < 0? 1: 0)) {
      throw new ArrayIndexOutOfBoundsException(numDigits);
    }
    
    if (sign < 0) {
      UnsafeAccess.putByte(ptr++, (byte) '-');
    }
    value = sign * value;
    for (int i = 0; i < numDigits; i++) {
      int d = (int)(value % 10);
      UnsafeAccess.putByte(ptr + numDigits - 1 - i, (byte)(d + '0'));
      value /= 10;
    }
    return numDigits + (sign < 0? 1: 0);
  }
  
  // TODO tests
  public static int sizeAsStr(long value) {
    int numDigits = 0;
    int sign = Long.signum(value);
    long v = sign * value;
    do { numDigits++; v /= 10; } while (v > 0);
    if (sign < 0) numDigits++;
    return numDigits;
  }
  
}
