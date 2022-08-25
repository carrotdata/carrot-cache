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
package com.carrot.cache.util;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
/** Utility class for network and file I/O related code */
public class IOUtils {

  /**
   * Drain byte buffer to a file channel
   *
   * @param buf byte buffer
   * @param fc file channel
   * @throws IOException
   */
  public static void drainBuffer(ByteBuffer buf, FileChannel fc) throws IOException {
    buf.flip();
    while (buf.hasRemaining()) {
      fc.write(buf);
    }
    buf.clear();
  }

  /**
   * Load no less than required number of bytes to a byte buffer
   *
   * @param fc file channel
   * @param buf byte buffer
   * @param required required number of bytes
   * @return available number of bytes
   * @throws IOException
   */
  public static long ensureAvailable(FileChannel fc, ByteBuffer buf, int required)
      throws IOException {
    int avail = buf.remaining();
    if (avail < required) {
      boolean compact = false;
      if (buf.capacity() - buf.position() < required) {
        buf.compact();
        compact = true;
      } else {
        buf.mark();
      }
      int n = 0;
      while (true) {
        n = fc.read(buf);
        if (n == -1) {
          if (avail == 0) {
            return -1;
          } // End-Of-Stream
          else {
            throw new IOException("Unexpected End-Of-Stream");
          }
        }
        avail += n;
        if (avail >= required) {
          if (compact) {
            buf.flip();
          } else {
            buf.reset();
          }
          break;
        }
      }
    }
    return avail;
  }
  /**
   * Reads data from a file into a buffer under lock
   * @param file file
   * @param fileOffset offset at a file
   * @param buffer buffer to read into
   * @param bufOffset offset at a buffer
   * @param len how many bytes to read
   * @throws IOException
   */
  public static void readFully(RandomAccessFile file, long fileOffset, byte[] buffer, int bufOffset, int len) 
      throws IOException {
    synchronized (file) {
      file.seek(fileOffset);
      file.readFully(buffer, bufOffset, len);
    }
  }

  /**
   * Reads data from a file into a buffer under lock
   *
   * @param file file
   * @param fileOffset offset at a file
   * @param buffer buffer to read into
   * @param len how many bytes to read
   * @throws IOException
   */
  public static void readFully(RandomAccessFile file, long fileOffset, ByteBuffer buffer, int len)
      throws IOException {
    synchronized (file) {
      if (buffer.hasArray()) {
        int pos = buffer.position();
        byte[] arr = buffer.array();
        readFully(file, fileOffset, arr, pos, len);
      } else {
        FileChannel fc = file.getChannel();
        fc.position(fileOffset);
        int read = 0;
        while (read < len) {
          read += fc.read(buffer);
        }
      }
    }
  }
}
  
    
