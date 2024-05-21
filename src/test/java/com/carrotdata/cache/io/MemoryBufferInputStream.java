/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.carrotdata.cache.io;


import java.io.IOException;
import java.io.InputStream;
import java.util.Random;

import com.carrotdata.cache.util.UnsafeAccess;


public class MemoryBufferInputStream extends InputStream
{
  private long ptr;
  
  private long position;
  
  private final long length;
  
  private boolean closed;
  

    public MemoryBufferInputStream(long length)
    {
        this(length, true);
    }
    
    public MemoryBufferInputStream(long length, boolean init) {
      this.length = length;
      this.ptr = UnsafeAccess.malloc(length);
      if (init) {
        initMemoryBufferRandom();
      }
    }
    
    private void initMemoryBufferRandom() {
      byte[] buf = new byte[1024];
      long offset = 0;
      Random r = new Random();
      while (offset < this.length) {
        r.nextBytes(buf);
        int toCopy = (int) Math.min(buf.length, this.length - offset);
        UnsafeAccess.copy(buf, 0, this.ptr + offset, toCopy);
        offset += toCopy;
      }
    }

    @Override
    public int read()
    {   
      if (this.position == this.length) {
          return -1; // EOF;
      }
      return UnsafeAccess.toByte(this.position++) & 0xff;
    }

    @Override
    public int available() throws IOException {
      return (int) (this.length - this.position);
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      if (this.position == this.length) {
        return -1; // EOF
      }
      int toRead = (int) Math.min(this.length - this.position, len);
      UnsafeAccess.copy(this.ptr + this.position, b, off, toRead);
      this.position += toRead;
      return toRead;
    }

    @Override
    public int read(byte[] b) throws IOException {
      return read(b, 0, b.length);
    }

    @Override
    public long skip(long n) throws IOException {
      long toSkip = Math.min(this.length - this.position, n);
      this.position += toSkip;
      return toSkip;
    }

    @Override    
    public void close()
        throws IOException
    {
      if (this.closed) {
        return;
      }
      this.closed = true;
      UnsafeAccess.free(this.ptr);  
    }
    
    /**
     * Return length (size) of the memory buffer
     * @return length
     */
    public long length() {
      return this.length;
    }
    /**
     * Return memory buffer address
     * @return address
     */
    public long bufferAddress() {
      return this.ptr;
    }

    @Override
    public void reset() throws IOException {
      this.position = 0;
    }
    
}
