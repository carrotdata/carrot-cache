/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.onecache.core.util;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

import org.junit.Test;

public class TestRollingWindowCounter {

  @Test
  public void testCounter() throws InterruptedException {
    // Every bin has 1 second duration
    RollingWindowCounter counter = new RollingWindowCounter(5, 5);
    counter.increment();
    counter.increment();
    counter.increment();

    assertEquals(3, counter.binCount(0));
    assertEquals(0, counter.binCount(1));
    assertEquals(0, counter.binCount(2));
    assertEquals(0, counter.binCount(3));
    assertEquals(0, counter.binCount(4));

    assertEquals(3, counter.count());

    Thread.sleep(1000);

    counter.increment();
    counter.increment();
    counter.increment();

    assertEquals(3, counter.binCount(0));
    assertEquals(3, counter.binCount(1));
    assertEquals(0, counter.binCount(2));
    assertEquals(0, counter.binCount(3));
    assertEquals(0, counter.binCount(4));

    assertEquals(6, counter.count());

    Thread.sleep(1000);

    counter.increment();
    counter.increment();
    counter.increment();

    assertEquals(3, counter.binCount(0));
    assertEquals(3, counter.binCount(1));
    assertEquals(3, counter.binCount(2));
    assertEquals(0, counter.binCount(3));
    assertEquals(0, counter.binCount(4));

    assertEquals(9, counter.count());

    Thread.sleep(1000);

    counter.increment();
    counter.increment();
    counter.increment();

    assertEquals(3, counter.binCount(0));
    assertEquals(3, counter.binCount(1));
    assertEquals(3, counter.binCount(2));
    assertEquals(3, counter.binCount(3));
    assertEquals(0, counter.binCount(4));

    assertEquals(12, counter.count());
    Thread.sleep(1000);

    counter.increment();
    counter.increment();
    counter.increment();

    assertEquals(3, counter.binCount(0));
    assertEquals(3, counter.binCount(1));
    assertEquals(3, counter.binCount(2));
    assertEquals(3, counter.binCount(3));
    assertEquals(3, counter.binCount(4));

    assertEquals(15, counter.count());
  }
  
  @Test
  public void testSaveLoad() throws IOException {
    // Every bin has 1 second duration
    RollingWindowCounter counter = new RollingWindowCounter(5, 5);
    counter.increment();
    counter.increment();
    counter.increment();
    
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);
    counter.save(dos);
    
    counter = new RollingWindowCounter();
    ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
    DataInputStream dis = new DataInputStream(bais);
    counter.load(dis);
    
    assertEquals(3, counter.binCount(0));
    assertEquals(0, counter.binCount(1));
    assertEquals(0, counter.binCount(2));
    assertEquals(0, counter.binCount(3));
    assertEquals(0, counter.binCount(4));

    assertEquals(3, counter.count());

    

  }
}