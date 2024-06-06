/*
 * Copyright (C) 2024-present Carrot Data, Inc. 
 * <p>This program is free software: you can redistribute it
 * and/or modify it under the terms of the Server Side Public License, version 1, as published by
 * MongoDB, Inc.
 * <p>This program is distributed in the hope that it will be useful, but WITHOUT ANY
 * WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR
 * PURPOSE. See the Server Side Public License for more details. 
 * <p>You should have received a copy of the Server Side Public License along with this program. If not, see
 * <http://www.mongodb.com/licensing/server-side-public-license>.
 */
package com.carrotdata.cache.util;

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
