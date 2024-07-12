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
package com.carrotdata.cache.eviction;

import static org.junit.Assert.assertEquals;

import org.junit.Test;

public class TestFIFOEvictionPolicy {

  @Test
  public void testPolicy() {
    EvictionPolicy policy = new FIFOEvictionPolicy();

    assertEquals(EvictionPolicy.Action.DELETE, policy.actionOnHit());
    assertEquals(0, policy.getInsertIndex(0, 0));
    assertEquals(99, policy.getEvictionCandidateIndex(0, 100));
    assertEquals(0, policy.getPromotionIndex(0, 0, 0));
  }
}