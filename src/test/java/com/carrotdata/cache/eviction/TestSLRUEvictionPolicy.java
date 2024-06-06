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

public class TestSLRUEvictionPolicy {

  @Test
  public void testPolicy() {
    EvictionPolicy policy = new SLRUEvictionPolicy();

    assertEquals(EvictionPolicy.Action.PROMOTE, policy.actionOnHit());

    assertEquals(50, policy.getInsertIndex(0, 100));
    assertEquals(100, policy.getInsertIndex(0, 200));
    assertEquals(150, policy.getInsertIndex(0, 300));
    assertEquals(200, policy.getInsertIndex(0, 400));
    assertEquals(250, policy.getInsertIndex(0, 500));

    assertEquals(99, policy.getEvictionCandidateIndex(0, 100));
    assertEquals(999, policy.getEvictionCandidateIndex(0, 1000));

    assertEquals(0, policy.getPromotionIndex(0, 10, 1000));
    assertEquals(0, policy.getPromotionIndex(0, 100, 1000));
    assertEquals(0, policy.getPromotionIndex(0, 200, 1000));
    assertEquals(125, policy.getPromotionIndex(0, 300, 1000));
    assertEquals(250, policy.getPromotionIndex(0, 400, 1000));
    assertEquals(375, policy.getPromotionIndex(0, 500, 1000));
    assertEquals(375, policy.getPromotionIndex(0, 600, 1000));
    assertEquals(500, policy.getPromotionIndex(0, 700, 1000));
    assertEquals(625, policy.getPromotionIndex(0, 800, 1000));
    assertEquals(750, policy.getPromotionIndex(0, 900, 1000));

  }
}
