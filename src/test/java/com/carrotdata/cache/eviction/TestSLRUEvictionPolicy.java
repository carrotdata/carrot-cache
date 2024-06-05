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
