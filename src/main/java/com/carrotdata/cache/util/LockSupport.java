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

import java.util.concurrent.locks.ReentrantLock;

public class LockSupport {

  /* Global locks */
  private static ReentrantLock[] locks = new ReentrantLock[10007];

  static {
    // Initialize locks
    for (int i = 0; i < locks.length; i++) {
      locks[i] = new ReentrantLock();
    }
  }

  public static void lock(byte[] key, int keyOff, int keySize) {
    long hash = Math.abs(Utils.hash64(key, keyOff, keySize));
    ReentrantLock lock = locks[(int) (hash % locks.length)];
    lock.lock();
  }

  public static void unlock(byte[] key, int keyOff, int keySize) {
    long hash = Math.abs(Utils.hash64(key, keyOff, keySize));
    ReentrantLock lock = locks[(int) (hash % locks.length)];
    lock.unlock();
  }

  public static void lock(long keyPtr, int keySize) {
    long hash = Math.abs(Utils.hash64(keyPtr, keySize));
    ReentrantLock lock = locks[(int) (hash % locks.length)];
    lock.lock();
  }

  public static void unlock(long keyPtr, int keySize) {
    long hash = Math.abs(Utils.hash64(keyPtr, keySize));
    ReentrantLock lock = locks[(int) (hash % locks.length)];
    lock.unlock();
  }

}
