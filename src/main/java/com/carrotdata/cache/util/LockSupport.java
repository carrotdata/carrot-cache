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
