package com.sintecmedia;

import java.io.IOException;

import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.novus.curator.DistributedLock;

/**
 *
 * @author Manjeet Duhan
 * @date Aug 19, 2018
 *
 */
@RunWith(JUnit4.class)
public class DistributedLockTest {

  public void lockTest() {
    DistributedLock distributedLock = null;
    try {
      distributedLock = new DistributedLock("lockName");

      // get the lock and noone can access this piece of code event across nodes
      distributedLock.lock();
    } catch (Exception e) {
      // TODO Handle exception
    } finally {
      try {
        distributedLock.unlock();
      } catch (IOException e) {
        // handle exception
      }
    }
  }
}
