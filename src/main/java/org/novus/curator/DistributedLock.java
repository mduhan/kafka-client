package org.novus.curator;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs.Ids;
import org.apache.zookeeper.ZooKeeper;
import org.novus.common.NovusFactory;

/**
 * This class provided APIs to get distributed lock for multithreaded environments like connectors
 * 
 * DistributedLock distributedLock = NovusFactory.getLockObject("/basepath","lockname"));
 * distributedLock.lock(); Do Processing distributedLock.unlock();
 * 
 * @author Manjeet Duhan
 * @date Aug 19, 2018
 *
 */
public class DistributedLock {
  private final ZooKeeper zk;
  private final String lockBasePath;
  private final String lockName;
  private String lockPath;
  final Object lock = new Object();

  public DistributedLock(String lockBasePath, String lockName) throws Exception {
    this.zk = NovusFactory.novusCuratorClient().getCuratorClient().getZookeeperClient().getZooKeeper();
    this.lockBasePath = lockBasePath;
    this.lockName = lockName;
  }

  public DistributedLock(String lockName) throws Exception {
    this("/zookeeper", lockName);
  }

  /**
   * API to get lock on zookeeper lock
   * 
   * @throws Exception
   */
  public void lock() throws Exception {
    try {
      String path = lockBasePath + "/" + lockName;
      NovusFactory.novusCuratorClient().getCuratorClient().createContainers(path);
      lockPath = zk.create(path + "/", null, Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);

      synchronized (lock) {
        while (true) {
          List<String> nodes = zk.getChildren(path, new Watcher() {
            @Override
            public void process(WatchedEvent event) {
              synchronized (lock) {
                lock.notifyAll();
              }
            }
          });
          Collections.sort(nodes);
          if (lockPath.endsWith(nodes.get(0))) {
            return;
          } else {
            lock.wait();
          }
        }
      }
    } catch (KeeperException e) {
      throw new IOException(e);
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }

  /**
   * 
   * release lock
   * 
   * @throws IOException
   */
  public void unlock() throws IOException {
    try {
      zk.delete(lockPath, -1);
      lockPath = null;
      deleteLock();
    } catch (KeeperException e) {
      throw new IOException(e);
    } catch (InterruptedException e) {
      throw new IOException(e);
    }
  }
  
  /**
   * 
   * delete persistent node for lock
   * 
   * @throws IOException
   */
  public void deleteLock() throws IOException {
    try {
      zk.delete(lockBasePath+"/"+lockName, -1);
    } catch (KeeperException e) {
      // do nothing
    } catch (InterruptedException e) {
      // do nothing
    }
  }

}