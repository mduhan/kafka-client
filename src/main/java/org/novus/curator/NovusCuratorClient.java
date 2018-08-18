package org.novus.curator;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.atomic.DistributedAtomicLong;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.novus.common.NovusConstants;
import org.novus.common.NovusFactory;
import org.novus.exception.NovusException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * 
 * This class provides basic wrapper over curator recipe and apis to store and get data from zookeeper
 * 
 * @author Manjeet Duhan
 * @date Aug 19, 2018
 *
 */
public class NovusCuratorClient {

  private static final Logger log = LoggerFactory.getLogger(NovusCuratorClient.class);

  CuratorFramework curatorClient;

  private final String basePath = "/novus/";

  ObjectMapper mapper = new ObjectMapper();

  public NovusCuratorClient() {
    String connectString = System.getProperty(NovusConstants.ZOOKEEPER_URL, "localhost:2181");
    RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
    this.curatorClient = CuratorFrameworkFactory.newClient(connectString, retryPolicy);
    curatorClient.start();
  }

  /**
   * Delete node in zookeeper
   * 
   * @param pathId
   */
  public void delete(String pathId) {
    String zookeeperPathId = basePath + pathId;
    try {
      curatorClient.delete().deletingChildrenIfNeeded().forPath(zookeeperPathId);
    } catch (Exception e) {
      log.error("Error while deleting zookeeper node {} error {} ", pathId, e.getMessage());
      throw new NovusException("Error while deleting zookeeper node " + pathId, e);
    }

  }
  
  /**
   * Delete node in zookeeper
   * 
   * @param pathId
   */
  public void deleteJobStatus(String pathId) {
    String zookeeperPathId = basePath + pathId;
    try {
      curatorClient.delete().deletingChildrenIfNeeded().forPath(zookeeperPathId+NovusConstants.INITIAL_PATH);
      curatorClient.delete().deletingChildrenIfNeeded().forPath(zookeeperPathId+NovusConstants.PROGRESS_PATH);
    } catch (Exception e) {
      log.error("Error while deleting zookeeper jobstatus node {} error {} ", pathId, e.getMessage());
      throw new NovusException("Error while deleting zookeeper jobstatus node " + pathId, e);
    }

  }

  /**
   * append distributed long count
   * 
   * @param pathId
   * @param count
   * @return long
   */
  public long addInitial(String pathId, long count) {
    String zookeeperPathId = basePath + pathId+NovusConstants.INITIAL_PATH;
    RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
    DistributedAtomicLong atomic = new DistributedAtomicLong(curatorClient, zookeeperPathId, retryPolicy);
    try {
      return atomic.add(count).postValue();
    } catch (Exception e) {
      log.error("Error while adding count to zookeeper node {} error {}", pathId, e.getMessage());
      throw new NovusException("Error while adding count to zookeeper node " + pathId, e);
    }
  }
  
  /**
   * append distributed long count
   * 
   * @param pathId
   * @param count
   * @return long
   */
  public long addProgress(String pathId, long count) {
    String zookeeperPathId = basePath + pathId+NovusConstants.PROGRESS_PATH;
    RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
    DistributedAtomicLong atomic = new DistributedAtomicLong(curatorClient, zookeeperPathId, retryPolicy);
    try {
      return atomic.add(count).postValue();
    } catch (Exception e) {
      log.error("Error while adding count to zookeeper node {} error {}", pathId, e.getMessage());
      throw new NovusException("Error while adding count to zookeeper node " + pathId, e);
    }
  }
  
  /**
   * set distributed long count
   * 
   * @param pathId
   * @param count
   * @return long
   */
  public long set(String pathId, long count) {
    String zookeeperPathId = basePath + pathId;
    RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
    DistributedAtomicLong atomic = new DistributedAtomicLong(curatorClient, zookeeperPathId, retryPolicy);
    try {
      return atomic.trySet(count).postValue();
    } catch (Exception e) {
      log.error("Error while setting count to zookeeper node {} error {}", pathId, e.getMessage());
      throw new NovusException("Error while setting count to zookeeper node " + pathId, e);
    }
  }

  /**
   * Get distributed long count
   * 
   * @param pathId
   * @return longs
   */
  public long get(String pathId) {
    String zookeeperPathId = basePath + pathId;
    RetryPolicy retryPolicy = new ExponentialBackoffRetry(1000, 3);
    DistributedAtomicLong atomic = new DistributedAtomicLong(curatorClient, zookeeperPathId, retryPolicy);
    try {
      return atomic.get().preValue();
    } catch (Exception e) {
      log.error("Error while getting count from zookeeper node pathId {} error {} " + pathId, e.getMessage());
      throw new NovusException("Error while getting count from zookeeper node " + pathId, e);
    }
  }

  public CuratorFramework getCuratorClient() {
    return curatorClient;
  }

  /**
   * Get distributed long count for all children within this path
   * 
   * @param pathId
   * @return longs
   */
  public Map<String, Long> counters(String pathId) {
    String zookeeperPathId = basePath + pathId;
    Map<String, Long> counterData = new HashMap<>();

    try {
      List<String> list = NovusFactory.novusCuratorClient().curatorClient.getZookeeperClient().getZooKeeper()
          .getChildren(zookeeperPathId, null);
      list.forEach(data -> {
        counterData.put(data, NovusFactory.novusCuratorClient().get(pathId + "/" + data));
      });
      return counterData;
    } catch (Exception e) {
      log.error("Error while getting count from zookeeper node pathId {} error {} " + pathId, e.getMessage());
      throw new NovusException("Error while getting count from zookeeper node " + pathId, e);
    }
  }

  /**
   * Get list of brokers
   * 
   * @return String
   */
  public String getBrokers() {
    StringBuilder builder = new StringBuilder();
    List<String> ids = null;
    try {
      ids = curatorClient.getZookeeperClient().getZooKeeper().getChildren("/brokers/ids", false);
      if (ids.isEmpty()) {
        throw new NovusException("Kafka brokers are down");
      }
      for (String id : ids) {
        String brokerInfoString = null;
        brokerInfoString = new String(
            curatorClient.getZookeeperClient().getZooKeeper().getData("/brokers/ids/" + id, false, null));
        Map<String, Object> map = mapper.readValue(brokerInfoString, Map.class);
        builder.append((map.get("host") + ":" + map.get("port")));
        builder.append(",");
      }

    } catch (NovusException e) {
      throw e;
    } catch (Exception e) {
      throw new NovusException("Zookeeper is down", e);
    }
    return builder.deleteCharAt(builder.length() - 1).toString();
  }

}
