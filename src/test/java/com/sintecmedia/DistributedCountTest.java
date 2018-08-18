package com.sintecmedia;

import org.junit.Test;
import org.novus.common.NovusFactory;

/**
 * 
 * @author Manjeet Duhan
 * @date Aug 19, 2018
 *
 */
public class DistributedCountTest {

  @Test
  public void distributedCounterTest() {
    NovusFactory.novusCuratorClient().set("distributedCounter/childPath1", 20);

    System.out.println(NovusFactory.novusCuratorClient().get("distributedCounter/childPath1"));

    NovusFactory.novusCuratorClient().set("distributedCounter/childPath2", 50);

    System.out.println(NovusFactory.novusCuratorClient().counters("distributedCounter"));
  }
}
