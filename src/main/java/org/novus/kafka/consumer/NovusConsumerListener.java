package org.novus.kafka.consumer;

import java.util.Collection;

import org.apache.kafka.clients.consumer.ConsumerRecords;

/**
 * 
 * This interface has to be implemented by consumers to get data from kafka
 * 
 * @author Manjeet Duhan
 * @date Aug 19, 2018
 *
 * @param <K>
 * @param <V>
 */
public abstract class NovusConsumerListener<K, V> {

  Collection<String> topics;

  protected NovusConsumerListener(Collection<String> topics) {
    this.topics = topics;
  }

  public abstract void put(ConsumerRecords<K, V> comsumerRecords);

  public Collection<String> getTopics() {
    return topics;
  }
}
