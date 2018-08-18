package org.novus.kafka.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecords;

/**
 * This is always running thread which keeps on polling message from kafka and will push data to registered
 * consumers
 * 
 * @author Manjeet Duhan
 * @date Aug 19, 2018
 *
 * @param <K>
 * @param <V>
 */
public class ConsumerThread<K, V> implements Runnable {

  final long timeout = Long.parseLong(System.getProperty("kakfa.consumer.timeout", "20000"));

  NovusConsumerListener<K, V> consumerListener;

  NovusKafkaConsumer<K, V> consumer;

  public ConsumerThread(NovusConsumerListener<K, V> consumerListener, NovusKafkaConsumer<K, V> consumer) {
    this.consumerListener = consumerListener;
    this.consumer = consumer;
  }

  /**
   * This method will start putting data to consumer
   */
  @Override
  public void run() {
    while (true) {
      ConsumerRecords<K, V> comsumerRecords = consumer.poll(timeout);
      consumerListener.put(comsumerRecords);
      consumer.commitAsync();
    }
  }

}
