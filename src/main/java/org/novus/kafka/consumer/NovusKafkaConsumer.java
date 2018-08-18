package org.novus.kafka.consumer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;

/**
 * This is customized wrapper to read data from kafka
 * 
 * @author Manjeet Duhan
 * @date Aug 19, 2018
 *
 * @param <K>
 * @param <V>
 */
public class NovusKafkaConsumer<K, V> extends KafkaConsumer<K, V> {

  final long timeout = Long.parseLong(System.getProperty("kakfa.consumer.timeout", "20000"));

  Thread consumer;

  public NovusKafkaConsumer(Map<String, Object> configs, Deserializer<K> keyDeserializer,
      Deserializer<V> valueDeserializer) {
    super(configs, keyDeserializer, valueDeserializer);
  }

  public NovusKafkaConsumer(Map<String, Object> configs, Deserializer<K> keyDeserializer,
      Deserializer<V> valueDeserializer, NovusConsumerListener<K, V> consumerListener) {
    super(configs, keyDeserializer, valueDeserializer);
    this.subscribe(consumerListener.getTopics());
    consumer = new Thread(new ConsumerThread<>(consumerListener, this));
    consumer.start();
  }

  /**
   * Get records for list of partition for given topic
   * 
   * @param topic
   * @param partitions
   * @return
   */
  public ConsumerRecords<K, V> seekToBeginning(String topic, List<Integer> partitions) {
    List<TopicPartition> topicPartitions = new ArrayList<>();
    partitions.forEach(partition -> {
      TopicPartition topicPartition = new TopicPartition(topic, partition);
      topicPartitions.add(topicPartition);
    });
    this.assign(topicPartitions);
    this.seekToBeginning(topicPartitions);
    return this.poll(timeout);
  }

  /**
   * Get records for one partition for given topic
   * 
   * @param topic
   * @param partition
   * @return
   */
  public ConsumerRecords<K, V> seekToBeginning(String topic, Integer partition) {
    List<TopicPartition> topicPartitions = new ArrayList<>();
    topicPartitions.add(new TopicPartition(topic, partition));
    this.assign(topicPartitions);
    this.seekToBeginning(topicPartitions);
    return this.poll(timeout);
  }

  /**
   * Get records for given topic
   * 
   * @param topic
   * @return
   */
  public ConsumerRecords<K, V> seekToBeginning(String topic) {

    List<Integer> partitions = new ArrayList<>();
    this.partitionsFor(topic).forEach(partitionInfo -> {
      partitions.add(partitionInfo.partition());
    });
    return this.seekToBeginning(topic, partitions);
  }

  /**
   * Get record by offset
   * 
   * @param topic
   * @param partition
   * @param offset
   * @return
   */
  public ConsumerRecords<K, V> seek(String topic, Integer partition, long offset) {
    this.seek(new TopicPartition(topic, partition), offset);
    return this.poll(timeout);
  }

  /**
   * Get record for range of offset
   * 
   * @param topic
   * @param partition
   * @param fromOffset
   * @param toOffset
   * @return
   */
  public ConsumerRecords<K, V> seek(String topic, Integer partition, long fromOffset, long toOffset) {
    assert fromOffset < toOffset : "fromOffset should be less than toOffset";
    assert toOffset - fromOffset > 100 : "Difference between toOffset and fromOffset should be less than 100";
    TopicPartition topicPartition = new TopicPartition(topic, partition);
    List<ConsumerRecord<K, V>> consumerRecords = new ArrayList<>();
    Map<TopicPartition, List<ConsumerRecord<K, V>>> records = new HashMap<>();
    records.put(topicPartition, consumerRecords);
    this.assign(Arrays.asList(topicPartition));
    while (fromOffset < toOffset) {
      this.seek(topicPartition, fromOffset);
      ConsumerRecords<K, V> record = seek(topic, partition, fromOffset);
      consumerRecords.add(record.iterator().next());
      fromOffset++;
    }
    return new ConsumerRecords<>(records);

  }

}
