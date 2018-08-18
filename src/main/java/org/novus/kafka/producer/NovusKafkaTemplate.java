package org.novus.kafka.producer;

import java.util.List;
import java.util.Map;

import org.novus.domain.NovusRecord;
import org.novus.exception.NovusException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

/**
 * This is customized novus template to send message to kafka
 * 
 * @author Manjeet Duhan
 * @date Aug 19, 2018
 *
 *       kafkaTemplate().push(topic, key,value);
 * @param <K>
 * @param <V>
 */
public class NovusKafkaTemplate<K, V> extends KafkaTemplate<K, V> {

  private static final Logger log = LoggerFactory.getLogger(NovusKafkaTemplate.class);

  public NovusKafkaTemplate(ProducerFactory<K, V> producerFactory) {
    super(producerFactory);
  }

  public void push(String topic, K key, V data) {
    super.send(topic, key, data);
    try {
      super.flush();
    } catch (Exception error) {
      log.error("Error while commiting the record for topic " + topic + " data: " + data, error);
      throw new NovusException("Error while commiting the record for topic " + topic + " data: " + data, error);
    }
  }

  /**
   * Push Bulk record for topic
   * 
   * @param topic
   * @param bulkRecord
   * 
   */
  public void push(String topic, Map<K, V> bulkRecord) {
    bulkRecord.entrySet().stream().forEach(entry -> {
      super.send(topic, entry.getKey(), entry.getValue());
    });
    try {

      super.flush();
    } catch (Exception error) {
      log.error("Error while commiting the record for topic " + topic, error);
      throw new NovusException("Error while commiting the bulk records", error);
    }

  }

  /**
   * Push Bulk record for topic
   * 
   * @param topic
   * @param bulkRecord
   * 
   */
  public void push(String topic, List<NovusRecord<K, V>> bulkRecord) {
    bulkRecord.stream().forEach(novusRecord -> {
      super.send(topic, novusRecord.getKey(), novusRecord.getValue());
    });
    try {
      super.flush();
    } catch (Exception error) {
      log.error("Error while commiting the record for topic " + topic, error);
      throw new NovusException("Error while commiting the bulk records", error);
    }

  }

}
