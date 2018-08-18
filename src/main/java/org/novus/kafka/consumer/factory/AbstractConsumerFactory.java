package org.novus.kafka.consumer.factory;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.novus.kafka.consumer.NovusConsumerListener;
import org.novus.kafka.consumer.NovusKafkaConsumer;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;

import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;

/**
 * This is novus base class for common process for all type of kafka consumers
 * 
 * @author Manjeet Duhan
 * @date Aug 19, 2018
 *
 * @param <K>
 * @param <V>
 */
public abstract class AbstractConsumerFactory<K, V> {

  Map<String, Object> props = new HashMap<>();

  public AbstractConsumerFactory(String bootstrapServers, String schemaServers, String consumerGroup) {
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaServers);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, consumerGroup);
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, System.getProperty("max.poll.records", "500"));
  }

  protected Map<String, Object> consumerConfigs(Map<String, Object> props) {

    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, props.get(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG));
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, props.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG));

    return props;
  }

  protected abstract ConsumerFactory<K, V> consumerFactory();

  private ConcurrentKafkaListenerContainerFactory<K, V> kafkaListenerContainerFactory() {
    ConcurrentKafkaListenerContainerFactory<K, V> factory = new ConcurrentKafkaListenerContainerFactory<>();
    factory.setConsumerFactory(consumerFactory());
    return factory;
  }

  public abstract NovusKafkaConsumer<K, V> consumer();

  public abstract NovusKafkaConsumer<K, V> consumer(NovusConsumerListener<K, V> consumerListener);

}
