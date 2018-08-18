package org.novus.kafka.producer.factory;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.Serializer;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.ProducerFactory;

import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;

/**
 * 
 * This is base producer factory for all type of producers
 * 
 * @author Manjeet Duhan
 * @date Aug 19, 2018
 *
 * @param <K>
 * @param <V>
 */
public abstract class AbstractProducerKafkaFactory<K, V> {

  Map<String, Object> props = new HashMap<>();

  public AbstractProducerKafkaFactory(String bootstrapServers, String schemaServers) {
    props.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaServers);
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
    props.put(ProducerConfig.BATCH_SIZE_CONFIG, System.getProperty("batch.size", "1000"));
  }

  protected Map<String, Object> producerConfigs(Map<String, Object> props) {

    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, props.get(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG));
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, props.get(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG));
    return props;
  }

  protected ProducerFactory<K, V> producerFactory(Serializer avroKeySerializer, Serializer avroValueSerializer) {
    return new DefaultKafkaProducerFactory<>(props, avroKeySerializer, avroValueSerializer);
  }

  protected ProducerFactory<K, V> producerFactory() {
    return new DefaultKafkaProducerFactory<>(props);
  }

}
