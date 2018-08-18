package org.novus.kafka.producer.factory;

import java.util.HashMap;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.novus.kafka.producer.NovusKafkaTemplate;

import io.confluent.kafka.serializers.KafkaAvroSerializer;

/**
 * This is schema registry based producer which will post data to kafka in schema registry supported avro type
 * 
 * @author Manjeet Duhan
 * @date Aug 19, 2018
 *
 */
public class ProducerSchemaRegistryFactory extends AbstractProducerKafkaFactory<GenericRecord, GenericRecord> {

  public ProducerSchemaRegistryFactory(String bootstrapServers, String schemaServers) {
    super(bootstrapServers, schemaServers);
  }

  private Map<String, Object> producerConfigs() {
    Map<String, Object> props = new HashMap<>();
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
    super.producerConfigs(props);
    return props;
  }

  public NovusKafkaTemplate<GenericRecord, GenericRecord> template() {
    producerConfigs();
    KafkaAvroSerializer avroKeySerializer = new KafkaAvroSerializer();
    avroKeySerializer.configure(props, true);
    KafkaAvroSerializer avroValueSerializer = new KafkaAvroSerializer();
    avroValueSerializer.configure(props, false);
    return new NovusKafkaTemplate<GenericRecord, GenericRecord>(
        producerFactory(avroKeySerializer, avroValueSerializer));
  }
}
