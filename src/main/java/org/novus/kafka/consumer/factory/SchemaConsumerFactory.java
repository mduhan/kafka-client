package org.novus.kafka.consumer.factory;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.novus.kafka.consumer.NovusConsumerListener;
import org.novus.kafka.consumer.NovusKafkaConsumer;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

/**
 * This is schema registry based consumer which will consume and format schema registry supported avro message
 * 
 * @author Manjeet Duhan
 * @date Aug 19, 2018
 *
 */
public class SchemaConsumerFactory extends AbstractConsumerFactory<Object, Object> {

  private KafkaAvroDeserializer avroKeyDeserializer;

  private KafkaAvroDeserializer avroValueDeserializer;

  public SchemaConsumerFactory(String bootstrapServers, String schemaServers, String consumerGroup) {
    super(bootstrapServers, schemaServers, consumerGroup);
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
    avroKeyDeserializer = new KafkaAvroDeserializer();
    avroKeyDeserializer.configure(props, true);
    avroValueDeserializer = new KafkaAvroDeserializer();
    avroValueDeserializer.configure(props, false);
  }

  @Override
  protected ConsumerFactory<Object, Object> consumerFactory() {
    return new DefaultKafkaConsumerFactory<>(props, avroKeyDeserializer, avroValueDeserializer);
  }

  @Override
  public NovusKafkaConsumer<Object, Object> consumer() {
    return new NovusKafkaConsumer<>(props, avroKeyDeserializer, avroValueDeserializer);
  }

  @Override
  public NovusKafkaConsumer<Object, Object> consumer(NovusConsumerListener<Object, Object> consumerListener) {
    return new NovusKafkaConsumer<>(props, avroKeyDeserializer, avroValueDeserializer, consumerListener);
  }

}
