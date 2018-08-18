package org.novus.kafka.consumer.factory;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.novus.kafka.consumer.NovusConsumerListener;
import org.novus.kafka.consumer.NovusKafkaConsumer;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;

/**
 * This is consumer for simple string data
 * 
 * @author Manjeet Duhan
 * @date Aug 19, 2018
 *
 */
public class StringConsumerFactory extends AbstractConsumerFactory<String, String> {

  private StringDeserializer stringKeyDeserializer;
  private StringDeserializer stringValueDeserializer;

  public StringConsumerFactory(String bootstrapServers, String schemaServers, String consumerGroup) {
    super(bootstrapServers, schemaServers, consumerGroup);
    stringKeyDeserializer = new StringDeserializer();
    stringKeyDeserializer.configure(consumerConfigs(props), true);
    stringValueDeserializer = new StringDeserializer();
    stringValueDeserializer.configure(consumerConfigs(props), false);
  }

  @Override
  protected ConsumerFactory<String, String> consumerFactory() {
    return new DefaultKafkaConsumerFactory<>(props, stringKeyDeserializer, stringValueDeserializer);
  }

  @Override
  public NovusKafkaConsumer<String, String> consumer() {
    return new NovusKafkaConsumer<>(props, stringKeyDeserializer, stringValueDeserializer);
  }

  @Override
  public NovusKafkaConsumer<String, String> consumer(NovusConsumerListener<String, String> consumerListener) {
    return new NovusKafkaConsumer<>(props, stringKeyDeserializer, stringValueDeserializer, consumerListener);
  }

}
