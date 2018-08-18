package org.novus.kafka.consumer.factory;

import org.novus.kafka.consumer.NovusConsumerListener;
import org.novus.kafka.consumer.NovusKafkaConsumer;
import org.springframework.kafka.core.ConsumerFactory;

/**
 * 
 * This is avro consumer which will return data in bytes[]
 * 
 * @author Manjeet Duhan
 * @date Aug 19, 2018
 *
 */
public class AvroConsumerFactory extends AbstractConsumerFactory<byte[], byte[]> {

  public AvroConsumerFactory(String bootstrapServers, String schemaServers, String consumerGroup) {
    super(bootstrapServers, schemaServers, consumerGroup);
  }

  @Override
  protected ConsumerFactory<byte[], byte[]> consumerFactory() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public NovusKafkaConsumer<byte[], byte[]> consumer() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public NovusKafkaConsumer<byte[], byte[]> consumer(NovusConsumerListener<byte[], byte[]> consumerListener) {
    // TODO Auto-generated method stub
    return null;
  }

}
