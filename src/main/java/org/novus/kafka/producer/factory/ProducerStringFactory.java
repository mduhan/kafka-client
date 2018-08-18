package org.novus.kafka.producer.factory;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.novus.kafka.producer.NovusKafkaTemplate;

/**
 * This is string based producer which will post data to kafka
 * 
 * @author Manjeet Duhan
 * @date Aug 19, 2018
 *
 */
public class ProducerStringFactory extends AbstractProducerKafkaFactory<String, String> {

  public ProducerStringFactory(String bootstrapServers, String schemaServers) {
    super(bootstrapServers, schemaServers);
  }

  private Map<String, Object> producerConfigs() {
    Map<String, Object> props = new HashMap<>();
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
    super.producerConfigs(props);
    return props;
  }

  public NovusKafkaTemplate<String, String> template() {
    producerConfigs();
    StringSerializer stringKeySerializer = new StringSerializer();
    stringKeySerializer.configure(props, true);
    StringSerializer stringValueSerializer = new StringSerializer();
    stringValueSerializer.configure(props, false);
    return new NovusKafkaTemplate<String, String>(producerFactory(stringKeySerializer, stringValueSerializer));
  }
}
