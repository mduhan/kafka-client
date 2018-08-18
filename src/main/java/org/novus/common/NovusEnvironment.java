package org.novus.common;

import static org.novus.common.NovusConstants.SCHEMA_REGISTRY_URL;

import org.apache.avro.generic.GenericRecord;
import org.novus.curator.NovusCuratorClient;
import org.novus.exception.NovusException;
import org.novus.kafka.consumer.NovusConsumerListener;
import org.novus.kafka.consumer.NovusKafkaConsumer;
import org.novus.kafka.consumer.factory.SchemaConsumerFactory;
import org.novus.kafka.consumer.factory.StringConsumerFactory;
import org.novus.kafka.producer.NovusKafkaTemplate;

/**
 * These are the different envirionment present in sintecmedia
 * 
 * @author Manjeet Duhan
 * @date Aug 19, 2018
 *
 */
public enum NovusEnvironment {

  LOCALHOST("localhost"), QA("qa"), STAGING("staging"), PRODUCTION("production"), JETTY("jetty"), DEFAULT(
      System.getProperty("novusEnv", "localhost"));

  String env;

  static NovusCuratorClient NOVUSCURATORCLIENT = new NovusCuratorClient();

  NovusEnvironment(String env) {
    this.env = env;
  }

  public String env() {
    return env;
  }

  /**
   * It will give schema registry template
   * 
   * @return NovusKafkaTemplate
   */
  public NovusKafkaTemplate<GenericRecord, GenericRecord> kafkaTemplate() {
    return NovusFactory.kafkaTemplate();
  }

  /**
   * It will give String and byte[] avro template
   * 
   * @return NovusKafkaTemplate
   */
  public NovusKafkaTemplate<String, String> kafkaTemplate(SchemaType schemaType) {
    if (schemaType.equals(SchemaType.STRING)) {
      return NovusFactory.kafkaTemplateString();
    } else if (schemaType.equals(SchemaType.BYTE)) {
      // no plans to implement simple byte avro
    }
    throw new IllegalArgumentException("SchemaType should have a valid value");
  }

  /**
   * It will give schema registry consumer
   * 
   * @return NovusKafkaConsumer
   */

  public NovusKafkaConsumer<Object, Object> kafkaConsumer(String consumerGroup) {

    String kafkaServers = NOVUSCURATORCLIENT.getBrokers();
    String schemaRegistryServers = System.getProperty(SCHEMA_REGISTRY_URL, "http://localhost:8081");
    return new SchemaConsumerFactory(kafkaServers, schemaRegistryServers, consumerGroup).consumer();
  }

  /**
   * It will give schema registry consumer
   * 
   * @param consumerGroup
   * @param consumerListener
   * @return
   */
  public NovusKafkaConsumer<Object, Object> kafkaConsumer(String consumerGroup,
      NovusConsumerListener<Object, Object> consumerListener) {

    String kafkaServers = NOVUSCURATORCLIENT.getBrokers();
    String schemaRegistryServers = System.getProperty(SCHEMA_REGISTRY_URL, "http://localhost:8081");
    return new SchemaConsumerFactory(kafkaServers, schemaRegistryServers, consumerGroup).consumer(consumerListener);
  }

  /**
   * It will give String and byte[] avro consumer
   * 
   * @param schemaType
   * @param consumerGroup
   * @return NovusKafkaConsumer
   * @throws NovusException
   */
  public NovusKafkaConsumer kafkaConsumer(SchemaType schemaType, String consumerGroup) {
    String kafkaServers = NOVUSCURATORCLIENT.getBrokers();
    String schemaRegistryServers = System.getProperty(SCHEMA_REGISTRY_URL, "http://localhost:8081");
    if (schemaType.equals(SchemaType.STRING)) {
      return new StringConsumerFactory(kafkaServers, schemaRegistryServers, consumerGroup).consumer();
    } else if (schemaType.equals(SchemaType.BYTE)) {

    }
    throw new NovusException("SchemaType should have a valid value");
  }

}
