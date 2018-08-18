package org.novus.common;

import static org.novus.common.NovusConstants.SCHEMA_REGISTRY_URL;

import org.apache.avro.generic.GenericRecord;
import org.novus.curator.NovusCuratorClient;
import org.novus.kafka.producer.NovusKafkaTemplate;
import org.novus.kafka.producer.factory.ProducerSchemaRegistryFactory;
import org.novus.kafka.producer.factory.ProducerStringFactory;

/**
 * 
 * This class is responsible to create singlton instance of required classes in novus client
 * 
 * @author Manjeet Duhan
 * @date Aug 19, 2018
 *
 */
public class NovusFactory {

  static NovusCuratorClient NOVUSCURATORCLIENT;
  static NovusKafkaTemplate<GenericRecord, GenericRecord> NOVUSKAFKATEMPLATE_SCHEMAREGISTRY;
  static NovusKafkaTemplate<String, String> NOVUSKAFKATEMPLATE_STRING;

  static {
    NOVUSCURATORCLIENT = new NovusCuratorClient();
    NOVUSKAFKATEMPLATE_SCHEMAREGISTRY = new ProducerSchemaRegistryFactory(NOVUSCURATORCLIENT.getBrokers(),
        System.getProperty(SCHEMA_REGISTRY_URL, "http://localhost:8081"))
                .template();
    NOVUSKAFKATEMPLATE_STRING = new ProducerStringFactory(NOVUSCURATORCLIENT.getBrokers(),
        System.getProperty(SCHEMA_REGISTRY_URL, "http://localhost:8081"))
                .template();
  }

  public static NovusKafkaTemplate<GenericRecord, GenericRecord> kafkaTemplate() {
    return NOVUSKAFKATEMPLATE_SCHEMAREGISTRY;
  }

  public static NovusKafkaTemplate<String, String> kafkaTemplateString() {
    return NOVUSKAFKATEMPLATE_STRING;
  }

  public static NovusCuratorClient novusCuratorClient() {
    return NOVUSCURATORCLIENT;
  }

}
