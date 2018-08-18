package com.sintecmedia;

import java.time.Instant;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.novus.common.FieldType;
import org.novus.common.NovusEnvironment;
import org.novus.common.SchemaType;
import org.novus.schemas.SchemaBuilder;

/**
 * 
 * Vault set up should be complete for these test to success
 * 
 * @author Manjeet Duhan
 * @date Aug 19, 2018
 *
 */
@RunWith(JUnit4.class)
public class ProducerTest {

  @Test
  public void StringMessageTest() {
    // set zookeeperUrl defaulted to localhost:2181
    // System.setProperty(NovusConstants.ZOOKEEPER_URL, "ZOOKEEPER_URL");

    NovusEnvironment.DEFAULT.kafkaTemplate(SchemaType.STRING).push("StringTopic", "myid", "Novus String message 2");
  }

  @Test
  public void SimpleAvroMessageTest() {
    // set zookeeperUrl defaulted to localhost:2181
    // System.setProperty(NovusConstants.ZOOKEEPER_URL, "ZOOKEEPER_URL");

    // set zookeeperUrl defaulted to http://localhost:8081
    // System.setProperty(NovusConstants.SCHEMA_REGISTRY_URL, "schemaRegistryUrl");

    // key schema avro
    SchemaBuilder schemaBuilder = new SchemaBuilder("keyschema");
    Schema key = schemaBuilder.field("id", FieldType.LONG).endRecord();
    // key value added
    GenericRecord avroKeyRecord = new GenericData.Record(key);
    avroKeyRecord.put("id", Instant.now().getEpochSecond());

    // parent schema and add child schema to tenant field
    SchemaBuilder parentBuilder = new SchemaBuilder("parentSchema");
    parentBuilder.field("module", FieldType.STRING);
    Schema parentSchema = parentBuilder.field("timestamp", FieldType.LONG).endRecord();

    // create parent record and add it to value
    GenericRecord valueRecord = new GenericData.Record(parentSchema);
    valueRecord.put("module", "IM");
    valueRecord.put("timestamp", Instant.now().getEpochSecond());

    NovusEnvironment.DEFAULT.kafkaTemplate().push("avroTopic", avroKeyRecord, valueRecord);
  }

  @Test
  public void NestedAvroMessageTest() {

    // key schema avro
    SchemaBuilder schemaBuilder = new SchemaBuilder("keyschema");
    Schema key = schemaBuilder.field("id", FieldType.LONG).endRecord();
    // key value added
    GenericRecord avroKeyRecord = new GenericData.Record(key);
    avroKeyRecord.put("id", Instant.now().getEpochSecond());

    // child schema avro
    SchemaBuilder childSchemaBuilder = new SchemaBuilder("childSchema");
    childSchemaBuilder.field("name", FieldType.STRING);
    childSchemaBuilder.field("plis", FieldType.INTEGER);
    Schema childSchema = childSchemaBuilder.field("executed", FieldType.BOOLEAN).endRecord();

    // parent schema and add child schema to tenant field
    SchemaBuilder parentBuilder = new SchemaBuilder("parentSchema");
    parentBuilder.field("module", FieldType.STRING);
    parentBuilder.field("childSchema", childSchema);
    Schema parentSchema = parentBuilder.field("timestamp", FieldType.LONG).endRecord();

    // create child record
    GenericRecord childRecord = new GenericData.Record(childSchema);
    childRecord.put("name", "nested name");
    childRecord.put("plis", 1233);
    childRecord.put("executed", false);

    // create parent record and add it to value
    GenericRecord valueRecord = new GenericData.Record(parentSchema);
    valueRecord.put("module", "IM");
    valueRecord.put("childSchema", childRecord);
    valueRecord.put("timestamp", Instant.now().getEpochSecond());

    NovusEnvironment.DEFAULT.kafkaTemplate().push("avroNestedTopic", avroKeyRecord, valueRecord);
  }

}
