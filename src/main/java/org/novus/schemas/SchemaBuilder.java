package org.novus.schemas;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder.FieldAssembler;
import org.novus.common.FieldType;

/**
 * 
 * SchemaBuilder schemaBuilder = new SchemaBuilder("schedulerkey"); Schema key = schemaBuilder.addFields("id",
 * FieldType.LONG).endRecord(); GenericRecord avroKeyRecord = new GenericData.Record(key);
 * avroKeyRecord.put("id", Instant.now().getEpochSecond());
 * 
 * @author Manjeet Duhan
 * @date Aug 19, 2018
 *
 */
public class SchemaBuilder {

  private FieldAssembler<Schema> fields;

  public SchemaBuilder(String schemaName) {
    this.fields = org.apache.avro.SchemaBuilder.record(schemaName).fields();
  }

  public FieldAssembler<Schema> field(String fieldName, FieldType fieldType) {
    switch (fieldType) {
    case STRING:
      return fields.name(fieldName).type().nullable().stringType().noDefault();
    case INTEGER:
      return fields.name(fieldName).type().nullable().intType().noDefault();
    case DOUBLE:
      return fields.name(fieldName).type().nullable().doubleType().noDefault();
    case BYTES:
      return fields.name(fieldName).type().nullable().bytesType().noDefault();
    case BOOLEAN:
      return fields.name(fieldName).type().nullable().booleanType().noDefault();
    case FLOAT:
      return fields.name(fieldName).type().nullable().floatType().noDefault();
    case LONG:
      return fields.name(fieldName).type().nullable().longType().noDefault();
    }
    throw new IllegalArgumentException("field type not supported in SchemaBuilder");
  }

  /**
   * This will add nested schema at the given field
   * 
   * @param fieldName
   * @param nestedSchema
   * @return
   */
  public FieldAssembler<Schema> field(String fieldName, Schema nestedSchema) {
    return fields.name(fieldName).type(nestedSchema).noDefault();
  }

}
