package org.novus.common;

/**
 * Different types of message supported to send to kafka
 * 
 * @author Manjeet Duhan
 * @date Aug 19, 2018
 *
 */
public enum SchemaType {

  STRING("string"), BYTE("byte"), AVRO("avro");

  String schemaType;

  SchemaType(String schemaType) {
    this.schemaType = schemaType;
  }

  public String client() {
    return schemaType;
  }
}
