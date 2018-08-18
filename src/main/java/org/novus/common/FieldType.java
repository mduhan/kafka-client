package org.novus.common;

/**
 * These are the different type of field types for schema registry
 * 
 * @author Manjeet Duhan
 * @date Aug 19, 2018
 *
 */
public enum FieldType {

  STRING("string"), INTEGER("integer"), DOUBLE("double"), BYTES("bytes"), BOOLEAN("booolean"), FLOAT("float"), LONG(
      "long");

  String schemaType;

  FieldType(String schemaType) {
    this.schemaType = schemaType;
  }

  public String client() {
    return schemaType;
  }
}
