package org.apache.hudi.common.types;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.IntNode;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class TestSchema {

  @Test
  public void testBasicSchemaCreation() {
    Schema stringSchema = Schema.create(Schema.Type.STRING);
    assertEquals(Schema.Type.STRING, stringSchema.getType());
    assertNull(stringSchema.getName());
  }

  @Test
  public void testNamedSchemaCreation() {
    Schema recordSchema = Schema.createRecord("TestRecord", "Test record schema", "org.apache.hudi.test", false);
    assertEquals(Schema.Type.RECORD, recordSchema.getType());
    assertEquals("TestRecord", recordSchema.getName());
    assertEquals("org.apache.hudi.test", recordSchema.getNamespace());
  }

  @Test
  public void testArraySchemaCreation() {
    Schema stringSchema = Schema.create(Schema.Type.STRING);
    Schema arraySchema = Schema.createArray(stringSchema);
    assertEquals(Schema.Type.ARRAY, arraySchema.getType());
    assertEquals(stringSchema, arraySchema.getElementType());
  }

  @Test
  public void testMapSchemaCreation() {
    Schema valueSchema = Schema.create(Schema.Type.INT);
    Schema mapSchema = Schema.createMap(valueSchema);
    assertEquals(Schema.Type.MAP, mapSchema.getType());
    assertEquals(valueSchema, mapSchema.getValueType());
  }

  @Test
  public void testUnionSchemaCreation() {
    List<Schema> unionTypes = Arrays.asList(
        Schema.create(Schema.Type.NULL),
        Schema.create(Schema.Type.STRING)
    );
    Schema unionSchema = Schema.createUnion(unionTypes);
    assertEquals(Schema.Type.UNION, unionSchema.getType());
    assertEquals(2, unionSchema.getTypes().size());
  }

  @Test
  public void testEnumSchemaCreation() {
    List<String> symbols = Arrays.asList("RED", "GREEN", "BLUE");
    Schema enumSchema = Schema.createEnum("Color", "Color enum", "org.apache.hudi.test", symbols);
    assertEquals(Schema.Type.ENUM, enumSchema.getType());
    assertEquals("Color", enumSchema.getName());
    assertEquals(symbols, enumSchema.getEnumSymbols());
  }

  @Test
  public void testFixedSchemaCreation() {
    Schema fixedSchema = Schema.createFixed("MD5", "MD5 hash", "org.apache.hudi.test", 16);
    assertEquals(Schema.Type.FIXED, fixedSchema.getType());
    assertEquals("MD5", fixedSchema.getName());
    assertEquals(16, fixedSchema.getFixedSize());
  }

  @Test
  public void testRecordWithFields() {
    Schema stringField = Schema.create(Schema.Type.STRING);
    Schema intField = Schema.create(Schema.Type.INT);

    Schema recordSchema = Schema.createRecord("TestRecord", "Test record", "org.apache.hudi.test", false);
    List<Schema.Field> fields = Arrays.asList(
        new Schema.Field("name", stringField, "Name field", (JsonNode) null),
        new Schema.Field("age", intField, "Age field", new IntNode(0))
    );
    recordSchema.setFields(fields);

    assertEquals(2, recordSchema.getFields().size());
    assertEquals("name", recordSchema.getFields().get(0).name());
    assertEquals("age", recordSchema.getFields().get(1).name());
  }

  @Test
  public void testAvroSchemaConversion() {
    // Create Hudi schema
    Schema hudiStringSchema = Schema.create(Schema.Type.STRING);
    Schema hudiIntSchema = Schema.create(Schema.Type.INT);
    Schema hudiRecordSchema = Schema.createRecord("TestRecord", "Test record", "org.apache.hudi.test", false);

    List<Schema.Field> fields = Arrays.asList(
        new Schema.Field("name", hudiStringSchema, "Name field", (JsonNode) null),
        new Schema.Field("age", hudiIntSchema, "Age field", new IntNode(0))
    );
    hudiRecordSchema.setFields(fields);

    // Convert to Avro
    org.apache.avro.Schema avroSchema = hudiRecordSchema.toAvroSchema();
    assertNotNull(avroSchema);
    assertEquals(org.apache.avro.Schema.Type.RECORD, avroSchema.getType());
    assertEquals("TestRecord", avroSchema.getName());
    assertEquals(2, avroSchema.getFields().size());

    // Convert back to Hudi
    Schema convertedHudiSchema = Schema.fromAvroSchema(avroSchema);
    assertEquals(hudiRecordSchema.getType(), convertedHudiSchema.getType());
    assertEquals(hudiRecordSchema.getName(), convertedHudiSchema.getName());
    assertEquals(hudiRecordSchema.getFields().size(), convertedHudiSchema.getFields().size());
  }

  @Test
  public void testPrimitiveTypesAvroConversion() {
    Schema[] primitiveSchemas = {
        Schema.create(Schema.Type.NULL),
        Schema.create(Schema.Type.BOOLEAN),
        Schema.create(Schema.Type.INT),
        Schema.create(Schema.Type.LONG),
        Schema.create(Schema.Type.FLOAT),
        Schema.create(Schema.Type.DOUBLE),
        Schema.create(Schema.Type.BYTES),
        Schema.create(Schema.Type.STRING)
    };

    for (Schema hudiSchema : primitiveSchemas) {
      org.apache.avro.Schema avroSchema = hudiSchema.toAvroSchema();
      Schema convertedSchema = Schema.fromAvroSchema(avroSchema);
      assertEquals(hudiSchema.getType(), convertedSchema.getType());
    }
  }

  @Test
  public void testArrayAvroConversion() {
    Schema stringSchema = Schema.create(Schema.Type.STRING);
    Schema arraySchema = Schema.createArray(stringSchema);

    org.apache.avro.Schema avroArraySchema = arraySchema.toAvroSchema();
    assertEquals(org.apache.avro.Schema.Type.ARRAY, avroArraySchema.getType());

    Schema convertedArraySchema = Schema.fromAvroSchema(avroArraySchema);
    assertEquals(Schema.Type.ARRAY, convertedArraySchema.getType());
    assertEquals(Schema.Type.STRING, convertedArraySchema.getElementType().getType());
  }

  @Test
  public void testMapAvroConversion() {
    Schema intSchema = Schema.create(Schema.Type.INT);
    Schema mapSchema = Schema.createMap(intSchema);

    org.apache.avro.Schema avroMapSchema = mapSchema.toAvroSchema();
    assertEquals(org.apache.avro.Schema.Type.MAP, avroMapSchema.getType());

    Schema convertedMapSchema = Schema.fromAvroSchema(avroMapSchema);
    assertEquals(Schema.Type.MAP, convertedMapSchema.getType());
    assertEquals(Schema.Type.INT, convertedMapSchema.getValueType().getType());
  }

  @Test
  public void testUnionAvroConversion() {
    List<Schema> unionTypes = Arrays.asList(
        Schema.create(Schema.Type.NULL),
        Schema.create(Schema.Type.STRING)
    );
    Schema unionSchema = Schema.createUnion(unionTypes);

    org.apache.avro.Schema avroUnionSchema = unionSchema.toAvroSchema();
    assertEquals(org.apache.avro.Schema.Type.UNION, avroUnionSchema.getType());
    assertEquals(2, avroUnionSchema.getTypes().size());

    Schema convertedUnionSchema = Schema.fromAvroSchema(avroUnionSchema);
    assertEquals(Schema.Type.UNION, convertedUnionSchema.getType());
    assertEquals(2, convertedUnionSchema.getTypes().size());
  }

  @Test
  public void testEnumAvroConversion() {
    List<String> symbols = Arrays.asList("RED", "GREEN", "BLUE");
    Schema enumSchema = Schema.createEnum("Color", "Color enum", "org.apache.hudi.test", symbols);

    org.apache.avro.Schema avroEnumSchema = enumSchema.toAvroSchema();
    assertEquals(org.apache.avro.Schema.Type.ENUM, avroEnumSchema.getType());
    assertEquals("Color", avroEnumSchema.getName());
    assertEquals(symbols, avroEnumSchema.getEnumSymbols());

    Schema convertedEnumSchema = Schema.fromAvroSchema(avroEnumSchema);
    assertEquals(Schema.Type.ENUM, convertedEnumSchema.getType());
    assertEquals("Color", convertedEnumSchema.getName());
    assertEquals(symbols, convertedEnumSchema.getEnumSymbols());
  }

  @Test
  public void testFixedAvroConversion() {
    Schema fixedSchema = Schema.createFixed("MD5", "MD5 hash", "org.apache.hudi.test", 16);

    org.apache.avro.Schema avroFixedSchema = fixedSchema.toAvroSchema();
    assertEquals(org.apache.avro.Schema.Type.FIXED, avroFixedSchema.getType());
    assertEquals("MD5", avroFixedSchema.getName());
    assertEquals(16, avroFixedSchema.getFixedSize());

    Schema convertedFixedSchema = Schema.fromAvroSchema(avroFixedSchema);
    assertEquals(Schema.Type.FIXED, convertedFixedSchema.getType());
    assertEquals("MD5", convertedFixedSchema.getName());
    assertEquals(16, convertedFixedSchema.getFixedSize());
  }

  @Test
  public void testSchemaStringSerializationDeserialization() {
    Schema stringSchema = Schema.create(Schema.Type.STRING);
    Schema recordSchema = Schema.createRecord("TestRecord", "Test record", "org.apache.hudi.test", false);

    List<Schema.Field> fields = Arrays.asList(
        new Schema.Field("name", stringSchema, "Name field", (JsonNode) null)
    );
    recordSchema.setFields(fields);

    // Convert to string
    String schemaString = recordSchema.toString();
    assertNotNull(schemaString);
    assertTrue(schemaString.contains("TestRecord"));

    // Parse back from string
    Schema parsedSchema = Schema.parse(schemaString);
    assertEquals(recordSchema.getType(), parsedSchema.getType());
    assertEquals(recordSchema.getName(), parsedSchema.getName());
    assertEquals(recordSchema.getFields().size(), parsedSchema.getFields().size());
  }

  @Test
  public void testAvroSchemaStringCompatibility() {
    // Create Avro schema string
    String avroSchemaString = "{\n" +
        "  \"type\": \"record\",\n" +
        "  \"name\": \"TestRecord\",\n" +
        "  \"namespace\": \"org.apache.hudi.test\",\n" +
        "  \"fields\": [\n" +
        "    {\"name\": \"name\", \"type\": \"string\"},\n" +
        "    {\"name\": \"age\", \"type\": \"int\", \"default\": 0}\n" +
        "  ]\n" +
        "}";

    // Parse Avro schema
    org.apache.avro.Schema avroSchema = new org.apache.avro.Schema.Parser().parse(avroSchemaString);

    // Convert to Hudi schema
    Schema hudiSchema = Schema.fromAvroSchema(avroSchema);
    assertEquals(Schema.Type.RECORD, hudiSchema.getType());
    assertEquals("TestRecord", hudiSchema.getName());
    assertEquals(2, hudiSchema.getFields().size());

    // Convert back to Avro and serialize to string
    org.apache.avro.Schema convertedAvroSchema = hudiSchema.toAvroSchema();
    String convertedAvroString = convertedAvroSchema.toString();

    // Parse both strings and compare
    org.apache.avro.Schema originalParsed = new org.apache.avro.Schema.Parser().parse(avroSchemaString);
    org.apache.avro.Schema convertedParsed = new org.apache.avro.Schema.Parser().parse(convertedAvroString);

    assertEquals(originalParsed, convertedParsed);
  }

  @Test
  public void testComplexNestedSchemaConversion() {
    // Create a complex nested schema
    Schema addressSchema = Schema.createRecord("Address", "Address record", "org.apache.hudi.test", false);
    List<Schema.Field> addressFields = Arrays.asList(
        new Schema.Field("street", Schema.create(Schema.Type.STRING), "Street", (JsonNode) null),
        new Schema.Field("city", Schema.create(Schema.Type.STRING), "City", (JsonNode) null),
        new Schema.Field("zipcode", Schema.create(Schema.Type.INT), "Zip code", new IntNode(0))
    );
    addressSchema.setFields(addressFields);

    Schema personSchema = Schema.createRecord("Person", "Person record", "org.apache.hudi.test", false);
    Schema phoneArraySchema = Schema.createArray(Schema.create(Schema.Type.STRING));
    Schema metadataMapSchema = Schema.createMap(Schema.create(Schema.Type.STRING));

    List<Schema.Field> personFields = Arrays.asList(
        new Schema.Field("name", Schema.create(Schema.Type.STRING), "Name", (JsonNode) null),
        new Schema.Field("age", Schema.create(Schema.Type.INT), "Age", new IntNode(0)),
        new Schema.Field("address", addressSchema, "Address", (JsonNode) null),
        new Schema.Field("phoneNumbers", phoneArraySchema, "Phone numbers", (JsonNode) null),
        new Schema.Field("metadata", metadataMapSchema, "Metadata", (JsonNode) null)
    );
    personSchema.setFields(personFields);

    // Convert to Avro and back
    org.apache.avro.Schema avroPersonSchema = personSchema.toAvroSchema();
    Schema convertedPersonSchema = Schema.fromAvroSchema(avroPersonSchema);

    assertEquals(personSchema.getType(), convertedPersonSchema.getType());
    assertEquals(personSchema.getName(), convertedPersonSchema.getName());
    assertEquals(personSchema.getFields().size(), convertedPersonSchema.getFields().size());

    // Verify nested address schema
    Schema convertedAddressField = convertedPersonSchema.getField("address").schema();
    assertEquals(Schema.Type.RECORD, convertedAddressField.getType());
    assertEquals("Address", convertedAddressField.getName());
    assertEquals(3, convertedAddressField.getFields().size());
  }

  @Test
  public void testSchemaEquality() {
    Schema schema1 = Schema.create(Schema.Type.STRING);
    Schema schema2 = Schema.create(Schema.Type.STRING);
    Schema schema3 = Schema.create(Schema.Type.INT);

    assertEquals(schema1, schema2);
    assertNotEquals(schema1, schema3);

    Schema record1 = Schema.createRecord("Test", "Test record", "org.apache.hudi.test", false);
    Schema record2 = Schema.createRecord("Test", "Test record", "org.apache.hudi.test", false);
    Schema record3 = Schema.createRecord("Different", "Different record", "org.apache.hudi.test", false);

    assertEquals(record1, record2);
    assertNotEquals(record1, record3);
  }

  @Test
  public void testSchemaHashCode() {
    Schema schema1 = Schema.create(Schema.Type.STRING);
    Schema schema2 = Schema.create(Schema.Type.STRING);

    assertEquals(schema1.hashCode(), schema2.hashCode());

    Schema record1 = Schema.createRecord("Test", "Test record", "org.apache.hudi.test", false);
    Schema record2 = Schema.createRecord("Test", "Test record", "org.apache.hudi.test", false);

    assertEquals(record1.hashCode(), record2.hashCode());
  }

  @Test
  public void testSchemaParser() {
    String schemaJson = "{\"type\": \"string\"}";
    
    // Test static parse method
    Schema schema1 = Schema.parse(schemaJson);
    assertEquals(Schema.Type.STRING, schema1.getType());
    
    // Test Parser class for Avro compatibility
    Schema.Parser parser = new Schema.Parser();
    Schema schema2 = parser.parse(schemaJson);
    assertEquals(Schema.Type.STRING, schema2.getType());
    
    assertEquals(schema1, schema2);
  }

  @Test
  public void testFieldPositions() {
    Schema recordSchema = Schema.createRecord("TestRecord", "Test record", "org.apache.hudi.test", false);
    List<Schema.Field> fields = Arrays.asList(
        new Schema.Field("field1", Schema.create(Schema.Type.STRING), "First field", (JsonNode) null),
        new Schema.Field("field2", Schema.create(Schema.Type.INT), "Second field", new IntNode(0)),
        new Schema.Field("field3", Schema.create(Schema.Type.BOOLEAN), "Third field", (JsonNode) null)
    );
    recordSchema.setFields(fields);

    List<Schema.Field> recordFields = recordSchema.getFields();
    assertEquals(0, recordFields.get(0).pos());
    assertEquals(1, recordFields.get(1).pos());
    assertEquals(2, recordFields.get(2).pos());
    
    assertEquals("field1", recordFields.get(0).name());
    assertEquals("field2", recordFields.get(1).name());
    assertEquals("field3", recordFields.get(2).name());
  }

  @Test
  public void testFieldProperties() {
    Schema.Field field = new Schema.Field("testField", Schema.create(Schema.Type.STRING), "Test field", (JsonNode) null);
    
    // Test Object properties
    field.addProp("stringProp", "stringValue");
    field.addProp("intProp", 42);
    field.addProp("boolProp", true);
    
    assertEquals("stringValue", field.getProp("stringProp"));
    assertEquals(42, field.getProp("intProp"));
    assertEquals(true, field.getProp("boolProp"));
    
    // Test getObjectProps
    Map<String, Object> props = field.getObjectProps();
    assertEquals(3, props.size());
    assertEquals("stringValue", props.get("stringProp"));
    assertEquals(42, props.get("intProp"));
    assertEquals(true, props.get("boolProp"));
  }

  @Test
  public void testSchemaProperties() {
    Schema schema = Schema.create(Schema.Type.STRING);
    
    // Test Object properties
    schema.addProp("stringProp", "stringValue");
    schema.addProp("intProp", 42);
    schema.addProp("boolProp", true);
    
    assertEquals("stringValue", schema.getProp("stringProp"));
    assertEquals(42, schema.getProp("intProp"));
    assertEquals(true, schema.getProp("boolProp"));
    
    // Test getObjectProps
    Map<String, Object> props = schema.getObjectProps();
    assertEquals(3, props.size());
    assertEquals("stringValue", props.get("stringProp"));
    assertEquals(42, props.get("intProp"));
    assertEquals(true, props.get("boolProp"));
  }

  @Test
  public void testLogicalTypes() {
    // Test decimal logical type
    Schema bytesSchema = Schema.create(Schema.Type.BYTES);
    Schema.LogicalType decimal = Schema.LogicalTypes.decimal(10, 2);
    bytesSchema.setLogicalType(decimal);
    
    assertEquals(decimal, bytesSchema.getLogicalType());
    assertEquals("decimal", bytesSchema.getLogicalType().getName());
    
    // Test timestamp logical type
    Schema longSchema = Schema.create(Schema.Type.LONG);
    Schema.LogicalType timestamp = Schema.LogicalTypes.timestampMillis();
    longSchema.setLogicalType(timestamp);
    
    assertEquals(timestamp, longSchema.getLogicalType());
    assertEquals("timestamp-millis", longSchema.getLogicalType().getName());
    
    // Test date logical type
    Schema intSchema = Schema.create(Schema.Type.INT);
    Schema.LogicalType date = Schema.LogicalTypes.date();
    intSchema.setLogicalType(date);
    
    assertEquals(date, intSchema.getLogicalType());
    assertEquals("date", intSchema.getLogicalType().getName());
  }

  @Test
  public void testLogicalTypeValidation() {
    // Test that decimal validates correctly
    Schema bytesSchema = Schema.create(Schema.Type.BYTES);
    assertDoesNotThrow(() -> {
      bytesSchema.setLogicalType(Schema.LogicalTypes.decimal(10, 2));
    });
    
    // Test that decimal throws exception for wrong type
    Schema stringSchema = Schema.create(Schema.Type.STRING);
    assertThrows(IllegalArgumentException.class, () -> {
      stringSchema.setLogicalType(Schema.LogicalTypes.decimal(10, 2));
    });
    
    // Test timestamp validation
    Schema longSchema = Schema.create(Schema.Type.LONG);
    assertDoesNotThrow(() -> {
      longSchema.setLogicalType(Schema.LogicalTypes.timestampMillis());
    });
    
    Schema intSchema = Schema.create(Schema.Type.INT);
    assertThrows(IllegalArgumentException.class, () -> {
      intSchema.setLogicalType(Schema.LogicalTypes.timestampMillis());
    });
  }

  @Test
  public void testCompleteAvroCompatibility() {
    // Test that we can use the same API calls as Avro Schema
    String complexSchemaJson = 
        "{\"type\": \"record\", " +
        "\"name\": \"ComplexRecord\", " +
        "\"namespace\": \"org.apache.hudi.test\", " +
        "\"fields\": [" +
        "{\"name\": \"id\", \"type\": \"long\"}, " +
        "{\"name\": \"name\", \"type\": \"string\"}, " +
        "{\"name\": \"scores\", \"type\": {\"type\": \"array\", \"items\": \"int\"}}, " +
        "{\"name\": \"metadata\", \"type\": {\"type\": \"map\", \"values\": \"string\"}}" +
        "]}";
    
    // Parse using both methods
    Schema schema1 = Schema.parse(complexSchemaJson);
    Schema.Parser parser = new Schema.Parser();
    Schema schema2 = parser.parse(complexSchemaJson);
    
    // Verify both schemas are equivalent
    assertEquals(schema1, schema2);
    assertEquals(Schema.Type.RECORD, schema1.getType());
    assertEquals("ComplexRecord", schema1.getName());
    assertEquals("org.apache.hudi.test", schema1.getNamespace());
    assertEquals(4, schema1.getFields().size());
    
    // Test field access by name - common Avro pattern
    Schema.Field idField = schema1.getField("id");
    assertNotNull(idField);
    assertEquals("id", idField.name());
    assertEquals(Schema.Type.LONG, idField.schema().getType());
    assertEquals(0, idField.pos());
    
    Schema.Field scoresField = schema1.getField("scores");
    assertEquals(Schema.Type.ARRAY, scoresField.schema().getType());
    assertEquals(Schema.Type.INT, scoresField.schema().getElementType().getType());
    assertEquals(2, scoresField.pos());
    
    // Test conversion to/from Avro
    org.apache.avro.Schema avroSchema = schema1.toAvroSchema();
    assertNotNull(avroSchema);
    
    Schema convertedBack = Schema.fromAvroSchema(avroSchema);
    assertEquals(schema1, convertedBack);
  }
}
