package org.apache.hudi.common.types;

import org.junit.jupiter.api.Test;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class for HoodieSchema and HoodieField to verify they work as expected
 * and provide the same functionality as Avro's Schema and Field classes.
 */
public class HoodieSchemaTest {

    @Test
    public void testPrimitiveSchemas() {
        // Test creation of primitive schemas
        HoodieSchema stringSchema = HoodieSchema.create(HoodieSchema.Type.STRING);
        assertEquals(HoodieSchema.Type.STRING, stringSchema.getType());
        assertEquals("\"string\"", stringSchema.toString());

        HoodieSchema intSchema = HoodieSchema.create(HoodieSchema.Type.INT);
        assertEquals(HoodieSchema.Type.INT, intSchema.getType());
        assertEquals("\"int\"", intSchema.toString());

        HoodieSchema nullSchema = HoodieSchema.create(HoodieSchema.Type.NULL);
        assertEquals(HoodieSchema.Type.NULL, nullSchema.getType());
        assertEquals("\"null\"", nullSchema.toString());
    }

    @Test
    public void testRecordSchema() {
        // Create fields
        HoodieField idField = new HoodieField("id", HoodieSchema.create(HoodieSchema.Type.LONG));
        HoodieField nameField = new HoodieField("name", HoodieSchema.create(HoodieSchema.Type.STRING));
        
        List<HoodieField> fields = Arrays.asList(idField, nameField);
        
        // Create record schema
        HoodieSchema recordSchema = HoodieSchema.createRecord("User", "com.example", "User record", fields);
        
        assertEquals(HoodieSchema.Type.RECORD, recordSchema.getType());
        assertEquals("User", recordSchema.getName());
        assertEquals("com.example", recordSchema.getNamespace());
        assertEquals("com.example.User", recordSchema.getFullName());
        assertEquals("User record", recordSchema.getDoc());
        assertEquals(2, recordSchema.getFields().size());
        
        // Test field access
        HoodieField retrievedIdField = recordSchema.getField("id");
        assertNotNull(retrievedIdField);
        assertEquals("id", retrievedIdField.name());
        assertEquals(HoodieSchema.Type.LONG, retrievedIdField.schema().getType());
        
        HoodieField retrievedNameField = recordSchema.getField("name");
        assertNotNull(retrievedNameField);
        assertEquals("name", retrievedNameField.name());
        assertEquals(HoodieSchema.Type.STRING, retrievedNameField.schema().getType());
    }

    @Test
    public void testArraySchema() {
        HoodieSchema stringSchema = HoodieSchema.create(HoodieSchema.Type.STRING);
        HoodieSchema arraySchema = HoodieSchema.createArray(stringSchema);
        
        assertEquals(HoodieSchema.Type.ARRAY, arraySchema.getType());
        assertEquals(stringSchema, arraySchema.getElementType());
        assertEquals("{\"type\":\"array\",\"items\":\"string\"}", arraySchema.toString());
    }

    @Test
    public void testMapSchema() {
        HoodieSchema intSchema = HoodieSchema.create(HoodieSchema.Type.INT);
        HoodieSchema mapSchema = HoodieSchema.createMap(intSchema);
        
        assertEquals(HoodieSchema.Type.MAP, mapSchema.getType());
        assertEquals(intSchema, mapSchema.getValueType());
        assertEquals("{\"type\":\"map\",\"values\":\"int\"}", mapSchema.toString());
    }

    @Test
    public void testUnionSchema() {
        HoodieSchema nullSchema = HoodieSchema.create(HoodieSchema.Type.NULL);
        HoodieSchema stringSchema = HoodieSchema.create(HoodieSchema.Type.STRING);
        
        List<HoodieSchema> types = Arrays.asList(nullSchema, stringSchema);
        HoodieSchema unionSchema = HoodieSchema.createUnion(types);
        
        assertEquals(HoodieSchema.Type.UNION, unionSchema.getType());
        assertEquals(2, unionSchema.getTypes().size());
        assertEquals(nullSchema, unionSchema.getTypes().get(0));
        assertEquals(stringSchema, unionSchema.getTypes().get(1));
        assertEquals("[\"null\",\"string\"]", unionSchema.toString());
    }

    @Test
    public void testNullableUnion() {
        HoodieSchema stringSchema = HoodieSchema.create(HoodieSchema.Type.STRING);
        HoodieSchema nullableStringSchema = HoodieSchema.createNullableUnion(stringSchema);
        
        assertEquals(HoodieSchema.Type.UNION, nullableStringSchema.getType());
        assertEquals(2, nullableStringSchema.getTypes().size());
        assertEquals(HoodieSchema.Type.NULL, nullableStringSchema.getTypes().get(0).getType());
        assertEquals(HoodieSchema.Type.STRING, nullableStringSchema.getTypes().get(1).getType());
    }

    @Test
    public void testEnumSchema() {
        List<String> symbols = Arrays.asList("RED", "GREEN", "BLUE");
        HoodieSchema enumSchema = HoodieSchema.createEnum("Color", "com.example", "Color enum", symbols);
        
        assertEquals(HoodieSchema.Type.ENUM, enumSchema.getType());
        assertEquals("Color", enumSchema.getName());
        assertEquals("com.example", enumSchema.getNamespace());
        assertEquals("Color enum", enumSchema.getDoc());
        assertEquals(symbols, enumSchema.getEnumSymbols());
    }

    @Test
    public void testFixedSchema() {
        HoodieSchema fixedSchema = HoodieSchema.createFixed("MD5", "com.example", "MD5 hash", 16);
        
        assertEquals(HoodieSchema.Type.FIXED, fixedSchema.getType());
        assertEquals("MD5", fixedSchema.getName());
        assertEquals("com.example", fixedSchema.getNamespace());
        assertEquals("MD5 hash", fixedSchema.getDoc());
        assertEquals(16, fixedSchema.getFixedSize());
    }

    @Test
    public void testFieldWithDefaultValue() {
        HoodieSchema stringSchema = HoodieSchema.create(HoodieSchema.Type.STRING);
        HoodieField fieldWithDefault = new HoodieField("name", stringSchema, "Name field", "unknown");
        
        assertEquals("name", fieldWithDefault.name());
        assertEquals(stringSchema, fieldWithDefault.schema());
        assertEquals("Name field", fieldWithDefault.doc());
        assertEquals("unknown", fieldWithDefault.defaultValue());
        assertTrue(fieldWithDefault.hasDefaultValue());
    }

    @Test
    public void testFieldBuilder() {
        HoodieSchema intSchema = HoodieSchema.create(HoodieSchema.Type.INT);
        HoodieField field = new HoodieField.Builder("age", intSchema)
                .setDoc("Age in years")
                .setDefaultValue(0)
                .setPos(1)
                .setProp("required", true)
                .build();
        
        assertEquals("age", field.name());
        assertEquals(intSchema, field.schema());
        assertEquals("Age in years", field.doc());
        assertEquals(0, field.defaultValue());
        assertEquals(1, field.pos());
        assertEquals(true, field.getProp("required"));
    }

    @Test
    public void testParsePrimitiveSchemas() {
        assertEquals(HoodieSchema.Type.STRING, HoodieSchema.parse("\"string\"").getType());
        assertEquals(HoodieSchema.Type.INT, HoodieSchema.parse("\"int\"").getType());
        assertEquals(HoodieSchema.Type.NULL, HoodieSchema.parse("\"null\"").getType());
    }

    @Test
    public void testSchemaEquality() {
        HoodieSchema schema1 = HoodieSchema.create(HoodieSchema.Type.STRING);
        HoodieSchema schema2 = HoodieSchema.create(HoodieSchema.Type.STRING);
        HoodieSchema schema3 = HoodieSchema.create(HoodieSchema.Type.INT);
        
        assertEquals(schema1, schema2);
        assertNotEquals(schema1, schema3);
        assertEquals(schema1.hashCode(), schema2.hashCode());
    }

    @Test
    public void testFieldEquality() {
        HoodieSchema stringSchema = HoodieSchema.create(HoodieSchema.Type.STRING);
        HoodieField field1 = new HoodieField("name", stringSchema);
        HoodieField field2 = new HoodieField("name", stringSchema);
        HoodieField field3 = new HoodieField("other", stringSchema);
        
        assertEquals(field1, field2);
        assertNotEquals(field1, field3);
        assertEquals(field1.hashCode(), field2.hashCode());
    }

    @Test
    public void testIllegalOperations() {
        HoodieSchema stringSchema = HoodieSchema.create(HoodieSchema.Type.STRING);
        
        // Test calling getFields() on non-record schema
        assertThrows(IllegalStateException.class, () -> {
            stringSchema.getFields();
        });
        
        // Test calling getElementType() on non-array schema
        assertThrows(IllegalStateException.class, () -> {
            stringSchema.getElementType();
        });
        
        // Test calling getValueType() on non-map schema
        assertThrows(IllegalStateException.class, () -> {
            stringSchema.getValueType();
        });
        
        // Test calling getTypes() on non-union schema
        assertThrows(IllegalStateException.class, () -> {
            stringSchema.getTypes();
        });
    }
}
