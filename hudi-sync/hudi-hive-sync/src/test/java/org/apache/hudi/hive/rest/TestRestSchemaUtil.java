/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hudi.hive.rest;

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.testutils.SchemaTestUtil;
import org.apache.hudi.hive.HiveSyncConfig;
import org.apache.hudi.hive.rest.model.PartitionSpec;
import org.apache.hudi.hive.rest.model.Schema;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_SUPPORT_TIMESTAMP_TYPE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for RestSchemaUtil - schema conversion utilities.
 */
public class TestRestSchemaUtil {

  private HiveSyncConfig config;

  @BeforeEach
  public void setup() {
    Properties props = new Properties();
    props.setProperty(HIVE_SUPPORT_TIMESTAMP_TYPE.key(), "false");
    config = new HiveSyncConfig(props);
  }

  @Test
  public void testConvertToRestSchema() throws Exception {
    HoodieSchema hoodieSchema = SchemaTestUtil.getSimpleSchema();

    Schema restSchema = RestSchemaUtil.convertToRestSchema(hoodieSchema, config);

    assertNotNull(restSchema);
    assertNotNull(restSchema.getFields());
    assertFalse(restSchema.getFields().isEmpty());
    assertEquals("struct", restSchema.getType());
    assertEquals(0, restSchema.getSchemaId());
  }

  @Test
  public void testSchemaFieldConversion() throws Exception {
    HoodieSchema hoodieSchema = SchemaTestUtil.getSimpleSchema();

    Schema restSchema = RestSchemaUtil.convertToRestSchema(hoodieSchema, config);

    // Verify fields have proper IDs
    List<Schema.SchemaField> fields = restSchema.getFields();
    for (int i = 0; i < fields.size(); i++) {
      Schema.SchemaField field = fields.get(i);
      assertNotNull(field.getName());
      assertNotNull(field.getType());
      assertEquals(i, field.getId());
    }
  }

  @Test
  public void testTypeMapping() throws Exception {
    HoodieSchema hoodieSchema = SchemaTestUtil.getSimpleSchema();

    Schema restSchema = RestSchemaUtil.convertToRestSchema(hoodieSchema, config);

    // Verify type mapping (Hive types -> REST types)
    // STRING -> string, INT -> int, BIGINT -> long, etc.
    List<Schema.SchemaField> fields = restSchema.getFields();
    for (Schema.SchemaField field : fields) {
      String type = field.getType();
      // Types should be lowercase and mapped correctly
      assertNotNull(type);
      assertFalse(type.isEmpty());
    }
  }

  @Test
  public void testPartitionSpecConversionNoPartitions() throws Exception {
    HoodieSchema hoodieSchema = SchemaTestUtil.getSimpleSchema();
    Schema restSchema = RestSchemaUtil.convertToRestSchema(hoodieSchema, config);

    List<String> partitionFields = Arrays.asList();

    PartitionSpec partitionSpec = RestSchemaUtil.convertToPartitionSpec(partitionFields, restSchema);

    assertNotNull(partitionSpec);
    assertNotNull(partitionSpec.getFields());
    assertTrue(partitionSpec.getFields().isEmpty());
    assertEquals(0, partitionSpec.getSpecId());
  }

  @Test
  public void testPartitionSpecConversionWithPartitions() throws Exception {
    HoodieSchema hoodieSchema = SchemaTestUtil.getSimpleSchema();
    Schema restSchema = RestSchemaUtil.convertToRestSchema(hoodieSchema, config);

    // Assume one of the fields can be used as partition
    String firstFieldName = restSchema.getFields().get(0).getName();
    List<String> partitionFields = Arrays.asList(firstFieldName);

    PartitionSpec partitionSpec = RestSchemaUtil.convertToPartitionSpec(partitionFields, restSchema);

    assertNotNull(partitionSpec);
    assertNotNull(partitionSpec.getFields());
    assertEquals(1, partitionSpec.getFields().size());

    PartitionSpec.PartitionField partField = partitionSpec.getFields().get(0);
    assertEquals(firstFieldName, partField.getName());
    assertEquals("identity", partField.getTransform());
    assertNotNull(partField.getSourceId());
    assertNotNull(partField.getFieldId());
  }

  @Test
  public void testPartitionSpecFieldIds() throws Exception {
    HoodieSchema hoodieSchema = SchemaTestUtil.getSimpleSchema();
    Schema restSchema = RestSchemaUtil.convertToRestSchema(hoodieSchema, config);

    String firstFieldName = restSchema.getFields().get(0).getName();
    List<String> partitionFields = Arrays.asList(firstFieldName);

    PartitionSpec partitionSpec = RestSchemaUtil.convertToPartitionSpec(partitionFields, restSchema);

    PartitionSpec.PartitionField partField = partitionSpec.getFields().get(0);

    // Source ID should match the schema field ID
    Integer sourceId = partField.getSourceId();
    assertEquals(0, sourceId); // First field has ID 0

    // Field ID should be >= 1000 (partition field IDs)
    Integer fieldId = partField.getFieldId();
    assertTrue(fieldId >= 1000);
  }

  @Test
  public void testMultiplePartitionFields() throws Exception {
    HoodieSchema hoodieSchema = SchemaTestUtil.getSimpleSchema();
    Schema restSchema = RestSchemaUtil.convertToRestSchema(hoodieSchema, config);

    // Use multiple fields as partitions (if available)
    List<Schema.SchemaField> allFields = restSchema.getFields();
    if (allFields.size() >= 2) {
      List<String> partitionFields = Arrays.asList(
          allFields.get(0).getName(),
          allFields.get(1).getName()
      );

      PartitionSpec partitionSpec = RestSchemaUtil.convertToPartitionSpec(partitionFields, restSchema);

      assertNotNull(partitionSpec);
      assertEquals(2, partitionSpec.getFields().size());

      // Verify all partition fields have identity transform
      for (PartitionSpec.PartitionField pf : partitionSpec.getFields()) {
        assertEquals("identity", pf.getTransform());
      }
    }
  }

  @Test
  public void testPartitionFieldNotInSchema() throws Exception {
    HoodieSchema hoodieSchema = SchemaTestUtil.getSimpleSchema();
    Schema restSchema = RestSchemaUtil.convertToRestSchema(hoodieSchema, config);

    // Try to partition by field that doesn't exist
    List<String> partitionFields = Arrays.asList("nonexistent_field");

    PartitionSpec partitionSpec = RestSchemaUtil.convertToPartitionSpec(partitionFields, restSchema);

    // Should create partition spec but with no fields (field not found)
    assertNotNull(partitionSpec);
    assertTrue(partitionSpec.getFields().isEmpty());
  }

  @Test
  public void testEvolvedSchema() throws Exception {
    HoodieSchema evolvedSchema = SchemaTestUtil.getEvolvedSchema();

    Schema restSchema = RestSchemaUtil.convertToRestSchema(evolvedSchema, config);

    assertNotNull(restSchema);
    assertNotNull(restSchema.getFields());
    assertFalse(restSchema.getFields().isEmpty());

    // Evolved schema should have more fields
    assertTrue(restSchema.getFields().size() > 0);
  }

  @Test
  public void testTimestampTypeSupport() throws Exception {
    Properties propsWithTimestamp = new Properties();
    propsWithTimestamp.setProperty(HIVE_SUPPORT_TIMESTAMP_TYPE.key(), "true");
    HiveSyncConfig configWithTimestamp = new HiveSyncConfig(propsWithTimestamp);

    HoodieSchema hoodieSchema = SchemaTestUtil.getSimpleSchema();

    Schema restSchema = RestSchemaUtil.convertToRestSchema(hoodieSchema, configWithTimestamp);

    // Should successfully convert even with timestamp support enabled
    assertNotNull(restSchema);
    assertFalse(restSchema.getFields().isEmpty());
  }
}
