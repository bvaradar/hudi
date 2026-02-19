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
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.hive.HiveSyncConfig;
import org.apache.hudi.hive.ddl.RESTCatalogDDLExecutor;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_SYNC_REST_CATALOG_AUTH_TOKEN;
import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_SYNC_REST_CATALOG_AUTH_TYPE;
import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_SYNC_REST_CATALOG_TIMEOUT_SECONDS;
import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_SYNC_REST_CATALOG_URL;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_BASE_PATH;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_DATABASE_NAME;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_PARTITION_EXTRACTOR_CLASS;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_PARTITION_FIELDS;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for RESTCatalogDDLExecutor using Java's built-in HttpServer.
 * Pattern based on TestTimelineService.java
 */
public class TestRestCatalogDDLExecutor {

  private MockRestServer mockServer;
  private RESTCatalogDDLExecutor executor;
  private HiveSyncConfig config;

  @BeforeEach
  public void setup() throws IOException {
    // Start mock HTTP server using Java's built-in HttpServer (no dependencies!)
    mockServer = new MockRestServer();
    mockServer.start();

    // Create test config
    Properties props = new Properties();
    props.setProperty(HIVE_SYNC_REST_CATALOG_URL.key(), mockServer.getBaseUrl());
    props.setProperty(HIVE_SYNC_REST_CATALOG_AUTH_TYPE.key(), "NONE");
    props.setProperty(HIVE_SYNC_REST_CATALOG_TIMEOUT_SECONDS.key(), "10");
    props.setProperty(META_SYNC_DATABASE_NAME.key(), "test_db");
    props.setProperty(META_SYNC_BASE_PATH.key(), "/tmp/test");
    props.setProperty(META_SYNC_PARTITION_FIELDS.key(), "");
    props.setProperty(META_SYNC_PARTITION_EXTRACTOR_CLASS.key(),
        "org.apache.hudi.hive.MultiPartKeysValueExtractor");

    config = new HiveSyncConfig(props);
    executor = new RESTCatalogDDLExecutor(config);
  }

  @AfterEach
  public void teardown() {
    if (executor != null) {
      executor.close();
    }
    if (mockServer != null) {
      mockServer.stop();
    }
  }

  @Test
  public void testCreateDatabase() {
    // Mock successful namespace creation
    mockServer.stubCreateNamespace("test_db");

    // Execute
    assertDoesNotThrow(() -> executor.createDatabase("test_db"));
  }

  @Test
  public void testCreateTable() throws Exception {
    // Mock table creation
    mockServer.stubCreateTable("test_db", "test_table");

    // Create test schema
    HoodieSchema schema = SchemaTestUtil.getSimpleSchema();
    Map<String, String> serdeProps = new HashMap<>();
    serdeProps.put("serialization.format", "1");
    Map<String, String> tableProps = new HashMap<>();
    tableProps.put("EXTERNAL", "TRUE");

    // Execute
    assertDoesNotThrow(() -> {
      executor.createTable("test_table", schema,
          "org.apache.hadoop.mapred.TextInputFormat",
          "org.apache.hadoop.mapred.TextOutputFormat",
          "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
          serdeProps, tableProps);
    });
  }

  @Test
  public void testUpdateTableDefinition() throws Exception {
    // Mock table update
    mockServer.stubUpdateTable("test_db", "test_table");

    // Create updated schema
    HoodieSchema schema = SchemaTestUtil.getEvolvedSchema();

    // Execute
    assertDoesNotThrow(() -> executor.updateTableDefinition("test_table", schema));
  }

  @Test
  public void testGetTableSchema() {
    // Mock table metadata response
    mockServer.stubGetTable("test_db", "test_table", true);

    // Execute
    Map<String, String> schema = executor.getTableSchema("test_table");

    // Verify
    assertNotNull(schema);
    assertTrue(schema.containsKey("id"));
    assertTrue(schema.containsKey("name"));
  }

  @Test
  public void testGetTableSchemaNotFound() {
    // Mock table not found
    mockServer.stubGetTable("test_db", "nonexistent_table", false);

    // Execute and expect exception
    assertThrows(Exception.class, () -> {
      executor.getTableSchema("nonexistent_table");
    });
  }

  @Test
  public void testUpdateTableComments() {
    // First, mock getTable response
    mockServer.stubGetTable("test_db", "test_table", true);

    // Then mock update response
    mockServer.stubUpdateTable("test_db", "test_table");

    // Create comments map
    Map<String, Pair<String, String>> alterSchema = new HashMap<>();
    alterSchema.put("id", Pair.of("string", "Updated comment"));

    // Execute
    assertDoesNotThrow(() -> executor.updateTableComments("test_table", alterSchema));
  }

  @Test
  public void testPartitionOperationsDoNotFail() {
    // Partition operations should not throw exceptions
    // REST Catalog handles partitions differently
    assertDoesNotThrow(() -> {
      executor.addPartitionsToTable("test_table", Collections.singletonList("2023-01-01"));
      executor.updatePartitionsToTable("test_table", Collections.singletonList("2023-01-01"));
      executor.dropPartitionsToTable("test_table", Collections.singletonList("2023-01-01"));
    });
  }

  @Test
  public void testBearerAuthentication() throws IOException {
    // Create config with Bearer token
    Properties props = new Properties();
    props.setProperty(HIVE_SYNC_REST_CATALOG_URL.key(), mockServer.getBaseUrl());
    props.setProperty(HIVE_SYNC_REST_CATALOG_AUTH_TYPE.key(), "BEARER");
    props.setProperty(HIVE_SYNC_REST_CATALOG_AUTH_TOKEN.key(), "test-token-123");
    props.setProperty(META_SYNC_DATABASE_NAME.key(), "test_db");
    props.setProperty(META_SYNC_BASE_PATH.key(), "/tmp/test");
    props.setProperty(META_SYNC_PARTITION_FIELDS.key(), "");
    props.setProperty(META_SYNC_PARTITION_EXTRACTOR_CLASS.key(),
        "org.apache.hudi.hive.MultiPartKeysValueExtractor");

    HiveSyncConfig authConfig = new HiveSyncConfig(props);
    RESTCatalogDDLExecutor authExecutor = new RESTCatalogDDLExecutor(authConfig);

    // Mock response
    mockServer.stubCreateTable("test_db", "test_table");

    // Execute
    HoodieSchema schema = SchemaTestUtil.getSimpleSchema();
    assertDoesNotThrow(() -> {
      authExecutor.createTable("test_table", schema, "in", "out", "serde",
          Collections.emptyMap(), Collections.emptyMap());
    });

    authExecutor.close();
  }

  @Test
  public void testErrorHandling() {
    // Mock error response
    mockServer.stubError("/api/v1/namespaces/test_db", 500, "Internal Server Error");

    // Execute and expect exception
    assertThrows(Exception.class, () -> {
      executor.createDatabase("test_db");
    });
  }

  @Test
  @org.junit.jupiter.api.Timeout(value = 10, unit = java.util.concurrent.TimeUnit.SECONDS)
  public void testInvalidUrl() {
    // Create executor with invalid URL - use TEST-NET-1 address
    Properties props = new Properties();
    props.setProperty(HIVE_SYNC_REST_CATALOG_URL.key(), "http://192.0.2.1:9999/api/v1");
    props.setProperty(HIVE_SYNC_REST_CATALOG_AUTH_TYPE.key(), "NONE");
    props.setProperty(HIVE_SYNC_REST_CATALOG_TIMEOUT_SECONDS.key(), "2");
    props.setProperty(META_SYNC_DATABASE_NAME.key(), "test_db");
    props.setProperty(META_SYNC_BASE_PATH.key(), "/tmp/test");
    props.setProperty(META_SYNC_PARTITION_FIELDS.key(), "");
    props.setProperty(META_SYNC_PARTITION_EXTRACTOR_CLASS.key(),
        "org.apache.hudi.hive.MultiPartKeysValueExtractor");

    HiveSyncConfig invalidConfig = new HiveSyncConfig(props);
    RESTCatalogDDLExecutor invalidExecutor = new RESTCatalogDDLExecutor(invalidConfig);

    // Should timeout/fail
    assertThrows(Exception.class, () -> {
      invalidExecutor.createDatabase("test_db");
    });

    invalidExecutor.close();
  }
}
