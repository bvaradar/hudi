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

import org.apache.hudi.hive.rest.model.CreateTableRequest;
import org.apache.hudi.hive.rest.model.PartitionSpec;
import org.apache.hudi.hive.rest.model.Schema;
import org.apache.hudi.hive.rest.model.TableMetadata;
import org.apache.hudi.hive.rest.model.UpdateTableRequest;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Tests for HiveRestCatalogClient using Java's built-in HttpServer.
 */
public class TestHiveRestCatalogClient {

  private MockRestServer mockServer;
  private HiveRestCatalogClient client;

  @BeforeEach
  public void setup() throws IOException {
    mockServer = new MockRestServer();
    mockServer.start();

    client = new HiveRestCatalogClient(
        mockServer.getBaseUrl(),
        30, // timeout seconds
        HiveRestCatalogClient.AuthType.NONE,
        null, null, null, null, null, null, null
    );
  }

  @AfterEach
  public void teardown() throws IOException {
    if (client != null) {
      client.close();
    }
    if (mockServer != null) {
      mockServer.stop();
    }
  }

  @Test
  public void testNamespaceExists() {
    mockServer.stubNamespaceExists("test_db", true);
    assertTrue(client.namespaceExists("test_db"));
  }

  @Test
  public void testNamespaceDoesNotExist() {
    mockServer.stubNamespaceExists("nonexistent_db", false);
    assertFalse(client.namespaceExists("nonexistent_db"));
  }

  @Test
  public void testCreateNamespace() {
    mockServer.stubCreateNamespace("new_db");

    Map<String, String> properties = new HashMap<>();
    properties.put("owner", "test_user");

    assertDoesNotThrow(() -> client.createNamespace("new_db", properties));
  }

  @Test
  public void testGetTableExists() {
    mockServer.stubGetTable("test_db", "test_table", true);

    TableMetadata metadata = client.getTable("test_db", "test_table");
    assertNotNull(metadata);
    assertNotNull(metadata.getSchema());
  }

  @Test
  public void testGetTableNotFound() {
    mockServer.stubGetTable("test_db", "nonexistent_table", false);

    TableMetadata metadata = client.getTable("test_db", "nonexistent_table");
    assertNull(metadata);
  }

  @Test
  public void testCreateTable() {
    mockServer.stubCreateTable("test_db", "new_table");

    CreateTableRequest request = new CreateTableRequest();
    request.setName("new_table");
    request.setLocation("/tmp/test");

    Schema schema = new Schema(0, new ArrayList<>());
    request.setSchema(schema);

    PartitionSpec partitionSpec = new PartitionSpec(0, new ArrayList<>());
    request.setPartitionSpec(partitionSpec);

    assertDoesNotThrow(() -> client.createTable("test_db", request));
  }

  @Test
  public void testUpdateTable() {
    mockServer.stubUpdateTable("test_db", "test_table");

    UpdateTableRequest request = new UpdateTableRequest();
    UpdateTableRequest.SetPropertiesUpdate update = new UpdateTableRequest.SetPropertiesUpdate();
    Map<String, String> props = new HashMap<>();
    props.put("key", "value");
    update.setUpdates(props);
    request.addUpdate(update);

    assertDoesNotThrow(() -> client.updateTable("test_db", "test_table", request));
  }

  @Test
  public void testDropTable() {
    mockServer.stubResponse("/api/v1/namespaces/test_db/tables/test_table", "DELETE", 204, "");

    assertDoesNotThrow(() -> client.dropTable("test_db", "test_table"));
  }

  @Test
  public void testBearerAuthentication() throws IOException {
    HiveRestCatalogClient authClient = new HiveRestCatalogClient(
        mockServer.getBaseUrl(),
        30,
        HiveRestCatalogClient.AuthType.BEARER,
        "test-token-123",
        null, null, null, null, null, null
    );

    mockServer.stubNamespaceExists("test_db", true);

    assertTrue(authClient.namespaceExists("test_db"));
    authClient.close();
  }

  @Test
  public void testBasicAuthentication() throws IOException {
    HiveRestCatalogClient authClient = new HiveRestCatalogClient(
        mockServer.getBaseUrl(),
        30,
        HiveRestCatalogClient.AuthType.BASIC,
        null,
        "testuser", "testpass",
        null, null, null, null
    );

    mockServer.stubNamespaceExists("test_db", true);

    assertTrue(authClient.namespaceExists("test_db"));
    authClient.close();
  }

  @Test
  public void testOAuth2Authentication() throws IOException {
    // Stub OAuth2 token endpoint
    mockServer.stubOAuth2TokenResponse("/oauth/token", "access-token-123", 3600);

    HiveRestCatalogClient authClient = new HiveRestCatalogClient(
        mockServer.getBaseUrl(),
        30,
        HiveRestCatalogClient.AuthType.OAUTH2,
        null, null, null,
        "http://localhost:" + mockServer.getPort() + "/oauth/token",
        "client-id",
        "client-secret",
        "catalog"
    );

    mockServer.stubNamespaceExists("test_db", true);

    assertTrue(authClient.namespaceExists("test_db"));
    authClient.close();
  }

  @Test
  public void testErrorHandling() {
    mockServer.stubError("/api/v1/namespaces/test_db", 500, "Internal Server Error");

    assertThrows(HiveRestCatalogException.class, () -> {
      client.createNamespace("test_db", null);
    });
  }

  @Test
  @org.junit.jupiter.api.Timeout(value = 10, unit = java.util.concurrent.TimeUnit.SECONDS)
  public void testInvalidUrl() {
    // Use TEST-NET-1 address (192.0.2.1) which is guaranteed to be unreachable
    HiveRestCatalogClient invalidClient = new HiveRestCatalogClient(
        "http://192.0.2.1:9999/api/v1",
        2, // short timeout
        HiveRestCatalogClient.AuthType.NONE,
        null, null, null, null, null, null, null
    );

    // namespaceExists catches IOException and returns false (doesn't throw)
    assertFalse(invalidClient.namespaceExists("test_db"));

    assertDoesNotThrow(() -> invalidClient.close());
  }

  @Test
  public void testMissingRequiredAuthConfig() {
    // Bearer auth without token should fail
    assertThrows(IllegalArgumentException.class, () -> {
      new HiveRestCatalogClient(
          mockServer.getBaseUrl(),
          30,
          HiveRestCatalogClient.AuthType.BEARER,
          null, // missing token
          null, null, null, null, null, null
      );
    });

    // Basic auth without username should fail
    assertThrows(IllegalArgumentException.class, () -> {
      new HiveRestCatalogClient(
          mockServer.getBaseUrl(),
          30,
          HiveRestCatalogClient.AuthType.BASIC,
          null,
          null, // missing username
          "password",
          null, null, null, null
      );
    });

    // OAuth2 without token URL should fail
    assertThrows(IllegalArgumentException.class, () -> {
      new HiveRestCatalogClient(
          mockServer.getBaseUrl(),
          30,
          HiveRestCatalogClient.AuthType.OAUTH2,
          null, null, null,
          null, // missing token URL
          "client-id",
          "client-secret",
          "catalog"
      );
    });
  }
}
