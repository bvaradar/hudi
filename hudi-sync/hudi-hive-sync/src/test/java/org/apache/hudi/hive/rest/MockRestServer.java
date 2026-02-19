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

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * Helper class for mocking REST Catalog HTTP endpoints in tests.
 * Uses Java's built-in HttpServer (no external dependencies).
 *
 * Pattern based on TestTimelineService.java
 */
public class MockRestServer {

  private final HttpServer server;
  private final ObjectMapper objectMapper;
  private final Map<String, MockResponse> responses;
  private int port;

  public MockRestServer() throws IOException {
    this(0); // Random port
  }

  public MockRestServer(int port) throws IOException {
    this.server = HttpServer.create(new InetSocketAddress(port), 0);
    this.objectMapper = new ObjectMapper();
    this.responses = new HashMap<>();
    this.server.setExecutor(null);
    // Create a single context that handles all requests
    this.server.createContext("/", new MockHandler());
  }

  public void start() {
    server.start();
    this.port = server.getAddress().getPort();
  }

  public void stop() {
    server.stop(0);
  }

  public String getBaseUrl() {
    return "http://localhost:" + port + "/api/v1";
  }

  public int getPort() {
    return port;
  }

  /**
   * Register a response for a specific path and HTTP method.
   */
  public void stubResponse(String path, String method, int statusCode, String responseBody) {
    String key = method + " " + path;
    responses.put(key, new MockResponse(statusCode, responseBody));
  }

  /**
   * Stub namespace exists check.
   */
  public void stubNamespaceExists(String namespace, boolean exists) {
    String path = "/api/v1/namespaces/" + namespace;
    if (exists) {
      String responseBody = String.format("{\"namespace\":\"%s\",\"properties\":{}}", namespace);
      stubResponse(path, "GET", 200, responseBody);
    } else {
      stubResponse(path, "GET", 404, "{\"error\":\"Namespace not found\"}");
    }
  }

  /**
   * Stub namespace creation.
   */
  public void stubCreateNamespace(String namespace) {
    String path = "/api/v1/namespaces";
    String responseBody = String.format("{\"namespace\":\"%s\",\"properties\":{}}", namespace);
    stubResponse(path, "POST", 200, responseBody);
  }

  /**
   * Stub table retrieval.
   */
  public void stubGetTable(String namespace, String table, boolean exists) {
    String path = "/api/v1/namespaces/" + namespace + "/tables/" + table;
    if (exists) {
      String responseBody = "{"
          + "\"metadata\":{"
          + "  \"format-version\":1,"
          + "  \"table-uuid\":\"test-uuid\","
          + "  \"location\":\"/tmp/test\","
          + "  \"schema\":{"
          + "    \"type\":\"struct\","
          + "    \"schema-id\":0,"
          + "    \"fields\":["
          + "      {\"id\":0,\"name\":\"id\",\"type\":\"string\",\"required\":false},"
          + "      {\"id\":1,\"name\":\"name\",\"type\":\"string\",\"required\":false}"
          + "    ]"
          + "  },"
          + "  \"partition-spec\":{"
          + "    \"spec-id\":0,"
          + "    \"fields\":[]"
          + "  },"
          + "  \"properties\":{"
          + "    \"table_type\":\"EXTERNAL\""
          + "  }"
          + "}"
          + "}";
      stubResponse(path, "GET", 200, responseBody);
    } else {
      stubResponse(path, "GET", 404, "{\"error\":\"Table not found\"}");
    }
  }

  /**
   * Stub table creation.
   */
  public void stubCreateTable(String namespace, String table) {
    String path = "/api/v1/namespaces/" + namespace + "/tables";
    String responseBody = "{"
        + "\"metadata\":{"
        + "  \"format-version\":1,"
        + "  \"table-uuid\":\"test-uuid\","
        + "  \"location\":\"/tmp/test\","
        + "  \"schema\":{\"type\":\"struct\",\"schema-id\":0,\"fields\":[]},"
        + "  \"partition-spec\":{\"spec-id\":0,\"fields\":[]},"
        + "  \"properties\":{}"
        + "}"
        + "}";
    stubResponse(path, "POST", 200, responseBody);
  }

  /**
   * Stub table update.
   */
  public void stubUpdateTable(String namespace, String table) {
    String path = "/api/v1/namespaces/" + namespace + "/tables/" + table;
    String responseBody = "{"
        + "\"metadata\":{"
        + "  \"format-version\":1,"
        + "  \"table-uuid\":\"test-uuid\","
        + "  \"location\":\"/tmp/test\","
        + "  \"schema\":{\"type\":\"struct\",\"schema-id\":1,\"fields\":[]},"
        + "  \"partition-spec\":{\"spec-id\":0,\"fields\":[]},"
        + "  \"properties\":{}"
        + "}"
        + "}";
    stubResponse(path, "POST", 200, responseBody);
  }

  /**
   * Stub OAuth2 token response.
   */
  public void stubOAuth2TokenResponse(String tokenUrl, String accessToken, int expiresIn) {
    Map<String, Object> tokenResponse = new HashMap<>();
    tokenResponse.put("access_token", accessToken);
    tokenResponse.put("token_type", "Bearer");
    tokenResponse.put("expires_in", expiresIn);

    try {
      String responseBody = objectMapper.writeValueAsString(tokenResponse);
      stubResponse(tokenUrl, "POST", 200, responseBody);
    } catch (Exception e) {
      throw new RuntimeException("Failed to create OAuth2 token response", e);
    }
  }

  /**
   * Stub error response.
   */
  public void stubError(String path, int statusCode, String errorMessage) {
    String responseBody = String.format("{\"error\":\"%s\"}", errorMessage);
    stubResponse(path, "GET", statusCode, responseBody);
    stubResponse(path, "POST", statusCode, responseBody);
  }

  /**
   * Mock response holder.
   */
  private static class MockResponse {
    final int statusCode;
    final String body;

    MockResponse(int statusCode, String body) {
      this.statusCode = statusCode;
      this.body = body;
    }
  }

  /**
   * HTTP handler that returns mocked responses based on request path and method.
   * Pattern based on TestTimelineService.MyHandler
   */
  private class MockHandler implements HttpHandler {
    @Override
    public void handle(HttpExchange exchange) throws IOException {
      String requestPath = exchange.getRequestURI().getPath();
      String requestMethod = exchange.getRequestMethod();
      String key = requestMethod + " " + requestPath;

      MockResponse response = responses.get(key);
      if (response == null) {
        response = new MockResponse(404, "{\"error\":\"Not Found\"}");
      }

      byte[] responseBytes = response.body.getBytes(StandardCharsets.UTF_8);
      exchange.getResponseHeaders().add("Content-Type", "application/json");
      exchange.sendResponseHeaders(response.statusCode, responseBytes.length);
      try (OutputStream os = exchange.getResponseBody()) {
        os.write(responseBytes);
      }
    }
  }
}
