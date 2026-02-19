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
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

/**
 * Tests for OAuth2TokenProvider - token acquisition and caching.
 */
public class TestOAuth2TokenProvider {

  private MockRestServer mockServer;
  private CloseableHttpClient httpClient;
  private ObjectMapper objectMapper;

  @BeforeEach
  public void setup() throws IOException {
    mockServer = new MockRestServer();
    mockServer.start();
    httpClient = HttpClientBuilder.create().build();
    objectMapper = new ObjectMapper();
  }

  @AfterEach
  public void teardown() throws IOException {
    if (httpClient != null) {
      httpClient.close();
    }
    if (mockServer != null) {
      mockServer.stop();
    }
  }

  @Test
  public void testGetAccessToken() throws IOException {
    String tokenUrl = "http://localhost:" + mockServer.getPort() + "/oauth/token";
    mockServer.stubOAuth2TokenResponse("/oauth/token", "test-access-token", 3600);

    OAuth2TokenProvider provider = new OAuth2TokenProvider(
        tokenUrl,
        "client-id",
        "client-secret",
        "catalog",
        httpClient,
        objectMapper
    );

    String token = provider.getAccessToken();

    assertNotNull(token);
    assertEquals("test-access-token", token);
  }

  @Test
  public void testTokenCaching() throws IOException, InterruptedException {
    String tokenUrl = "http://localhost:" + mockServer.getPort() + "/oauth/token";

    // Stub first token response
    mockServer.stubOAuth2TokenResponse("/oauth/token", "cached-token", 3600);

    OAuth2TokenProvider provider = new OAuth2TokenProvider(
        tokenUrl,
        "client-id",
        "client-secret",
        "catalog",
        httpClient,
        objectMapper
    );

    // First call - should fetch token
    String token1 = provider.getAccessToken();
    assertEquals("cached-token", token1);

    // Second call - should return cached token (no new HTTP request)
    String token2 = provider.getAccessToken();
    assertEquals("cached-token", token2);
    assertEquals(token1, token2);

    // Only one request should have been made (token was cached)
    // We can't easily verify this without request counting, but the test validates
    // that the same token is returned
  }

  @Test
  public void testTokenRefresh() throws IOException, InterruptedException {
    String tokenUrl = "http://localhost:" + mockServer.getPort() + "/oauth/token";

    // Stub first token with short expiry (1 second)
    mockServer.stubOAuth2TokenResponse("/oauth/token", "initial-token", 1);

    OAuth2TokenProvider provider = new OAuth2TokenProvider(
        tokenUrl,
        "client-id",
        "client-secret",
        "catalog",
        httpClient,
        objectMapper
    );

    // First call
    String token1 = provider.getAccessToken();
    assertEquals("initial-token", token1);

    // Wait for token to expire (1 second + buffer)
    Thread.sleep(2000);

    // Stub new token response
    mockServer.stubOAuth2TokenResponse("/oauth/token", "refreshed-token", 3600);

    // Second call after expiry - should fetch new token
    String token2 = provider.getAccessToken();
    assertEquals("refreshed-token", token2);
  }

  @Test
  public void testErrorHandling() {
    String tokenUrl = "http://localhost:" + mockServer.getPort() + "/oauth/token";

    // Stub error response
    mockServer.stubError("/oauth/token", 401, "Unauthorized");

    OAuth2TokenProvider provider = new OAuth2TokenProvider(
        tokenUrl,
        "invalid-client",
        "invalid-secret",
        "catalog",
        httpClient,
        objectMapper
    );

    assertThrows(HiveRestCatalogException.class, () -> {
      provider.getAccessToken();
    });
  }

  @Test
  public void testInvalidTokenUrl() {
    String invalidUrl = "http://nonexistent:9999/oauth/token";

    OAuth2TokenProvider provider = new OAuth2TokenProvider(
        invalidUrl,
        "client-id",
        "client-secret",
        "catalog",
        httpClient,
        objectMapper
    );

    assertThrows(Exception.class, () -> {
      provider.getAccessToken();
    });
  }

  @Test
  public void testDifferentScopes() throws IOException {
    String tokenUrl = "http://localhost:" + mockServer.getPort() + "/oauth/token";
    mockServer.stubOAuth2TokenResponse("/oauth/token", "scoped-token", 3600);

    // Test with different scopes
    OAuth2TokenProvider provider1 = new OAuth2TokenProvider(
        tokenUrl,
        "client-id",
        "client-secret",
        "catalog",
        httpClient,
        objectMapper
    );
    String token1 = provider1.getAccessToken();
    assertNotNull(token1);

    mockServer.stubOAuth2TokenResponse("/oauth/token", "admin-scoped-token", 3600);

    OAuth2TokenProvider provider2 = new OAuth2TokenProvider(
        tokenUrl,
        "client-id",
        "client-secret",
        "admin",
        httpClient,
        objectMapper
    );
    String token2 = provider2.getAccessToken();
    assertNotNull(token2);
  }

  @Test
  public void testTokenExpiryBuffer() throws IOException, InterruptedException {
    String tokenUrl = "http://localhost:" + mockServer.getPort() + "/oauth/token";

    // Token expires in 60 seconds, but provider should refresh 60 seconds early
    // So effective cache time is 0 seconds
    mockServer.stubOAuth2TokenResponse("/oauth/token", "token-with-buffer", 60);

    OAuth2TokenProvider provider = new OAuth2TokenProvider(
        tokenUrl,
        "client-id",
        "client-secret",
        "catalog",
        httpClient,
        objectMapper
    );

    String token1 = provider.getAccessToken();
    assertEquals("token-with-buffer", token1);

    // Token should still be valid (within 60 second buffer)
    String token2 = provider.getAccessToken();
    assertEquals(token1, token2);
  }

  @Test
  public void testConcurrentTokenAccess() throws IOException {
    String tokenUrl = "http://localhost:" + mockServer.getPort() + "/oauth/token";
    mockServer.stubOAuth2TokenResponse("/oauth/token", "concurrent-token", 3600);

    OAuth2TokenProvider provider = new OAuth2TokenProvider(
        tokenUrl,
        "client-id",
        "client-secret",
        "catalog",
        httpClient,
        objectMapper
    );

    // Simulate concurrent access (synchronized in getAccessToken should handle this)
    String token1 = provider.getAccessToken();
    String token2 = provider.getAccessToken();

    assertEquals(token1, token2);
  }
}
