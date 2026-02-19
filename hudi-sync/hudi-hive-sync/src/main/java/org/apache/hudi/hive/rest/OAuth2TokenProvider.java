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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpStatus;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;

import java.io.IOException;

/**
 * Handles OAuth2 token acquisition and refresh for REST Catalog authentication.
 */
@Slf4j
class OAuth2TokenProvider {

  private final String tokenUrl;
  private final String clientId;
  private final String clientSecret;
  private final String scope;
  private final CloseableHttpClient httpClient;
  private final ObjectMapper objectMapper;

  private String cachedToken;
  private long tokenExpiryTime;

  public OAuth2TokenProvider(String tokenUrl, String clientId, String clientSecret, String scope,
                              CloseableHttpClient httpClient, ObjectMapper objectMapper) {
    this.tokenUrl = tokenUrl;
    this.clientId = clientId;
    this.clientSecret = clientSecret;
    this.scope = scope;
    this.httpClient = httpClient;
    this.objectMapper = objectMapper;
    this.tokenExpiryTime = 0;
  }

  public synchronized String getAccessToken() throws IOException {
    if (cachedToken != null && System.currentTimeMillis() < tokenExpiryTime) {
      return cachedToken;
    }

    HttpPost request = new HttpPost(tokenUrl);
    request.setHeader("Content-Type", "application/x-www-form-urlencoded");

    String body = String.format("grant_type=client_credentials&client_id=%s&client_secret=%s&scope=%s",
        clientId, clientSecret, scope);
    request.setEntity(new StringEntity(body, ContentType.APPLICATION_FORM_URLENCODED));

    try (CloseableHttpResponse response = httpClient.execute(request)) {
      int statusCode = response.getStatusLine().getStatusCode();
      if (statusCode != HttpStatus.SC_OK) {
        String errorBody = response.getEntity() != null ? EntityUtils.toString(response.getEntity()) : "No response body";
        throw new HiveRestCatalogException("Failed to obtain OAuth2 token: " + statusCode + " - " + errorBody);
      }

      String responseBody = EntityUtils.toString(response.getEntity());
      JsonNode jsonNode = objectMapper.readTree(responseBody);

      cachedToken = jsonNode.get("access_token").asText();
      int expiresIn = jsonNode.has("expires_in") ? jsonNode.get("expires_in").asInt() : 3600;
      tokenExpiryTime = System.currentTimeMillis() + (expiresIn - 60) * 1000L;

      log.debug("Obtained new OAuth2 access token, expires in {} seconds", expiresIn);
      return cachedToken;
    }
  }
}
