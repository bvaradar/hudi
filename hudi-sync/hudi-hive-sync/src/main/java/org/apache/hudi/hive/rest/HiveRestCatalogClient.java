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

import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.hive.rest.model.CreateTableRequest;
import org.apache.hudi.hive.rest.model.Namespace;
import org.apache.hudi.hive.rest.model.TableMetadata;
import org.apache.hudi.hive.rest.model.UpdateTableRequest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpHeaders;
import org.apache.http.HttpStatus;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpDelete;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.util.EntityUtils;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

/**
 * HTTP client for REST Catalog operations.
 * Supports Bearer, Basic, and OAuth2 authentication.
 */
@Slf4j
public class HiveRestCatalogClient implements Closeable {

  private final String baseUrl;
  private final CloseableHttpClient httpClient;
  private final ObjectMapper objectMapper;
  private final AuthType authType;
  private final String authToken;
  private final String basicAuthHeader;
  private final OAuth2TokenProvider oauth2TokenProvider;

  public HiveRestCatalogClient(String baseUrl, int timeoutSeconds, AuthType authType,
                                String token, String username, String password,
                                String oauth2TokenUrl, String oauth2ClientId,
                                String oauth2ClientSecret, String oauth2Scope) {
    ValidationUtils.checkArgument(!StringUtils.isNullOrEmpty(baseUrl), "REST Catalog URL cannot be null or empty");
    this.baseUrl = baseUrl.endsWith("/") ? baseUrl.substring(0, baseUrl.length() - 1) : baseUrl;
    this.authType = authType;
    this.objectMapper = new ObjectMapper();

    RequestConfig requestConfig = RequestConfig.custom()
        .setConnectTimeout(timeoutSeconds * 1000)
        .setConnectionRequestTimeout(timeoutSeconds * 1000)
        .setSocketTimeout(timeoutSeconds * 1000)
        .build();

    this.httpClient = HttpClientBuilder.create()
        .setDefaultRequestConfig(requestConfig)
        .build();

    switch (authType) {
      case BEARER:
        ValidationUtils.checkArgument(!StringUtils.isNullOrEmpty(token), "Bearer token required for BEARER auth type");
        this.authToken = token;
        this.basicAuthHeader = null;
        this.oauth2TokenProvider = null;
        break;
      case BASIC:
        ValidationUtils.checkArgument(!StringUtils.isNullOrEmpty(username), "Username required for BASIC auth type");
        ValidationUtils.checkArgument(!StringUtils.isNullOrEmpty(password), "Password required for BASIC auth type");
        this.authToken = null;
        this.basicAuthHeader = "Basic " + Base64.getEncoder().encodeToString(
            (username + ":" + password).getBytes(StandardCharsets.UTF_8));
        this.oauth2TokenProvider = null;
        break;
      case OAUTH2:
        ValidationUtils.checkArgument(!StringUtils.isNullOrEmpty(oauth2TokenUrl), "Token URL required for OAUTH2 auth type");
        ValidationUtils.checkArgument(!StringUtils.isNullOrEmpty(oauth2ClientId), "Client ID required for OAUTH2 auth type");
        ValidationUtils.checkArgument(!StringUtils.isNullOrEmpty(oauth2ClientSecret), "Client secret required for OAUTH2 auth type");
        this.authToken = null;
        this.basicAuthHeader = null;
        this.oauth2TokenProvider = new OAuth2TokenProvider(oauth2TokenUrl, oauth2ClientId, oauth2ClientSecret,
            oauth2Scope != null ? oauth2Scope : "catalog", httpClient, objectMapper);
        break;
      case NONE:
      default:
        this.authToken = null;
        this.basicAuthHeader = null;
        this.oauth2TokenProvider = null;
        break;
    }
  }

  private void setAuthHeader(HttpUriRequest request) throws IOException {
    switch (authType) {
      case BEARER:
        request.setHeader(HttpHeaders.AUTHORIZATION, "Bearer " + authToken);
        break;
      case BASIC:
        request.setHeader(HttpHeaders.AUTHORIZATION, basicAuthHeader);
        break;
      case OAUTH2:
        String token = oauth2TokenProvider.getAccessToken();
        request.setHeader(HttpHeaders.AUTHORIZATION, "Bearer " + token);
        break;
      case NONE:
      default:
        // No auth header
        break;
    }
  }

  public boolean namespaceExists(String namespace) {
    String url = String.format("%s/namespaces/%s", baseUrl, namespace);
    HttpGet request = new HttpGet(url);
    try {
      setAuthHeader(request);
      try (CloseableHttpResponse response = httpClient.execute(request)) {
        int statusCode = response.getStatusLine().getStatusCode();
        return statusCode == HttpStatus.SC_OK;
      }
    } catch (IOException e) {
      log.warn("Failed to check if namespace {} exists", namespace, e);
      return false;
    }
  }

  public void createNamespace(String namespace, Map<String, String> properties) {
    String url = String.format("%s/namespaces", baseUrl);
    HttpPost request = new HttpPost(url);
    try {
      setAuthHeader(request);
      request.setHeader(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.toString());

      Namespace ns = new Namespace(namespace, properties != null ? properties : new HashMap<>());
      String payload = objectMapper.writeValueAsString(ns);
      request.setEntity(new StringEntity(payload, ContentType.APPLICATION_JSON));

      try (CloseableHttpResponse response = httpClient.execute(request)) {
        int statusCode = response.getStatusLine().getStatusCode();
        if (statusCode >= 300) {
          String errorBody = response.getEntity() != null ? EntityUtils.toString(response.getEntity()) : "No response body";
          throw new HiveRestCatalogException("Failed to create namespace " + namespace + ": " + statusCode + " - " + errorBody);
        }
        log.info("Created namespace: {}", namespace);
      }
    } catch (IOException e) {
      throw new HiveRestCatalogException("Failed to create namespace " + namespace, e);
    }
  }

  public TableMetadata getTable(String namespace, String tableName) {
    String url = String.format("%s/namespaces/%s/tables/%s", baseUrl, namespace, tableName);
    HttpGet request = new HttpGet(url);
    try {
      setAuthHeader(request);
      try (CloseableHttpResponse response = httpClient.execute(request)) {
        int statusCode = response.getStatusLine().getStatusCode();
        if (statusCode == HttpStatus.SC_NOT_FOUND) {
          return null;
        }
        if (statusCode >= 300) {
          String errorBody = response.getEntity() != null ? EntityUtils.toString(response.getEntity()) : "No response body";
          throw new HiveRestCatalogException("Failed to get table " + namespace + "." + tableName + ": " + statusCode + " - " + errorBody);
        }
        String responseBody = EntityUtils.toString(response.getEntity());
        JsonNode rootNode = objectMapper.readTree(responseBody);
        JsonNode metadataNode = rootNode.has("metadata") ? rootNode.get("metadata") : rootNode;
        return objectMapper.treeToValue(metadataNode, TableMetadata.class);
      }
    } catch (IOException e) {
      throw new HiveRestCatalogException("Failed to get table " + namespace + "." + tableName, e);
    }
  }

  public void createTable(String namespace, CreateTableRequest createRequest) {
    String url = String.format("%s/namespaces/%s/tables", baseUrl, namespace);
    HttpPost request = new HttpPost(url);
    try {
      setAuthHeader(request);
      request.setHeader(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.toString());

      String payload = objectMapper.writeValueAsString(createRequest);
      request.setEntity(new StringEntity(payload, ContentType.APPLICATION_JSON));

      try (CloseableHttpResponse response = httpClient.execute(request)) {
        int statusCode = response.getStatusLine().getStatusCode();
        if (statusCode >= 300) {
          String errorBody = response.getEntity() != null ? EntityUtils.toString(response.getEntity()) : "No response body";
          throw new HiveRestCatalogException("Failed to create table " + namespace + "." + createRequest.getName()
              + ": " + statusCode + " - " + errorBody);
        }
        log.info("Created table: {}.{}", namespace, createRequest.getName());
      }
    } catch (IOException e) {
      throw new HiveRestCatalogException("Failed to create table " + namespace + "." + createRequest.getName(), e);
    }
  }

  public void updateTable(String namespace, String tableName, UpdateTableRequest updateRequest) {
    String url = String.format("%s/namespaces/%s/tables/%s", baseUrl, namespace, tableName);
    HttpPost request = new HttpPost(url);
    try {
      setAuthHeader(request);
      request.setHeader(HttpHeaders.CONTENT_TYPE, ContentType.APPLICATION_JSON.toString());

      String payload = objectMapper.writeValueAsString(updateRequest);
      request.setEntity(new StringEntity(payload, ContentType.APPLICATION_JSON));

      try (CloseableHttpResponse response = httpClient.execute(request)) {
        int statusCode = response.getStatusLine().getStatusCode();
        if (statusCode >= 300) {
          String errorBody = response.getEntity() != null ? EntityUtils.toString(response.getEntity()) : "No response body";
          throw new HiveRestCatalogException("Failed to update table " + namespace + "." + tableName
              + ": " + statusCode + " - " + errorBody);
        }
        log.info("Updated table: {}.{}", namespace, tableName);
      }
    } catch (IOException e) {
      throw new HiveRestCatalogException("Failed to update table " + namespace + "." + tableName, e);
    }
  }

  public void dropTable(String namespace, String tableName) {
    String url = String.format("%s/namespaces/%s/tables/%s", baseUrl, namespace, tableName);
    HttpDelete request = new HttpDelete(url);
    try {
      setAuthHeader(request);
      try (CloseableHttpResponse response = httpClient.execute(request)) {
        int statusCode = response.getStatusLine().getStatusCode();
        if (statusCode >= 300 && statusCode != HttpStatus.SC_NOT_FOUND) {
          String errorBody = response.getEntity() != null ? EntityUtils.toString(response.getEntity()) : "No response body";
          throw new HiveRestCatalogException("Failed to drop table " + namespace + "." + tableName
              + ": " + statusCode + " - " + errorBody);
        }
        log.info("Dropped table: {}.{}", namespace, tableName);
      }
    } catch (IOException e) {
      throw new HiveRestCatalogException("Failed to drop table " + namespace + "." + tableName, e);
    }
  }

  @Override
  public void close() throws IOException {
    if (httpClient != null) {
      httpClient.close();
    }
  }

  public enum AuthType {
    NONE,
    BEARER,
    BASIC,
    OAUTH2
  }
}
