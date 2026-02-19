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

package org.apache.hudi.hive.ddl;

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.common.util.StringUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.hive.HiveSyncConfig;
import org.apache.hudi.hive.HoodieHiveSyncException;
import org.apache.hudi.hive.rest.HiveRestCatalogClient;
import org.apache.hudi.hive.rest.RestSchemaUtil;
import org.apache.hudi.hive.rest.model.CreateTableRequest;
import org.apache.hudi.hive.rest.model.PartitionSpec;
import org.apache.hudi.hive.rest.model.Schema;
import org.apache.hudi.hive.rest.model.TableMetadata;
import org.apache.hudi.hive.rest.model.UpdateTableRequest;
import org.apache.hudi.sync.common.model.PartitionValueExtractor;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_SYNC_REST_CATALOG_AUTH_PASSWORD;
import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_SYNC_REST_CATALOG_AUTH_TOKEN;
import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_SYNC_REST_CATALOG_AUTH_TYPE;
import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_SYNC_REST_CATALOG_AUTH_USERNAME;
import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_SYNC_REST_CATALOG_OAUTH2_CLIENT_ID;
import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_SYNC_REST_CATALOG_OAUTH2_CLIENT_SECRET;
import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_SYNC_REST_CATALOG_OAUTH2_SCOPE;
import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_SYNC_REST_CATALOG_OAUTH2_TOKEN_URL;
import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_SYNC_REST_CATALOG_TIMEOUT_SECONDS;
import static org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_SYNC_REST_CATALOG_URL;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_BASE_PATH;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_DATABASE_NAME;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_PARTITION_EXTRACTOR_CLASS;
import static org.apache.hudi.sync.common.HoodieSyncConfig.META_SYNC_PARTITION_FIELDS;

/**
 * DDLExecutor implementation using REST Catalog HTTP API.
 * Compatible with Hive 4.2+ REST Catalog.
 */
@Slf4j
public class RESTCatalogDDLExecutor implements DDLExecutor {

  private final HiveSyncConfig syncConfig;
  private final String databaseName;
  private final HiveRestCatalogClient restClient;
  private final PartitionValueExtractor partitionValueExtractor;

  public RESTCatalogDDLExecutor(HiveSyncConfig syncConfig) {
    this.syncConfig = syncConfig;
    this.databaseName = syncConfig.getStringOrDefault(META_SYNC_DATABASE_NAME);

    String baseUrl = syncConfig.getString(HIVE_SYNC_REST_CATALOG_URL);
    ValidationUtils.checkArgument(!StringUtils.isNullOrEmpty(baseUrl),
        "REST Catalog URL is required when using REST sync mode");
    int timeoutSeconds = syncConfig.getIntOrDefault(HIVE_SYNC_REST_CATALOG_TIMEOUT_SECONDS);
    String authTypeStr = syncConfig.getStringOrDefault(HIVE_SYNC_REST_CATALOG_AUTH_TYPE);
    HiveRestCatalogClient.AuthType authType = HiveRestCatalogClient.AuthType.valueOf(authTypeStr.toUpperCase());

    String token = syncConfig.getString(HIVE_SYNC_REST_CATALOG_AUTH_TOKEN);
    String username = syncConfig.getString(HIVE_SYNC_REST_CATALOG_AUTH_USERNAME);
    String password = syncConfig.getString(HIVE_SYNC_REST_CATALOG_AUTH_PASSWORD);
    String oauth2TokenUrl = syncConfig.getString(HIVE_SYNC_REST_CATALOG_OAUTH2_TOKEN_URL);
    String oauth2ClientId = syncConfig.getString(HIVE_SYNC_REST_CATALOG_OAUTH2_CLIENT_ID);
    String oauth2ClientSecret = syncConfig.getString(HIVE_SYNC_REST_CATALOG_OAUTH2_CLIENT_SECRET);
    String oauth2Scope = syncConfig.getStringOrDefault(HIVE_SYNC_REST_CATALOG_OAUTH2_SCOPE);

    this.restClient = new HiveRestCatalogClient(baseUrl, timeoutSeconds, authType,
        token, username, password, oauth2TokenUrl, oauth2ClientId, oauth2ClientSecret, oauth2Scope);

    try {
      this.partitionValueExtractor =
          (PartitionValueExtractor) Class.forName(syncConfig.getStringOrDefault(META_SYNC_PARTITION_EXTRACTOR_CLASS)).newInstance();
    } catch (Exception e) {
      throw new HoodieHiveSyncException(
          "Failed to initialize PartitionValueExtractor class " + syncConfig.getStringOrDefault(META_SYNC_PARTITION_EXTRACTOR_CLASS), e);
    }
  }

  @Override
  public void createDatabase(String databaseName) {
    try {
      if (!restClient.namespaceExists(databaseName)) {
        Map<String, String> properties = new HashMap<>();
        properties.put("comment", "automatically created by hoodie");
        restClient.createNamespace(databaseName, properties);
        log.info("Created database: {}", databaseName);
      } else {
        log.info("Database {} already exists", databaseName);
      }
    } catch (Exception e) {
      log.error("Failed to create database {}", databaseName, e);
      throw new HoodieHiveSyncException("Failed to create database " + databaseName, e);
    }
  }

  @Override
  public void createTable(String tableName, HoodieSchema storageSchema, String inputFormatClass,
                          String outputFormatClass, String serdeClass,
                          Map<String, String> serdeProperties, Map<String, String> tableProperties) {
    try {
      Schema restSchema = RestSchemaUtil.convertToRestSchema(storageSchema, syncConfig);
      List<String> partitionFields = syncConfig.getSplitStrings(META_SYNC_PARTITION_FIELDS);
      PartitionSpec partitionSpec = RestSchemaUtil.convertToPartitionSpec(partitionFields, restSchema);

      CreateTableRequest createRequest = new CreateTableRequest();
      createRequest.setName(tableName);
      createRequest.setLocation(syncConfig.getString(META_SYNC_BASE_PATH));
      createRequest.setSchema(restSchema);
      createRequest.setPartitionSpec(partitionSpec);

      Map<String, String> properties = new HashMap<>();
      if (tableProperties != null) {
        properties.putAll(tableProperties);
      }
      properties.put("input_format", inputFormatClass);
      properties.put("output_format", outputFormatClass);
      properties.put("serde_class", serdeClass);
      if (serdeProperties != null) {
        serdeProperties.forEach((k, v) -> properties.put("serde." + k, v));
      }
      createRequest.setProperties(properties);

      restClient.createTable(databaseName, createRequest);
      log.info("Created table: {}.{}", databaseName, tableName);
    } catch (Exception e) {
      log.error("Failed to create table {}", tableName, e);
      throw new HoodieHiveSyncException("Failed to create table " + tableName, e);
    }
  }

  @Override
  public void updateTableDefinition(String tableName, HoodieSchema newSchema) {
    try {
      Schema restSchema = RestSchemaUtil.convertToRestSchema(newSchema, syncConfig);

      UpdateTableRequest updateRequest = new UpdateTableRequest();
      UpdateTableRequest.AddSchemaUpdate addSchemaUpdate = new UpdateTableRequest.AddSchemaUpdate();
      addSchemaUpdate.setSchema(restSchema);
      updateRequest.addUpdate(addSchemaUpdate);

      UpdateTableRequest.SetCurrentSchemaUpdate setCurrentSchemaUpdate = new UpdateTableRequest.SetCurrentSchemaUpdate();
      setCurrentSchemaUpdate.setSchemaId(restSchema.getSchemaId());
      updateRequest.addUpdate(setCurrentSchemaUpdate);

      restClient.updateTable(databaseName, tableName, updateRequest);
      log.info("Updated table definition for: {}.{}", databaseName, tableName);
    } catch (Exception e) {
      log.error("Failed to update table for {}", tableName, e);
      throw new HoodieHiveSyncException("Failed to update table for " + tableName, e);
    }
  }

  @Override
  public Map<String, String> getTableSchema(String tableName) {
    try {
      TableMetadata tableMetadata = restClient.getTable(databaseName, tableName);
      if (tableMetadata == null || tableMetadata.getSchema() == null) {
        throw new HoodieHiveSyncException("Table " + tableName + " not found or has no schema");
      }

      Map<String, String> schema = new HashMap<>();
      for (Schema.SchemaField field : tableMetadata.getSchema().getFields()) {
        schema.put(field.getName(), field.getType().toUpperCase());
      }

      if (tableMetadata.getPartitionSpec() != null && tableMetadata.getPartitionSpec().getFields() != null) {
        for (PartitionSpec.PartitionField partField : tableMetadata.getPartitionSpec().getFields()) {
          if (!schema.containsKey(partField.getName())) {
            schema.put(partField.getName(), "STRING");
          }
        }
      }

      return schema;
    } catch (Exception e) {
      throw new HoodieHiveSyncException("Failed to get table schema for : " + tableName, e);
    }
  }

  @Override
  public void addPartitionsToTable(String tableName, List<String> partitionsToAdd) {
    if (partitionsToAdd.isEmpty()) {
      log.info("No partitions to add for {}", tableName);
      return;
    }
    log.warn("REST Catalog partition management is handled via table metadata updates. "
        + "Adding {} partitions to {} - partition metadata will be managed by the catalog.", partitionsToAdd.size(), tableName);
  }

  @Override
  public void updatePartitionsToTable(String tableName, List<String> changedPartitions) {
    if (changedPartitions.isEmpty()) {
      log.info("No partitions to change for {}", tableName);
      return;
    }
    log.warn("REST Catalog partition management is handled via table metadata updates. "
        + "Updating {} partitions on {} - partition metadata will be managed by the catalog.", changedPartitions.size(), tableName);
  }

  @Override
  public void dropPartitionsToTable(String tableName, List<String> partitionsToDrop) {
    if (partitionsToDrop.isEmpty()) {
      log.info("No partitions to drop for {}", tableName);
      return;
    }
    log.warn("REST Catalog partition management is handled via table metadata updates. "
        + "Dropping {} partitions on {} - partition metadata will be managed by the catalog.", partitionsToDrop.size(), tableName);
  }

  @Override
  public void updateTableComments(String tableName, Map<String, Pair<String, String>> alterSchema) {
    try {
      TableMetadata tableMetadata = restClient.getTable(databaseName, tableName);
      if (tableMetadata == null || tableMetadata.getSchema() == null) {
        throw new HoodieHiveSyncException("Table " + tableName + " not found");
      }

      Schema currentSchema = tableMetadata.getSchema();
      for (Schema.SchemaField field : currentSchema.getFields()) {
        if (alterSchema.containsKey(field.getName())) {
          String comment = alterSchema.get(field.getName()).getRight();
          field.setDoc(comment);
        }
      }

      UpdateTableRequest updateRequest = new UpdateTableRequest();
      UpdateTableRequest.AddSchemaUpdate addSchemaUpdate = new UpdateTableRequest.AddSchemaUpdate();
      addSchemaUpdate.setSchema(currentSchema);
      updateRequest.addUpdate(addSchemaUpdate);

      restClient.updateTable(databaseName, tableName, updateRequest);
      log.info("Updated table comments for: {}.{}", databaseName, tableName);
    } catch (Exception e) {
      log.error("Failed to update table comments for {}", tableName, e);
      throw new HoodieHiveSyncException("Failed to update table comments for " + tableName, e);
    }
  }

  @Override
  public void close() {
    if (restClient != null) {
      try {
        restClient.close();
      } catch (IOException e) {
        log.warn("Failed to close REST catalog client", e);
      }
    }
  }
}
