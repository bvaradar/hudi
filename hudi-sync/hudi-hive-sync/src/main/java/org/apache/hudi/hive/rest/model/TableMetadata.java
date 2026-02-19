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

package org.apache.hudi.hive.rest.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Represents table metadata in REST Catalog format.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class TableMetadata implements Serializable {

  private static final long serialVersionUID = 1L;

  @JsonProperty("format-version")
  private Integer formatVersion;

  @JsonProperty("table-uuid")
  private String tableUuid;

  @JsonProperty("location")
  private String location;

  @JsonProperty("last-updated-ms")
  private Long lastUpdatedMs;

  @JsonProperty("last-column-id")
  private Integer lastColumnId;

  @JsonProperty("schema")
  private Schema schema;

  @JsonProperty("current-schema-id")
  private Integer currentSchemaId;

  @JsonProperty("partition-spec")
  private PartitionSpec partitionSpec;

  @JsonProperty("default-spec-id")
  private Integer defaultSpecId;

  @JsonProperty("last-partition-id")
  private Integer lastPartitionId;

  @JsonProperty("properties")
  private Map<String, String> properties;

  public TableMetadata() {
    this.properties = new HashMap<>();
  }

  public Integer getFormatVersion() {
    return formatVersion;
  }

  public void setFormatVersion(Integer formatVersion) {
    this.formatVersion = formatVersion;
  }

  public String getTableUuid() {
    return tableUuid;
  }

  public void setTableUuid(String tableUuid) {
    this.tableUuid = tableUuid;
  }

  public String getLocation() {
    return location;
  }

  public void setLocation(String location) {
    this.location = location;
  }

  public Long getLastUpdatedMs() {
    return lastUpdatedMs;
  }

  public void setLastUpdatedMs(Long lastUpdatedMs) {
    this.lastUpdatedMs = lastUpdatedMs;
  }

  public Integer getLastColumnId() {
    return lastColumnId;
  }

  public void setLastColumnId(Integer lastColumnId) {
    this.lastColumnId = lastColumnId;
  }

  public Schema getSchema() {
    return schema;
  }

  public void setSchema(Schema schema) {
    this.schema = schema;
  }

  public Integer getCurrentSchemaId() {
    return currentSchemaId;
  }

  public void setCurrentSchemaId(Integer currentSchemaId) {
    this.currentSchemaId = currentSchemaId;
  }

  public PartitionSpec getPartitionSpec() {
    return partitionSpec;
  }

  public void setPartitionSpec(PartitionSpec partitionSpec) {
    this.partitionSpec = partitionSpec;
  }

  public Integer getDefaultSpecId() {
    return defaultSpecId;
  }

  public void setDefaultSpecId(Integer defaultSpecId) {
    this.defaultSpecId = defaultSpecId;
  }

  public Integer getLastPartitionId() {
    return lastPartitionId;
  }

  public void setLastPartitionId(Integer lastPartitionId) {
    this.lastPartitionId = lastPartitionId;
  }

  public Map<String, String> getProperties() {
    return properties;
  }

  public void setProperties(Map<String, String> properties) {
    this.properties = properties;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    TableMetadata that = (TableMetadata) o;
    return Objects.equals(formatVersion, that.formatVersion)
        && Objects.equals(tableUuid, that.tableUuid)
        && Objects.equals(location, that.location)
        && Objects.equals(schema, that.schema)
        && Objects.equals(currentSchemaId, that.currentSchemaId)
        && Objects.equals(partitionSpec, that.partitionSpec)
        && Objects.equals(properties, that.properties);
  }

  @Override
  public int hashCode() {
    return Objects.hash(formatVersion, tableUuid, location, schema, currentSchemaId, partitionSpec, properties);
  }
}
