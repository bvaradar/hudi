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

import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Request to update a table via REST Catalog.
 */
public class UpdateTableRequest implements Serializable {

  private static final long serialVersionUID = 1L;

  @JsonProperty("updates")
  private List<TableUpdate> updates;

  public UpdateTableRequest() {
    this.updates = new ArrayList<>();
  }

  public List<TableUpdate> getUpdates() {
    return updates;
  }

  public void setUpdates(List<TableUpdate> updates) {
    this.updates = updates;
  }

  public void addUpdate(TableUpdate update) {
    this.updates.add(update);
  }

  /**
   * Base class for table updates.
   */
  public static class TableUpdate implements Serializable {

    private static final long serialVersionUID = 1L;

    @JsonProperty("action")
    private String action;

    public TableUpdate() {
    }

    public TableUpdate(String action) {
      this.action = action;
    }

    public String getAction() {
      return action;
    }

    public void setAction(String action) {
      this.action = action;
    }
  }

  /**
   * Update to set table properties.
   */
  public static class SetPropertiesUpdate extends TableUpdate {

    private static final long serialVersionUID = 1L;

    @JsonProperty("updates")
    private java.util.Map<String, String> updates;

    public SetPropertiesUpdate() {
      super("set-properties");
      this.updates = new java.util.HashMap<>();
    }

    public java.util.Map<String, String> getUpdates() {
      return updates;
    }

    public void setUpdates(java.util.Map<String, String> updates) {
      this.updates = updates;
    }
  }

  /**
   * Update to upgrade format version.
   */
  public static class UpgradeFormatVersionUpdate extends TableUpdate {

    private static final long serialVersionUID = 1L;

    @JsonProperty("format-version")
    private Integer formatVersion;

    public UpgradeFormatVersionUpdate() {
      super("upgrade-format-version");
    }

    public Integer getFormatVersion() {
      return formatVersion;
    }

    public void setFormatVersion(Integer formatVersion) {
      this.formatVersion = formatVersion;
    }
  }

  /**
   * Update to add a new schema.
   */
  public static class AddSchemaUpdate extends TableUpdate {

    private static final long serialVersionUID = 1L;

    @JsonProperty("schema")
    private Schema schema;

    public AddSchemaUpdate() {
      super("add-schema");
    }

    public Schema getSchema() {
      return schema;
    }

    public void setSchema(Schema schema) {
      this.schema = schema;
    }
  }

  /**
   * Update to set current schema.
   */
  public static class SetCurrentSchemaUpdate extends TableUpdate {

    private static final long serialVersionUID = 1L;

    @JsonProperty("schema-id")
    private Integer schemaId;

    public SetCurrentSchemaUpdate() {
      super("set-current-schema");
    }

    public Integer getSchemaId() {
      return schemaId;
    }

    public void setSchemaId(Integer schemaId) {
      this.schemaId = schemaId;
    }
  }
}
