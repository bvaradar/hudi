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
import java.util.Objects;

/**
 * Represents a table schema in REST Catalog format.
 * This follows the Iceberg REST Catalog specification format.
 */
public class Schema implements Serializable {

  private static final long serialVersionUID = 1L;

  @JsonProperty("type")
  private String type = "struct";

  @JsonProperty("schema-id")
  private Integer schemaId;

  @JsonProperty("fields")
  private List<SchemaField> fields;

  public Schema() {
    this.fields = new ArrayList<>();
  }

  public Schema(Integer schemaId, List<SchemaField> fields) {
    this.schemaId = schemaId;
    this.fields = fields != null ? fields : new ArrayList<>();
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public Integer getSchemaId() {
    return schemaId;
  }

  public void setSchemaId(Integer schemaId) {
    this.schemaId = schemaId;
  }

  public List<SchemaField> getFields() {
    return fields;
  }

  public void setFields(List<SchemaField> fields) {
    this.fields = fields;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    Schema schema = (Schema) o;
    return Objects.equals(type, schema.type)
        && Objects.equals(schemaId, schema.schemaId)
        && Objects.equals(fields, schema.fields);
  }

  @Override
  public int hashCode() {
    return Objects.hash(type, schemaId, fields);
  }

  /**
   * Represents a field in a table schema.
   */
  public static class SchemaField implements Serializable {

    private static final long serialVersionUID = 1L;

    @JsonProperty("id")
    private Integer id;

    @JsonProperty("name")
    private String name;

    @JsonProperty("required")
    private boolean required;

    @JsonProperty("type")
    private String type;

    @JsonProperty("doc")
    private String doc;

    public SchemaField() {
    }

    public SchemaField(Integer id, String name, boolean required, String type) {
      this.id = id;
      this.name = name;
      this.required = required;
      this.type = type;
    }

    public SchemaField(Integer id, String name, boolean required, String type, String doc) {
      this.id = id;
      this.name = name;
      this.required = required;
      this.type = type;
      this.doc = doc;
    }

    public Integer getId() {
      return id;
    }

    public void setId(Integer id) {
      this.id = id;
    }

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public boolean isRequired() {
      return required;
    }

    public void setRequired(boolean required) {
      this.required = required;
    }

    public String getType() {
      return type;
    }

    public void setType(String type) {
      this.type = type;
    }

    public String getDoc() {
      return doc;
    }

    public void setDoc(String doc) {
      this.doc = doc;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      SchemaField that = (SchemaField) o;
      return required == that.required
          && Objects.equals(id, that.id)
          && Objects.equals(name, that.name)
          && Objects.equals(type, that.type)
          && Objects.equals(doc, that.doc);
    }

    @Override
    public int hashCode() {
      return Objects.hash(id, name, required, type, doc);
    }
  }
}
