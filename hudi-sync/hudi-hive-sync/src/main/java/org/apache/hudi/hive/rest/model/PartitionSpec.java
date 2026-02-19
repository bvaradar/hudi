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
 * Represents partition specification in REST Catalog format.
 */
public class PartitionSpec implements Serializable {

  private static final long serialVersionUID = 1L;

  @JsonProperty("spec-id")
  private Integer specId;

  @JsonProperty("fields")
  private List<PartitionField> fields;

  public PartitionSpec() {
    this.fields = new ArrayList<>();
  }

  public PartitionSpec(Integer specId, List<PartitionField> fields) {
    this.specId = specId;
    this.fields = fields != null ? fields : new ArrayList<>();
  }

  public Integer getSpecId() {
    return specId;
  }

  public void setSpecId(Integer specId) {
    this.specId = specId;
  }

  public List<PartitionField> getFields() {
    return fields;
  }

  public void setFields(List<PartitionField> fields) {
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
    PartitionSpec that = (PartitionSpec) o;
    return Objects.equals(specId, that.specId)
        && Objects.equals(fields, that.fields);
  }

  @Override
  public int hashCode() {
    return Objects.hash(specId, fields);
  }

  /**
   * Represents a partition field.
   */
  public static class PartitionField implements Serializable {

    private static final long serialVersionUID = 1L;

    @JsonProperty("source-id")
    private Integer sourceId;

    @JsonProperty("field-id")
    private Integer fieldId;

    @JsonProperty("name")
    private String name;

    @JsonProperty("transform")
    private String transform;

    public PartitionField() {
    }

    public PartitionField(Integer sourceId, Integer fieldId, String name, String transform) {
      this.sourceId = sourceId;
      this.fieldId = fieldId;
      this.name = name;
      this.transform = transform;
    }

    public Integer getSourceId() {
      return sourceId;
    }

    public void setSourceId(Integer sourceId) {
      this.sourceId = sourceId;
    }

    public Integer getFieldId() {
      return fieldId;
    }

    public void setFieldId(Integer fieldId) {
      this.fieldId = fieldId;
    }

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public String getTransform() {
      return transform;
    }

    public void setTransform(String transform) {
      this.transform = transform;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }
      PartitionField that = (PartitionField) o;
      return Objects.equals(sourceId, that.sourceId)
          && Objects.equals(fieldId, that.fieldId)
          && Objects.equals(name, that.name)
          && Objects.equals(transform, that.transform);
    }

    @Override
    public int hashCode() {
      return Objects.hash(sourceId, fieldId, name, transform);
    }
  }
}
