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
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Represents a namespace (database) in REST Catalog.
 */
public class Namespace implements Serializable {

  private static final long serialVersionUID = 1L;

  @JsonProperty("namespace")
  private String namespace;

  @JsonProperty("properties")
  private Map<String, String> properties;

  public Namespace() {
    this.properties = new HashMap<>();
  }

  public Namespace(String namespace) {
    this.namespace = namespace;
    this.properties = new HashMap<>();
  }

  public Namespace(String namespace, Map<String, String> properties) {
    this.namespace = namespace;
    this.properties = properties != null ? properties : new HashMap<>();
  }

  public String getNamespace() {
    return namespace;
  }

  public void setNamespace(String namespace) {
    this.namespace = namespace;
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
    Namespace that = (Namespace) o;
    return Objects.equals(namespace, that.namespace)
        && Objects.equals(properties, that.properties);
  }

  @Override
  public int hashCode() {
    return Objects.hash(namespace, properties);
  }
}
