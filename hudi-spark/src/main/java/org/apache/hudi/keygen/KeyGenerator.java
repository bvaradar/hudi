/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.keygen;

import org.apache.hudi.AvroConversionHelper;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.util.ValidationUtils;

import org.apache.avro.generic.GenericRecord;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import scala.Function1;

/**
 * Abstract class to extend for plugging in extraction of {@link HoodieKey} from an Avro record.
 */
public abstract class KeyGenerator implements Serializable {

  protected static final String DEFAULT_PARTITION_PATH = "default";
  protected static final String NULL_RECORDKEY_PLACEHOLDER = "__null__";
  protected static final String EMPTY_RECORDKEY_PLACEHOLDER = "__empty__";
  protected static final String DEFAULT_PARTITION_PATH_SEPARATOR = "/";

  protected static final Logger LOG = LogManager.getLogger(KeyGenerator.class);

  protected transient TypedProperties config;

  private List<String> recordKeyFields;
  private List<String> partitionPathFields;
  private Map<String, List<Integer>> rowKeyPositions = new HashMap<>();
  private Map<String, List<Integer>> partitionPathPositions = new HashMap<>();
  private Function1<Object, Object> converterFn = null;
  protected StructType structType;

  protected KeyGenerator(TypedProperties config) {
    this.config = config;
  }

  /**
   * Generate a Hoodie Key out of provided generic record.
   */
  public abstract HoodieKey getKey(GenericRecord record);

  public void initializeRowKeyGenerator(StructType structType, String structName, String recordNamespace) {
    // parse simple feilds
    getRecordKeyFields().stream()
        .filter(f -> !(f.contains(".")))
        .forEach(f -> rowKeyPositions.put(f, Collections.singletonList((Integer) (structType.getFieldIndex(f).get()))));
    // parse nested fields
    getRecordKeyFields().stream()
        .filter(f -> f.contains("."))
        .forEach(f -> rowKeyPositions.put(f, RowKeyGeneratorHelper.getNestedFieldIndices(structType, f, true)));
    // parse simple fields
    if(getPartitionPathFields() != null) {
      getPartitionPathFields().stream()
          .filter(f -> !f.isEmpty())
          .filter(f -> !(f.contains(".")))
          .forEach(f -> partitionPathPositions.put(f, Collections.singletonList((Integer) (structType.getFieldIndex(f).get()))));
      // parse nested fields
      getPartitionPathFields().stream()
          .filter(f -> !f.isEmpty())
          .filter(f -> f.contains("."))
          .forEach(f -> partitionPathPositions.put(f, RowKeyGeneratorHelper.getNestedFieldIndices(structType, f, false)));
    }
    this.structType = structType;
    converterFn = AvroConversionHelper.createConverterToAvro(structType, structName, recordNamespace);
  }

  public String getRecordKeyFromRow(Row row) {
    ValidationUtils.checkArgument((converterFn != null), "initializeRowKeyGenerator hasn't been invoked");
    GenericRecord genericRecord = (GenericRecord) converterFn.apply(row);
    return getKey(genericRecord).getRecordKey();
  }

  public String getPartitionPathFromRow(Row row) {
    ValidationUtils.checkArgument((converterFn != null), "initializeRowKeyGenerator hasn't been invoked");
    GenericRecord genericRecord = (GenericRecord) converterFn.apply(row);
    return getKey(genericRecord).getPartitionPath();
  }

  protected List<String> getRecordKeyFields() {
    return recordKeyFields;
  }

  protected List<String> getPartitionPathFields() {
    return partitionPathFields;
  }

  protected void setRecordKeyFields(List<String> recordKeyFields) {
    this.recordKeyFields = recordKeyFields;
  }

  protected void setPartitionPathFields(List<String> partitionPathFields) {
    this.partitionPathFields = partitionPathFields;
  }

  protected Map<String, List<Integer>> getRowKeyPositions() {
    return rowKeyPositions;
  }

  protected Map<String, List<Integer>> getPartitionPathPositions() {
    return partitionPathPositions;
  }
}
