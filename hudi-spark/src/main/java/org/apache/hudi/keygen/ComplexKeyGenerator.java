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

import org.apache.hudi.DataSourceUtils;
import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.exception.HoodieKeyException;

import org.apache.avro.generic.GenericRecord;

import java.util.Arrays;
import java.util.stream.Collectors;
import org.apache.spark.sql.Row;

/**
 * Complex key generator, which takes names of fields to be used for recordKey and partitionPath as configs.
 */
public class ComplexKeyGenerator extends KeyGenerator {

  protected final boolean hiveStylePartitioning;

  public ComplexKeyGenerator(TypedProperties props) {
    super(props);
    this.setRecordKeyFields(Arrays.stream(props.getString(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY())
        .split(",")).map(String::trim).collect(Collectors.toList()));
    this.setPartitionPathFields(Arrays.stream(props.getString(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY())
        .split(",")).map(String::trim).collect(Collectors.toList()));
    this.hiveStylePartitioning = props.getBoolean(DataSourceWriteOptions.HIVE_STYLE_PARTITIONING_OPT_KEY(),
        Boolean.parseBoolean(DataSourceWriteOptions.DEFAULT_HIVE_STYLE_PARTITIONING_OPT_VAL()));
  }

  @Override
  public HoodieKey getKey(GenericRecord record) {
    if (getRecordKeyFields() == null || getPartitionPathFields() == null) {
      throw new HoodieKeyException("Unable to find field names for record key or partition path in cfg");
    }

    boolean keyIsNullEmpty = true;
    StringBuilder recordKey = new StringBuilder();
    for (String recordKeyField : getRecordKeyFields()) {
      String recordKeyValue = DataSourceUtils.getNestedFieldValAsString(record, recordKeyField, true);
      if (recordKeyValue == null) {
        recordKey.append(recordKeyField + ":" + NULL_RECORDKEY_PLACEHOLDER + ",");
      } else if (recordKeyValue.isEmpty()) {
        recordKey.append(recordKeyField + ":" + EMPTY_RECORDKEY_PLACEHOLDER + ",");
      } else {
        recordKey.append(recordKeyField + ":" + recordKeyValue + ",");
        keyIsNullEmpty = false;
      }
    }
    recordKey.deleteCharAt(recordKey.length() - 1);
    if (keyIsNullEmpty) {
      throw new HoodieKeyException("recordKey values: \"" + recordKey + "\" for fields: "
          + getRecordKeyFields().toString() + " cannot be entirely null or empty.");
    }

    StringBuilder partitionPath = new StringBuilder();
    for (String partitionPathField : getPartitionPathFields()) {
      String fieldVal = DataSourceUtils.getNestedFieldValAsString(record, partitionPathField, true);
      if (fieldVal == null || fieldVal.isEmpty()) {
        partitionPath.append(hiveStylePartitioning ? partitionPathField + "=" + DEFAULT_PARTITION_PATH
                : DEFAULT_PARTITION_PATH);
      } else {
        partitionPath.append(hiveStylePartitioning ? partitionPathField + "=" + fieldVal : fieldVal);
      }
      partitionPath.append(DEFAULT_PARTITION_PATH_SEPARATOR);
    }
    partitionPath.deleteCharAt(partitionPath.length() - 1);

    return new HoodieKey(recordKey.toString(), partitionPath.toString());
  }

  public String getRecordKeyFromRow(Row row) {
    return RowKeyGeneratorHelper.getRecordKeyFromRow(row, getRecordKeyFields(), getRowKeyPositions(), true);
  }

  public String getPartitionPathFromRow(Row row) {
    return RowKeyGeneratorHelper.getPartitionPathFromRow(row, getPartitionPathFields(),
        hiveStylePartitioning, getPartitionPathPositions());
  }
}
