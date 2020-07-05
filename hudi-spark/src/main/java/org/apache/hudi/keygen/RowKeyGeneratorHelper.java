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

import org.apache.hudi.exception.HoodieKeyException;

import org.apache.spark.sql.Row;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class RowKeyGeneratorHelper {

  private static final String DEFAULT_PARTITION_PATH = "default";
  private static final String DEFAULT_PARTITION_PATH_SEPARATOR = "/";

  protected static final String NULL_RECORDKEY_PLACEHOLDER = "__null__";
  protected static final String EMPTY_RECORDKEY_PLACEHOLDER = "__empty__";

  public static String getRecordKeyFromRow(Row row, List<String> recordKeyFields, List<Integer> recordKeyFieldsPos) {
    AtomicBoolean keyIsNullOrEmpty = new AtomicBoolean(true);
    String toReturn = IntStream.range(0, recordKeyFields.size()).mapToObj(idx -> {
      String field = recordKeyFields.get(idx);
      Integer fieldPos = recordKeyFieldsPos.get(idx);
      if (row.isNullAt(fieldPos)) {
        return fieldPos + ":" + NULL_RECORDKEY_PLACEHOLDER;
      }
      String val = row.getAs(field).toString();
      if (val.isEmpty()) {
        return fieldPos + ":" + EMPTY_RECORDKEY_PLACEHOLDER;
      }
      keyIsNullOrEmpty.set(false);
      return fieldPos + ":" + val;
    }).collect(Collectors.joining(","));
    if (keyIsNullOrEmpty.get()) {
      throw new HoodieKeyException("recordKey value: \"" + toReturn + "\" for fields: \"" + Arrays.toString(recordKeyFields.toArray()) + "\" cannot be null or empty.");
    }
    return toReturn;
  }

  public static String getPartitionPathFromRow(Row row, List<String> partitionPathFields,
      List<Integer> partitionPathFieldsPos, boolean hiveStylePartitioning) {
    return IntStream.range(0, partitionPathFields.size()).mapToObj(idx -> {
      String field = partitionPathFields.get(idx);
      Integer fieldPos = partitionPathFieldsPos.get(idx);
      if (row.isNullAt(fieldPos)) {
        return hiveStylePartitioning ? field + "=" + DEFAULT_PARTITION_PATH : DEFAULT_PARTITION_PATH;
      }

      String fieldVal = row.getAs(field).toString();
      if (fieldVal.isEmpty()) {
        return hiveStylePartitioning ? field + "=" + DEFAULT_PARTITION_PATH : DEFAULT_PARTITION_PATH;
      }

      return hiveStylePartitioning ? field + "=" + fieldVal : fieldVal;
    }).collect(Collectors.joining(DEFAULT_PARTITION_PATH_SEPARATOR));
  }
}
