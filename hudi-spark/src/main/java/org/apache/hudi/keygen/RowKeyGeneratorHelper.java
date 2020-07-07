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
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import scala.Option;

import static org.apache.hudi.keygen.KeyGenerator.DEFAULT_PARTITION_PATH;
import static org.apache.hudi.keygen.KeyGenerator.DEFAULT_PARTITION_PATH_SEPARATOR;
import static org.apache.hudi.keygen.KeyGenerator.EMPTY_RECORDKEY_PLACEHOLDER;
import static org.apache.hudi.keygen.KeyGenerator.NULL_RECORDKEY_PLACEHOLDER;

/**
 * Helper class to fetch fields from Row.
 */
public class RowKeyGeneratorHelper {

  /**
   * Generates record key for the corresponding {@link Row}.
   * @param row instance of {@link Row} of interest
   * @param recordKeyFields record key fields as a list
   * @param recordKeyPositions record key positions for the corresponding record keys in {@code recordKeyFields}
   * @param prefixFieldName {@code true} if field name need to be prefixed in the returned result. {@code false} otherwise.
   * @return the record key thus generated
   */
  public static String getRecordKeyFromRow(Row row, List<String> recordKeyFields, Map<String, List<Integer>> recordKeyPositions, boolean prefixFieldName) {
    AtomicBoolean keyIsNullOrEmpty = new AtomicBoolean(true);
    String toReturn = IntStream.range(0, recordKeyFields.size()).mapToObj(idx -> {
      String field = recordKeyFields.get(idx);
      String val = null;
      List<Integer> fieldPositions = recordKeyPositions.get(field);
      if (fieldPositions.size() == 1) { // simple field
        Integer fieldPos = fieldPositions.get(0);
        if (row.isNullAt(fieldPos)) {
          val = NULL_RECORDKEY_PLACEHOLDER;
        } else {
          val = row.getAs(field).toString();
          if (val.isEmpty()) {
            val = EMPTY_RECORDKEY_PLACEHOLDER;
          } else {
            keyIsNullOrEmpty.set(false);
          }
        }
      } else { // nested fields
        val = getNestedFieldVal(row, recordKeyPositions.get(field)).toString();
        if (!val.contains(NULL_RECORDKEY_PLACEHOLDER) && !val.contains(EMPTY_RECORDKEY_PLACEHOLDER)) {
          keyIsNullOrEmpty.set(false);
        }
      }
      return prefixFieldName ? (field + ":" + val) : val;
    }).collect(Collectors.joining(","));
    if (keyIsNullOrEmpty.get()) {
      throw new HoodieKeyException("recordKey value: \"" + toReturn + "\" for fields: \"" + Arrays.toString(recordKeyFields.toArray()) + "\" cannot be null or empty.");
    }
    return toReturn;
  }

  /**
   * Generates partition path for the corresponding {@link Row}.
   * @param row instance of {@link Row} of interest
   * @param partitionPathFields partition path fields as a list
   * @param hiveStylePartitioning {@code true} if hive style partitioning is set. {@code false} otherwise
   * @param partitionPathPositions partition path positions for the corresponding fields in {@code partitionPathFields}
   * @return the generated partition path for the row
   */
  public static String getPartitionPathFromRow(Row row, List<String> partitionPathFields, boolean hiveStylePartitioning, Map<String, List<Integer>> partitionPathPositions) {
    return IntStream.range(0, partitionPathFields.size()).mapToObj(idx -> {
      String field = partitionPathFields.get(idx);
      String val = null;
      List<Integer> fieldPositions = partitionPathPositions.get(field);
      if (fieldPositions.size() == 1) { // simple
        Integer fieldPos = fieldPositions.get(0);
        // for partition path, if field is not found, index will be set to -1
        if (fieldPos == -1 || row.isNullAt(fieldPos)) {
          val = DEFAULT_PARTITION_PATH;
        } else {
          val = row.getAs(field).toString();
          if (val.isEmpty()) {
            val = DEFAULT_PARTITION_PATH;
          }
        }
        if (hiveStylePartitioning) {
          val = field + "=" + val;
        }
      } else { // nested
        Object nestedVal = getNestedFieldVal(row, partitionPathPositions.get(field));
        if (nestedVal.toString().contains(NULL_RECORDKEY_PLACEHOLDER) || nestedVal.toString().contains(EMPTY_RECORDKEY_PLACEHOLDER)) {
          val = hiveStylePartitioning ? field + "=" + DEFAULT_PARTITION_PATH : DEFAULT_PARTITION_PATH;
        } else {
          val = hiveStylePartitioning ? field + "=" + nestedVal.toString() : nestedVal.toString();
        }
      }
      return val;
    }).collect(Collectors.joining(DEFAULT_PARTITION_PATH_SEPARATOR));
  }

  /**
   * Fetch the field value located at the positions requested for.
   * @param row instance of {@link Row} of interest
   * @param positions tree style positions where the leaf node need to be fetched and returned
   * @return the field value as per the positions requested for.
   */
  public static Object getNestedFieldVal(Row row, List<Integer> positions) {
    if (positions.size() == 1 && positions.get(0) == -1) {
      return DEFAULT_PARTITION_PATH;
    }
    int index = 0;
    int totalCount = positions.size();
    Row valueToProcess = row;
    Object toReturn = null;

    while (index < totalCount) {
      if (index < totalCount - 1) {
        if (valueToProcess.isNullAt(positions.get(index))) {
          toReturn = NULL_RECORDKEY_PLACEHOLDER;
          break;
        }
        valueToProcess = (Row) valueToProcess.get(positions.get(index));
      } else { // last index
        if (valueToProcess.getAs(positions.get(index)).toString().isEmpty()) {
          toReturn = EMPTY_RECORDKEY_PLACEHOLDER;
          break;
        }
        toReturn = valueToProcess.getAs(positions.get(index));
      }
      index++;
    }
    return toReturn;
  }

  /**
   * Generate the tree style positions for the field requested for as per the defined struct type.
   * @param structType schema of interest
   * @param field field of interest for which the positions are requested for
   * @param isRecordKey {@code true} if the field requested for is a record key. {@code false} incase of a partition path.
   * @return the positions of the field as per the struct type.
   */
  public static List<Integer> getNestedFieldIndices(StructType structType, String field, boolean isRecordKey) {
    String[] slices = field.split("\\.");
    List<Integer> positions = new ArrayList<>();
    int index = 0;
    int totalCount = slices.length;
    while (index < totalCount) {
      String slice = slices[index];
      Option<Object> curIndexOpt = structType.getFieldIndex(slice);
      if (curIndexOpt.isDefined()) {
        int curIndex = (int) curIndexOpt.get();
        positions.add(curIndex);
        final StructField nestedField = structType.fields()[curIndex];
        if (index < totalCount - 1) {
          if (!(nestedField.dataType() instanceof StructType)) {
            if (isRecordKey) {
              throw new HoodieKeyException("Nested field should be of type StructType " + nestedField);
            } else {
              positions = Collections.singletonList(-1);
              break;
            }
          }
          structType = (StructType) nestedField.dataType();
        }
      } else {
        if (isRecordKey) {
          throw new HoodieKeyException("Can't find " + slice + " in StructType for the field " + field);
        } else {
          positions = Collections.singletonList(-1);
          break;
        }
      }
      index++;
    }
    return positions;
  }
}
