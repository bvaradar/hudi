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

package org.apache.hudi.hive.rest;

import org.apache.hudi.common.schema.HoodieSchema;
import org.apache.hudi.hive.HiveSyncConfig;
import org.apache.hudi.hive.rest.model.PartitionSpec;
import org.apache.hudi.hive.rest.model.Schema;
import org.apache.hudi.hive.util.HiveSchemaUtil;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Utility for converting Hudi schemas to REST Catalog format.
 */
public class RestSchemaUtil {

  public static Schema convertToRestSchema(HoodieSchema hoodieSchema, HiveSyncConfig syncConfig) {
    LinkedHashMap<String, String> mapSchema = HiveSchemaUtil.hoodieSchemaToMapSchema(hoodieSchema,
        syncConfig.getBoolean(org.apache.hudi.hive.HiveSyncConfigHolder.HIVE_SUPPORT_TIMESTAMP_TYPE), false);

    List<Schema.SchemaField> fields = new ArrayList<>();
    int fieldId = 0;
    for (Map.Entry<String, String> entry : mapSchema.entrySet()) {
      String restType = hiveTypeToRestType(entry.getValue());
      Schema.SchemaField field = new Schema.SchemaField(fieldId++, entry.getKey(), false, restType);
      fields.add(field);
    }

    return new Schema(0, fields);
  }

  public static PartitionSpec convertToPartitionSpec(List<String> partitionFields, Schema schema) {
    List<PartitionSpec.PartitionField> partFields = new ArrayList<>();
    int partFieldId = 1000;

    for (String partitionField : partitionFields) {
      Integer sourceId = findFieldIdByName(schema, partitionField);
      if (sourceId != null) {
        PartitionSpec.PartitionField pf = new PartitionSpec.PartitionField(sourceId, partFieldId++,
            partitionField, "identity");
        partFields.add(pf);
      }
    }

    return new PartitionSpec(0, partFields);
  }

  private static Integer findFieldIdByName(Schema schema, String fieldName) {
    for (Schema.SchemaField field : schema.getFields()) {
      if (field.getName().equals(fieldName)) {
        return field.getId();
      }
    }
    return null;
  }

  private static String hiveTypeToRestType(String hiveType) {
    String lowerType = hiveType.toLowerCase();
    if (lowerType.equals("string")) {
      return "string";
    } else if (lowerType.equals("int") || lowerType.equals("integer")) {
      return "int";
    } else if (lowerType.equals("bigint")) {
      return "long";
    } else if (lowerType.equals("boolean")) {
      return "boolean";
    } else if (lowerType.equals("float")) {
      return "float";
    } else if (lowerType.equals("double")) {
      return "double";
    } else if (lowerType.equals("timestamp")) {
      return "timestamp";
    } else if (lowerType.equals("date")) {
      return "date";
    } else if (lowerType.startsWith("decimal")) {
      return lowerType;
    } else if (lowerType.equals("binary")) {
      return "binary";
    } else {
      return "string";
    }
  }
}
