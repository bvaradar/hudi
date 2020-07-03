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

package org.apache.hudi;

import static org.apache.spark.sql.functions.callUDF;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.keygen.KeyGenerator;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import scala.collection.JavaConverters;

public class HoodieDatasetBulkInsertHelper {

  private static final Logger LOG = LogManager
      .getLogger(org.apache.hudi.table.action.commit.dataset.BulkInsertDatasetHelper.class);

  public static Dataset<Row> sortAndCreateHoodieDataset(SQLContext sqlContext, HoodieWriteConfig config,
      Dataset<Row> rowDataset, String instantTime) {

    // De-dupe/merge if needed
    Dataset<Row> dedupedRecords = rowDataset;

    final Dataset<Row> rows;
    List<Column> sortFields = Stream.concat(
        config.getPartitionPathFields().stream().filter(p -> !p.isEmpty()).map(Column::new),
        config.getRecordKeyFields().stream().map(Column::new)).collect(Collectors.toList());
    rows = dedupedRecords.sort(JavaConverters.collectionAsScalaIterableConverter(sortFields).asScala().toSeq())
        .coalesce(config.getBulkInsertShuffleParallelism());

    TypedProperties properties = new TypedProperties();
    properties.putAll(config.getProps());
    KeyGenerator keyGenerator = (KeyGenerator) ReflectionUtils.loadClass(config.getKeyGeneratorClass(), properties);
    ValidationUtils.checkArgument(keyGenerator.isRowKeyExtractionSupported(),
        "Key Generator (" + keyGenerator.getClass() + ") do not support APIs for extracting record key "
            + "and partition path from Row");
    List<Column> originalFields =
        Arrays.stream(rows.schema().fields()).map(f -> new Column(f.name())).collect(Collectors.toList());
    StructType structTypeForUDF = rows.schema();
    keyGenerator.initializeRowKeyGenerator(structTypeForUDF);

    /**
    sqlContext.udf().register("hudi_recordkey_gen_function", new UDF1<Row, String>() {
      @Override
      public String call(Row row) throws Exception {
        return keyGenerator.getRecordKeyFromRow(row);
      }
    }, DataTypes.StringType);

    sqlContext.udf().register("hudi_partition_gen_function", new UDF1<Row, String>() {
      @Override
      public String call(Row row) throws Exception {
        return keyGenerator.getPartitionPathFromRow(row);
      }
    }, DataTypes.StringType);

    final Dataset<Row> rowDatasetWithRecordKeys = rows.withColumn(HoodieRecord.RECORD_KEY_METADATA_FIELD,
        callUDF("hudi_recordkey_gen_function", org.apache.spark.sql.functions.struct(
            JavaConverters.collectionAsScalaIterableConverter(originalFields).asScala().toSeq())));

    final Dataset<Row> rowDatasetWithRecordKeysAndPartitionPath =
        rowDatasetWithRecordKeys.withColumn(HoodieRecord.PARTITION_PATH_METADATA_FIELD,
            callUDF("hudi_partition_gen_function",
                org.apache.spark.sql.functions.struct(
                    JavaConverters.collectionAsScalaIterableConverter(originalFields).asScala().toSeq())));


    Dataset<Row> rowDatasetWithHoodieColumns =
        rows.withColumn(HoodieRecord.COMMIT_TIME_METADATA_FIELD,
                functions.lit(instantTime).cast(DataTypes.StringType))
            .withColumn(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD,
                functions.lit("").cast(DataTypes.StringType))
            .withColumn(HoodieRecord.RECORD_KEY_METADATA_FIELD,
                functions.col("uuid").cast(DataTypes.StringType))
            .withColumn(HoodieRecord.PARTITION_PATH_METADATA_FIELD,
                functions.col("partition_path").cast(DataTypes.StringType))
            .withColumn(HoodieRecord.FILENAME_METADATA_FIELD,
                functions.lit("").cast(DataTypes.StringType));

    List<Column> orderedFields = Stream.concat(HoodieRecord.HOODIE_META_COLUMNS.stream().map(Column::new),
        originalFields.stream()).collect(Collectors.toList());
    return rowDatasetWithHoodieColumns.select(
        JavaConverters.collectionAsScalaIterableConverter(orderedFields).asScala().toSeq());
     **/
    return rows;
  }
}
