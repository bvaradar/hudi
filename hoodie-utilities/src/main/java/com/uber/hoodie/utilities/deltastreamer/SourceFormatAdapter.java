/*
 *  Copyright (c) 2018 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 *
 */

package com.uber.hoodie.utilities.deltastreamer;

import com.uber.hoodie.AvroConversionUtils;
import com.uber.hoodie.common.util.collection.Pair;
import com.uber.hoodie.utilities.sources.AvroSource;
import com.uber.hoodie.utilities.sources.JsonSource;
import com.uber.hoodie.utilities.sources.RowSource;
import com.uber.hoodie.utilities.sources.Source;
import com.uber.hoodie.utilities.sources.helpers.AvroConvertor;
import java.util.Optional;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

/**
 * Adapts data-format provided by the source to the data-format required by the client (DeltaStreamer)
 */
public final class SourceFormatAdapter {

  private final Source source;

  public SourceFormatAdapter(Source source) {
    this.source = source;
  }

  /**
   * Fetch new data in avro format. If the source provides data in different format, they are translated
   * to Avro format
   * @param lastCkptStr
   * @param sourceLimit
   * @return
   */
  public Pair<Optional<JavaRDD<GenericRecord>>, String> fetchNewDataInAvroFormat(Optional<String> lastCkptStr,
      long sourceLimit) {
    switch (source.getSourceType()) {
      case AVRO:
        return ((AvroSource)source).fetchNewData(lastCkptStr, sourceLimit);
      case JSON: {
        AvroConvertor convertor = new AvroConvertor(source.getSchemaProvider().getSourceSchema());
        Pair<Optional<JavaRDD<String>>, String> r = ((JsonSource)source).fetchNewData(lastCkptStr, sourceLimit);
        return Pair.of(Optional.ofNullable(
            r.getLeft().map(rdd -> rdd.map(convertor::fromJson))
                .orElse(null)), r.getRight());
      }
      case ROW: {
        Pair<Optional<Dataset<Row>>, String> r = ((RowSource)source).fetchNewData(lastCkptStr, sourceLimit);
        return Pair.of(Optional.ofNullable(r.getLeft().map(
            rdd -> (AvroConversionUtils.createRdd(rdd, "hudi_source", "hudi.source").toJavaRDD()))
            .orElse(null)), r.getRight());
      }
      default:
        throw new IllegalArgumentException("Unknown source type (" + source.getSourceType() + ")");
    }
  }

  /**
   * Fetch new data in row format. If the source provides data in different format, they are translated
   * to Row format
   * @param lastCkptStr
   * @param sourceLimit
   * @return
   */
  public Pair<Optional<Dataset<Row>>, String> fetchNewDataInRowFormat(Optional<String> lastCkptStr, long sourceLimit) {
    switch (source.getSourceType()) {
      case ROW:
        return ((RowSource)source).fetchNewData(lastCkptStr, sourceLimit);
      case AVRO: {
        Schema sourceSchema = source.getSchemaProvider().getSourceSchema();
        Pair<Optional<JavaRDD<GenericRecord>>, String> r = ((AvroSource)source).fetchNewData(lastCkptStr, sourceLimit);
        return Pair.of(Optional.ofNullable(
            r.getLeft().map(rdd -> AvroConversionUtils.createDataFrame(JavaRDD.toRDD(rdd),
                sourceSchema.toString(), source.getSparkSession()))
                .orElse(source.getSparkSession().emptyDataFrame())), r.getRight());
      }
      case JSON: {
        Schema sourceSchema = source.getSchemaProvider().getSourceSchema();
        Pair<Optional<JavaRDD<String>>, String> r = ((JsonSource)source).fetchNewData(lastCkptStr, sourceLimit);
        StructType dataType = AvroConversionUtils.convertAvroSchemaToStructType(sourceSchema);
        return Pair.of(Optional.ofNullable(
            r.getLeft().map(rdd -> source.getSparkSession().read().schema(dataType).json(rdd))
                .orElse(null)), r.getRight());
      }
      default:
        throw new IllegalArgumentException("Unknown source type (" + source.getSourceType() + ")");
    }
  }
}
