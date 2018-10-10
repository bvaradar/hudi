/*
 *  Copyright (c) 2017 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
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

package com.uber.hoodie.utilities.sources;

import com.uber.hoodie.common.util.TypedProperties;
import com.uber.hoodie.common.util.collection.Pair;
import com.uber.hoodie.utilities.schema.SchemaProvider;
import java.io.Serializable;
import java.util.Optional;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;

/**
 * Represents a source from which we can tail data. Assumes a constructor that takes properties.
 */
public abstract class Source<T> implements Serializable {

  public enum SourceType {
    JSON,
    AVRO,
    ROW
  }

  protected transient TypedProperties props;
  protected transient JavaSparkContext sparkContext;
  protected transient SparkSession sparkSession;
  protected transient SchemaProvider schemaProvider;

  private final SourceType sourceType;

  protected Source(TypedProperties props, JavaSparkContext sparkContext, SchemaProvider schemaProvider) {
    this(props, sparkContext, schemaProvider, SourceType.AVRO);
  }

  protected Source(TypedProperties props, JavaSparkContext sparkContext,
      SchemaProvider schemaProvider, SourceType sourceType) {
    this.props = props;
    this.sparkContext = sparkContext;
    this.sparkSession = SparkSession.builder().config(sparkContext.getConf()).getOrCreate();
    this.schemaProvider = schemaProvider;
    this.sourceType = sourceType;
  }

  public abstract Pair<Optional<T>, String> fetchNewData(Optional<String> lastCkptStr, long sourceLimit);

  public SourceType getSourceType() {
    return sourceType;
  }

  public SchemaProvider getSchemaProvider() {
    return schemaProvider;
  }

  public SparkSession getSparkSession() {
    return sparkSession;
  }
}
