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

package com.uber.hoodie.utilities.sources;

import com.uber.hoodie.common.util.TypedProperties;
import com.uber.hoodie.utilities.schema.SchemaProvider;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public abstract class RowSource extends Source<Dataset<Row>> {

  public RowSource(TypedProperties props,
      JavaSparkContext sparkContext,
      SchemaProvider schemaProvider) {
    super(props, sparkContext, schemaProvider, SourceType.ROW);
  }
}
