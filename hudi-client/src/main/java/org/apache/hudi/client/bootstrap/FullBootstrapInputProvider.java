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

package org.apache.hudi.client.bootstrap;

import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.util.collection.Pair;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.List;

public abstract class FullBootstrapInputProvider {

  protected final TypedProperties props;
  protected final JavaSparkContext jsc;

  public FullBootstrapInputProvider(TypedProperties props, JavaSparkContext jsc) {
    this.props = props;
    this.jsc = jsc;
  }

  /**
   * Generates a list of input partition and files and returns a RDD representing source.
   * @param partitionPaths Partition Paths
   * @return JavaRDD of input records
   */
  public abstract JavaRDD<HoodieRecord> generateInputRecordRDD(String tableName,
      List<Pair<String, List<String>>> partitionPaths);
}
