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

package org.apache.hudi.v2;

import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import org.apache.hadoop.conf.Configuration;
import org.apache.hudi.DataSourceUtils;
import org.apache.hudi.HoodieSparkSqlWriter;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.sources.DataSourceRegister;
import org.apache.spark.sql.sources.v2.DataSourceOptions;
import org.apache.spark.sql.sources.v2.DataSourceV2;
import org.apache.spark.sql.sources.v2.ReadSupport;
import org.apache.spark.sql.sources.v2.WriteSupport;
import org.apache.spark.sql.sources.v2.reader.DataSourceReader;
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter;
import org.apache.spark.sql.types.StructType;

public class DefaultSource implements DataSourceV2, ReadSupport, WriteSupport,
    DataSourceRegister {

  private SparkSession sparkSession = null;
  private Configuration configuration = null;

  @Override
  public String shortName() {
    return "hudiv2";
  }

  @Override
  public DataSourceReader createReader(StructType schema, DataSourceOptions options) {
    return null;
  }

  @Override
  public DataSourceReader createReader(DataSourceOptions options) {
    return null;
  }

  @Override
  public Optional<DataSourceWriter> createWriter(String writeUUID, StructType schema, SaveMode mode,
      DataSourceOptions options) {
    String instantTime = HoodieActiveTimeline.createNewInstantTime();
    Map<String, String> paramsWithDefaults = HoodieSparkSqlWriter.javaParametersWithWriteDefaults(options.asMap());
    Properties props = new Properties();
    props.putAll(paramsWithDefaults);
    String path = options.get("path").get();
    String tblName = options.get(HoodieWriteConfig.TABLE_NAME).get();
    HoodieWriteConfig config = DataSourceUtils.createHoodieConfig(null, path, tblName, options.asMap());
    HoodieTableMetaClient metaClient = new HoodieTableMetaClient(getConfiguration(), config.getBasePath());
    return Optional.of(new HoodieDataSourceV2Writer(instantTime, metaClient, config, schema));
  }

  private SparkSession getSparkSession() {
    if (sparkSession == null) {
      sparkSession = SparkSession.builder().getOrCreate();
    }
    return sparkSession;
  }

  private Configuration getConfiguration() {
    if (configuration == null) {
      this.configuration = getSparkSession().sparkContext().hadoopConfiguration();
    }
    return configuration;
  }
}
