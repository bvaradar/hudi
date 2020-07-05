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

package org.apache.hudi.internal;

import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.writer.DataWriter;
import org.apache.spark.sql.sources.v2.writer.DataWriterFactory;
import org.apache.spark.sql.types.StructType;

public class HoodieDataInternalWriterFactory implements DataWriterFactory<InternalRow> {

  private final String instantTime;
  private final HoodieTableMetaClient metaClient;
  private final HoodieWriteConfig writeConfig;
  private final StructType structType;

  public HoodieDataInternalWriterFactory(HoodieTableMetaClient metaClient, HoodieWriteConfig writeConfig,
      String instantTime, StructType structType) {
    this.metaClient = metaClient;
    this.writeConfig = writeConfig;
    this.instantTime = instantTime;
    this.structType = structType;
  }

  @Override
  public DataWriter<InternalRow> createDataWriter(int partitionId, long taskId, long epochId) {
    return new HoodieDataInternalWriter(metaClient, writeConfig, instantTime, partitionId, taskId, epochId, structType);
  }
}
