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

import java.io.IOException;
import java.util.Arrays;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstant.State;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter;
import org.apache.spark.sql.sources.v2.writer.DataWriterFactory;
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;

public class HoodieDataSourceInternalWriter implements DataSourceWriter {

  private final String instantTime;
  private final HoodieTableMetaClient metaClient;
  private final HoodieWriteConfig writeConfig;
  private final StructType structType;

  public HoodieDataSourceInternalWriter(String instantTime, HoodieTableMetaClient metaClient,
      HoodieWriteConfig writeConfig, StructType structType) {
    this.instantTime = instantTime;
    this.metaClient = metaClient;
    this.writeConfig = writeConfig;
    this.structType = structType;
  }

  @Override
  public DataWriterFactory<InternalRow> createWriterFactory() {
    metaClient.getActiveTimeline().createNewInstant(new HoodieInstant(State.REQUESTED, HoodieTimeline.COMMIT_ACTION,
        instantTime));
    metaClient.getActiveTimeline().transitionRequestedToInflight(
        new HoodieInstant(State.REQUESTED, HoodieTimeline.COMMIT_ACTION, instantTime), Option.empty());
    return new HoodieDataInternalWriterFactory(metaClient, writeConfig, instantTime, structType);
  }

  @Override
  public boolean useCommitCoordinator() {
    return true;
  }

  @Override
  public void onDataWriterCommit(WriterCommitMessage message) {
    System.out.println("Received commit of a data writer =" + message);
  }

  @Override
  public void commit(WriterCommitMessage[] messages) {
    HoodieCommitMetadata metadata = new HoodieCommitMetadata();

    Arrays.stream(messages).map(m -> (HoodieWriterCommitMessage) m)
        .flatMap(m -> m.getWriteStatuses().stream().map(m2 -> Pair.of(m2.getPartitionPath(), m2.getStat())))
        .forEach(p -> metadata.addWriteStat(p.getKey(), p.getValue()));
    try {
      /**
      System.out.println("Metadata =" + metadata);
      metaClient.getActiveTimeline()
          .saveAsComplete(new HoodieInstant(State.INFLIGHT, HoodieTimeline.COMMIT_ACTION, instantTime), Option
              .of(metadata.toJsonString().getBytes(StandardCharsets.UTF_8)));
       **/
    } catch (Exception ioe) {
      throw new HoodieException(ioe.getMessage(), ioe);
    }
  }

  @Override
  public void abort(WriterCommitMessage[] messages) {
    Arrays.stream(messages).map(m -> (HoodieWriterCommitMessage) m).forEach(cm -> {
      cm.getWriteStatuses().forEach(w -> {
        Path p = new Path(FSUtils.getPartitionPath(metaClient.getBasePath(), w.getPartitionPath()), w.getFilePath());
        try {
          metaClient.getFs().delete(p);
        } catch (IOException e) {
          throw new HoodieIOException(e.getMessage(), e);
        }
      });
    });
  }
}
