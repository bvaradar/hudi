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

package org.apache.hudi.table.action.bootstrap;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.avro.model.HoodieFileStatus;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.client.bootstrap.BootstrapKeyGenerator;
import org.apache.hudi.client.bootstrap.BootstrapMode;
import org.apache.hudi.client.bootstrap.BootstrapRecordPayload;
import org.apache.hudi.client.bootstrap.BootstrapWriteStatus;
import org.apache.hudi.client.bootstrap.FullBootstrapInputProvider;
import org.apache.hudi.client.bootstrap.selector.BootstrapSelector;
import org.apache.hudi.client.utils.ParquetReaderIterator;
import org.apache.hudi.common.bootstrap.FileStatusUtils;
import org.apache.hudi.common.bootstrap.index.BootstrapIndex;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.BootstrapSourceFileMapping;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.model.WriteOperationType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieInstant.State;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ReflectionUtils;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.common.util.queue.BoundedInMemoryExecutor;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.execution.SparkBoundedInMemoryExecutor;
import org.apache.hudi.io.HoodieBootstrapHandle;
import org.apache.hudi.table.HoodieTable;
import org.apache.hudi.table.WorkloadProfile;
import org.apache.hudi.table.action.commit.BaseCommitActionExecutor;
import org.apache.hudi.table.action.commit.BulkInsertCommitActionExecutor;
import org.apache.hudi.table.action.commit.HoodieWriteMetadata;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroReadSupport;
import org.apache.parquet.avro.AvroSchemaConverter;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.apache.spark.Partitioner;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class BootstrapActionExecutor<T extends HoodieRecordPayload<T>>
    extends BaseCommitActionExecutor<T, HoodieBootstrapWriteMetadata> {

  private static final Logger LOG = LogManager.getLogger(BootstrapActionExecutor.class);

  public BootstrapActionExecutor(JavaSparkContext jsc, HoodieWriteConfig config,
      HoodieTable<?> table) {
    super(jsc, new HoodieWriteConfig.Builder().withProps(config.getProps())
        .withAutoCommit(true).build(), table, HoodieTimeline.METADATA_BOOTSTRAP_INSTANT_TS, WriteOperationType.BOOTSTRAP);
  }

  private void checkArguments() {
    ValidationUtils.checkArgument(config.getBootstrapSourceBasePath() != null,
        "Ensure Bootstrap Source Path is set");
    ValidationUtils.checkArgument(config.getBootstrapPartitionSelectorClass() != null,
        "Ensure Bootstrap Partition Selector is set");
  }

  @Override
  public HoodieBootstrapWriteMetadata execute() {
    checkArguments();
    try {
      HoodieTableMetaClient metaClient = table.getMetaClient();
      Map<BootstrapMode, List<Pair<String, List<HoodieFileStatus>>>> partitionSelections =
          listSourcePartitionsAndTagBootstrapMode(metaClient);

      // First run metadata bootstrap which will implicitly commit
      HoodieWriteMetadata metadataResult = metadataBootstrap(partitionSelections.get(BootstrapMode.METADATA_ONLY_BOOTSTRAP));
      // if there are full bootstrap to be performed, perform that too
      HoodieWriteMetadata fullBootstrapResult =
          fullBootstrap(partitionSelections.get(BootstrapMode.FULL_BOOTSTRAP));
      return new HoodieBootstrapWriteMetadata(metadataResult, fullBootstrapResult);
    } catch (IOException ioe) {
      throw new HoodieIOException(ioe.getMessage(), ioe);
    }
  }

  /**
   * Perform Metadata Bootstrap.
   * @param partitionFilesList List of partitions and files within that partitions
   */
  protected HoodieWriteMetadata metadataBootstrap(List<Pair<String, List<HoodieFileStatus>>> partitionFilesList) {
    if (null == partitionFilesList) {
      return null;
    }

    HoodieTableMetaClient metaClient = table.getMetaClient();
    metaClient.getActiveTimeline().createNewInstant(
        new HoodieInstant(State.REQUESTED, HoodieTimeline.COMMIT_ACTION,
            HoodieTimeline.METADATA_BOOTSTRAP_INSTANT_TS));

    table.getActiveTimeline().transitionRequestedToInflight(new HoodieInstant(State.REQUESTED,
        HoodieTimeline.COMMIT_ACTION, HoodieTimeline.METADATA_BOOTSTRAP_INSTANT_TS), Option.empty());

    JavaRDD<BootstrapWriteStatus> bootstrapWriteStatuses = runMetadataBootstrap(partitionFilesList);

    HoodieWriteMetadata result = new HoodieWriteMetadata();
    updateIndexAndCommitIfNeeded(bootstrapWriteStatuses.map(w -> w), result);
    return result;
  }

  @Override
  protected void commit(Option<Map<String, String>> extraMetadata, HoodieWriteMetadata result) {
    // Perform bootstrap index write and then commit. Make sure both record-key and bootstrap-index
    // is all done in a single job DAG.
    Map<String, List<Pair<BootstrapSourceFileMapping, HoodieWriteStat>>> bootstrapSourceAndStats =
        result.getWriteStatuses().collect().stream()
            .map(w -> ((BootstrapWriteStatus)w).getBootstrapSourceAndWriteStat())
            .collect(Collectors.groupingBy(w -> w.getKey().getHudiPartitionPath()));
    HoodieTableMetaClient metaClient = table.getMetaClient();
    try (BootstrapIndex.IndexWriter indexWriter = BootstrapIndex.getBootstrapIndex(metaClient)
        .createWriter(config.getBootstrapSourceBasePath())) {
      indexWriter.begin();
      bootstrapSourceAndStats.forEach((key, value) -> indexWriter.appendNextPartition(key,
          value.stream().map(Pair::getKey).collect(Collectors.toList())));
      indexWriter.finish();
    }
    super.commit(extraMetadata, result, bootstrapSourceAndStats.values().stream()
        .flatMap(f -> f.stream().map(Pair::getValue)).collect(Collectors.toList()));
  }

  /**
   * Perform Metadata Bootstrap.
   * @param partitionFilesList List of partitions and files within that partitions
   */
  protected HoodieWriteMetadata fullBootstrap(List<Pair<String, List<HoodieFileStatus>>> partitionFilesList) {
    if (null == partitionFilesList) {
      return null;
    }

    HoodieTableMetaClient metaClient = table.getMetaClient();
    metaClient.getActiveTimeline().createNewInstant(
        new HoodieInstant(State.REQUESTED, HoodieTimeline.COMMIT_ACTION, HoodieTimeline.FULL_BOOTSTRAP_INSTANT_TS));

    table.getActiveTimeline().transitionRequestedToInflight(new HoodieInstant(State.REQUESTED,
        HoodieTimeline.COMMIT_ACTION, HoodieTimeline.FULL_BOOTSTRAP_INSTANT_TS), Option.empty());

    FullBootstrapInputProvider inputProvider =
        (FullBootstrapInputProvider) ReflectionUtils.loadClass(config.getFullBootstrapInputProvider(),
            new TypedProperties(config.getProps()), jsc);
    JavaRDD<HoodieRecord> inputRecordsRDD =
        inputProvider.generateInputRecordRDD("bootstrap_source", partitionFilesList);
    return new BulkInsertCommitActionExecutor(jsc, config, table, HoodieTimeline.FULL_BOOTSTRAP_INSTANT_TS,
        inputRecordsRDD, Option.empty()).execute();
  }

  private BootstrapWriteStatus handleMetadataBootstrap(BootstrapSourceFileMapping bootstrapSourceFileMapping, String partitionPath,
      BootstrapKeyGenerator keyGenerator) {

    Path sourceFilePath = FileStatusUtils.toPath(bootstrapSourceFileMapping.getSourceFileStatus().getPath());
    HoodieBootstrapHandle bootstrapHandle = new HoodieBootstrapHandle(config, HoodieTimeline.METADATA_BOOTSTRAP_INSTANT_TS,
        table, partitionPath, FSUtils.createNewFileIdPfx(), table.getSparkTaskContextSupplier());
    try {
      ParquetMetadata readFooter = ParquetFileReader.readFooter(table.getHadoopConf(), sourceFilePath,
          ParquetMetadataConverter.NO_FILTER);
      MessageType parquetSchema = readFooter.getFileMetaData().getSchema();
      Schema avroSchema = new AvroSchemaConverter().convert(parquetSchema);
      Schema recordKeySchema = HoodieAvroUtils.generateProjectionSchema(avroSchema,
          keyGenerator.getTopLevelKeyColumns());
      LOG.info("Schema to be used for reading record Keys :" + recordKeySchema);
      AvroReadSupport.setAvroReadSchema(table.getHadoopConf(), recordKeySchema);
      AvroReadSupport.setRequestedProjection(table.getHadoopConf(), recordKeySchema);

      BoundedInMemoryExecutor<GenericRecord, HoodieRecord, Void> wrapper = null;
      try (ParquetReader<IndexedRecord> reader =
          AvroParquetReader.<IndexedRecord>builder(sourceFilePath).withConf(table.getHadoopConf()).build()) {
        wrapper = new SparkBoundedInMemoryExecutor<GenericRecord, HoodieRecord, Void>(config,
            new ParquetReaderIterator(reader), new BootstrapRecordWriter(bootstrapHandle), inp -> {
          String recKey = keyGenerator.getRecordKey(inp);
          GenericRecord gr = new GenericData.Record(HoodieAvroUtils.RECORD_KEY_SCHEMA);
          gr.put(HoodieRecord.RECORD_KEY_METADATA_FIELD, recKey);
          BootstrapRecordPayload payload = new BootstrapRecordPayload(gr);
          HoodieRecord rec = new HoodieRecord(new HoodieKey(recKey, partitionPath), payload);
          return rec;
        });
        wrapper.execute();
      } catch (Exception e) {
        throw new HoodieException(e);
      } finally {
        bootstrapHandle.close();
        if (null != wrapper) {
          wrapper.shutdownNow();
        }
      }
    } catch (IOException e) {
      throw new HoodieIOException(e.getMessage(), e);
    }
    BootstrapWriteStatus writeStatus = (BootstrapWriteStatus)bootstrapHandle.getWriteStatus();
    writeStatus.setBootstrapSourceFileMapping(bootstrapSourceFileMapping);
    return writeStatus;
  }

  private Map<BootstrapMode, List<Pair<String, List<HoodieFileStatus>>>> listSourcePartitionsAndTagBootstrapMode(
      HoodieTableMetaClient metaClient) throws IOException {
    List<Pair<String, List<HoodieFileStatus>>> folders =
        FSUtils.getAllLeafFoldersWithFiles(metaClient.getFs(),
            config.getBootstrapSourceBasePath(), new PathFilter() {
              @Override
              public boolean accept(Path path) {
                // TODO: Needs to be abstracted out when supporting different formats
                return path.getName().endsWith(".parquet");
              }
            });

    BootstrapSelector selector =
        (BootstrapSelector) ReflectionUtils.loadClass(config.getBootstrapPartitionSelectorClass(), config);

    Map<BootstrapMode, List<String>> result = selector.select(folders);
    Map<String, List<HoodieFileStatus>> partitionToFiles = folders.stream().collect(
        Collectors.toMap(Pair::getKey, Pair::getValue));

    // Ensure all partitions are accounted for
    ValidationUtils.checkArgument(partitionToFiles.keySet().equals(
        result.values().stream().flatMap(Collection::stream).collect(Collectors.toSet())));

    return result.entrySet().stream().map(e -> Pair.of(e.getKey(), e.getValue().stream()
        .map(p -> Pair.of(p, partitionToFiles.get(p))).collect(Collectors.toList())))
        .collect(Collectors.toMap(Pair::getKey, Pair::getValue));
  }

  private JavaRDD<BootstrapWriteStatus> runMetadataBootstrap(List<Pair<String, List<HoodieFileStatus>>> partitions) {
    if (null == partitions || partitions.isEmpty()) {
      return jsc.emptyRDD();
    }

    BootstrapKeyGenerator keyGenerator = new BootstrapKeyGenerator(config);

    return jsc.parallelize(partitions.stream()
        .flatMap(p -> p.getValue().stream().map(f -> Pair.of(p.getLeft(), f))).collect(Collectors.toList()),
        config.getBootstrapParallelism())
        .map(partitionFsPair -> {
          BootstrapSourceFileMapping sourceFileMapping = new BootstrapSourceFileMapping(
              config.getBootstrapSourceBasePath(), partitionFsPair.getLeft(), partitionFsPair.getLeft(),
              partitionFsPair.getValue(),
              FSUtils.getFileId(FileStatusUtils.toPath(partitionFsPair.getRight().getPath()).getName()));
          return handleMetadataBootstrap(sourceFileMapping, partitionFsPair.getLeft(), keyGenerator);
        });
  }

  //TODO: Once we decouple commit protocol, we should change the class hierarchy to avoid doing this.
  @Override
  protected Partitioner getUpsertPartitioner(WorkloadProfile profile) {
    return null;
  }

  @Override
  protected Partitioner getInsertPartitioner(WorkloadProfile profile) {
    return null;
  }

  @Override
  protected Iterator<List<WriteStatus>> handleInsert(String idPfx, Iterator<HoodieRecord<T>> recordItr)
      throws Exception {
    return null;
  }

  @Override
  protected Iterator<List<WriteStatus>> handleUpdate(String partitionPath, String fileId,
      Iterator<HoodieRecord<T>> recordItr) throws IOException {
    return null;
  }
}
