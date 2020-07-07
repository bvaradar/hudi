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

import static org.apache.parquet.hadoop.ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED;
import static org.apache.parquet.hadoop.ParquetWriter.DEFAULT_IS_VALIDATING_ENABLED;
import static org.apache.parquet.hadoop.ParquetWriter.DEFAULT_WRITER_VERSION;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.client.HoodieInternalWriteStatus;
import org.apache.hudi.client.HoodieWriteClient;
import org.apache.hudi.client.WriteStatus;
import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.bloom.BloomFilterFactory;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodiePartitionMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.io.storage.HoodieParquetConfig;
import org.apache.hudi.io.storage.HoodieRowParquetWriteSupport;
import org.apache.hudi.io.storage.HoodieInternalRowParquetWriter;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.writer.DataWriter;
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;

public class HoodieDataInternalWriter implements DataWriter<InternalRow> {

  private static final long serialVersionUID = 1L;
  private static final Logger LOG = LogManager.getLogger(HoodieDataInternalWriter.class);

  private final String instantTime;
  private final int partitionId;
  private final long taskId;
  private final long epochId;
  private final HoodieTableMetaClient metaClient;
  private final HoodieWriteConfig writeConfig;
  private final StructType structType;
  private final List<HoodieInternalWriteStatus> writeStatusList = new ArrayList<>();

  protected HoodieInternalWriteStatus writeStatus;
  private HoodieInternalRowParquetWriter writer;
  private String lastKnownPartitionPath = null;
  private HoodieRowParquetWriteSupport writeSupport;
  private String currFileId;
  private Path currFilePath;
  private long currRecordsWritten = 0;
  private HoodieTimer currTimer;

  private static AtomicLong SEQGEN = new AtomicLong(1);

  public HoodieDataInternalWriter(HoodieTableMetaClient metaClient, HoodieWriteConfig writeConfig,
      String instantTime, int partitionId, long taskId, long epochId, StructType structType) {
    this.metaClient = metaClient;
    this.writeConfig = writeConfig;
    this.instantTime = instantTime;
    this.partitionId = partitionId;
    this.taskId = taskId;
    this.epochId = epochId;
    this.structType = structType;
  }

  @Override
  public void write(InternalRow record) throws IOException {
    String partitionPath = record.getUTF8String(
        HoodieRecord.HOODIE_META_COLUMNS_NAME_TO_POS.get(HoodieRecord.PARTITION_PATH_METADATA_FIELD)).toString();
    if ((lastKnownPartitionPath == null) || !lastKnownPartitionPath.equals(partitionPath) || !writer.canWrite()) {
      LOG.info("Creating new file for partition path " + partitionPath);
      createNewParquetWriter(partitionPath);
      lastKnownPartitionPath = partitionPath;
    }

    String seqId = HoodieRecord.generateSequenceId(instantTime, partitionId, SEQGEN.getAndIncrement());
    currRecordsWritten++;
    String recordKey = record.getUTF8String(
        HoodieRecord.HOODIE_META_COLUMNS_NAME_TO_POS.get(HoodieRecord.RECORD_KEY_METADATA_FIELD)).toString();
    HoodieInternalRow internalRow = new HoodieInternalRow(instantTime, seqId, recordKey, lastKnownPartitionPath,
        currFilePath.getName(), record);
    writer.write(internalRow);
    writeSupport.add(recordKey);
  }

  @Override
  public WriterCommitMessage commit() throws IOException {
    close();
    return new HoodieWriterCommitMessage(writeStatusList);
  }

  @Override
  public void abort() throws IOException {
    writeStatusList.forEach(ws -> {
      Path filePath =
          new Path(FSUtils.getPartitionPath(metaClient.getBasePath(), ws.getPartitionPath()), ws.getFilePath());
      try {
        metaClient.getFs().delete(filePath);
      } catch (IOException e) {
        throw new HoodieIOException(e.getMessage());
      }
    });
  }

  private void createNewParquetWriter(String partitionPath) throws IOException {
    BloomFilter filter = BloomFilterFactory
        .createBloomFilter(writeConfig.getBloomFilterNumEntries(), writeConfig.getBloomFilterFPP(),
            writeConfig.getDynamicBloomFilterMaxNumEntries(),
            writeConfig.getBloomFilterType());
    writeSupport = new HoodieRowParquetWriteSupport(metaClient.getHadoopConf(), structType, filter);

    if (null != writer) {
      close();
    }
    currTimer = new HoodieTimer();
    currTimer.startTimer();
    currRecordsWritten = 0;
    currFilePath = makeNewPath(partitionPath);
    // instantiate writer
    writer = new HoodieInternalRowParquetWriter(currFilePath, structType, metaClient.getHadoopConf(), writeConfig,
            new HoodieParquetConfig(null, writeConfig.getParquetCompressionCodec(),
            writeConfig.getParquetBlockSize(), writeConfig.getParquetPageSize(), writeConfig.getParquetMaxFileSize(),
            metaClient.getHadoopConf(), writeConfig.getParquetCompressionRatio()));
  }

  private Path makeNewPath(String partitionPath) {
    this.currFileId = UUID.randomUUID().toString();
    Path path = FSUtils.getPartitionPath(writeConfig.getBasePath(), partitionPath);
    if ((lastKnownPartitionPath == null) || (!lastKnownPartitionPath.equals(partitionPath))) {
      try {
        metaClient.getFs().mkdirs(path); // create a new partition as needed.
        HoodiePartitionMetadata partitionMetadata = new HoodiePartitionMetadata(metaClient.getFs(), instantTime,
            new Path(writeConfig.getBasePath()), FSUtils.getPartitionPath(writeConfig.getBasePath(), partitionPath));
        partitionMetadata.trySave(new Long(taskId).intValue());
      } catch (IOException e) {
        throw new HoodieIOException("Failed to make dir " + path, e);
      }
    }
    String writeToken = partitionId + "-" + taskId + "-" + epochId;
    return new Path(path.toString(), FSUtils.makeDataFileName(instantTime, writeToken, currFileId));
  }

  public void close() throws IOException {
    writer.close();
    HoodieInternalWriteStatus writeStatus = new HoodieInternalWriteStatus(false,
        writeConfig.getWriteStatusFailureFraction());
    writeStatus.setPartitionPath(lastKnownPartitionPath);
    writeStatus.setTotalRecords(currRecordsWritten);
    writeStatus.setPath(currFilePath.getName());
    writeStatus.setEndTime(currTimer.endTimer());

    HoodieWriteStat stat = new HoodieWriteStat();
    stat.setPartitionPath(writeStatus.getPartitionPath());
    stat.setNumWrites(writeStatus.getTotalRecords());
    stat.setNumDeletes(0);
    stat.setNumInserts(currRecordsWritten);
    stat.setPrevCommit(HoodieWriteStat.NULL_COMMIT);
    stat.setFileId(writeStatus.getFileId());

    Path fullPath = new Path(FSUtils.getPartitionPath(writeConfig.getBasePath(), lastKnownPartitionPath), currFilePath);
    stat.setPath(fullPath.toString());
    long fileSizeInBytes = FSUtils.getFileSize(metaClient.getFs(), fullPath);
    stat.setTotalWriteBytes(fileSizeInBytes);
    stat.setFileSizeInBytes(fileSizeInBytes);

    stat.setTotalWriteErrors(writeStatus.getFailedRowsSize());
    HoodieWriteStat.RuntimeStats runtimeStats = new HoodieWriteStat.RuntimeStats();
    runtimeStats.setTotalCreateTime(writeStatus.getEndTime());
    stat.setRuntimeStats(runtimeStats);
    writeStatus.setStat(stat);
    writeStatusList.add(writeStatus);
  }
}
