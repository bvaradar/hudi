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

import static org.apache.parquet.hadoop.ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED;
import static org.apache.parquet.hadoop.ParquetWriter.DEFAULT_IS_VALIDATING_ENABLED;
import static org.apache.parquet.hadoop.ParquetWriter.DEFAULT_WRITER_VERSION;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.client.EncodableWriteStatus;
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
import org.apache.hudi.io.storage.HoodieRowParquetWriteSupport;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.sources.v2.writer.DataWriter;
import org.apache.spark.sql.sources.v2.writer.WriterCommitMessage;
import org.apache.spark.sql.types.StructType;

public class HoodieDataWriter implements DataWriter<InternalRow> {

  private final String instantTime;
  private final int partitionId;
  private final long taskId;
  private final long epochId;
  private final HoodieTableMetaClient metaClient;
  private final HoodieWriteConfig writeConfig;
  private final StructType structType;
  private final List<EncodableWriteStatus> writeStatusList = new ArrayList<>();

  private String lastKnownPartitionPath = null;
  private HoodieRowParquetWriteSupport writeSupport;
  private ParquetWriter<InternalRow> writer;
  private String currFileId;
  private Path currFilePath;
  private long currRecordsWritten = 0;
  private long currInsertRecordsWritten = 0;
  private HoodieTimer currTimer;


  public HoodieDataWriter(HoodieTableMetaClient metaClient, HoodieWriteConfig writeConfig,
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
    /**
    String partitionPath = record.getUTF8String(
        HoodieRecord.HOODIE_META_COLUMNS_NAME_TO_POS.get(HoodieRecord.PARTITION_PATH_METADATA_FIELD)).toString();
    if (lastKnownPartitionPath == null || !lastKnownPartitionPath.equals(partitionPath)) {
      createNewParquetWriter(partitionPath);
      lastKnownPartitionPath = partitionPath;
    }
    currRecordsWritten++;
    currInsertRecordsWritten++;
     **/
    if (null == writer) {
      createNewParquetWriter("");
    }
    /**
    HoodieInternalRow internalRow = new HoodieInternalRow(instantTime,
        "dummy_seq", record.getString(2), record.getString(3), currFilePath.getName(),
        record);
    writer.write(internalRow);
     **/
    writer.write(record);
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
    currInsertRecordsWritten = 0;
    currRecordsWritten = 0;
    currFilePath = makeNewPath(partitionPath);
    // instantiate writer
    writer = new ParquetWriter<InternalRow>(currFilePath, writeSupport,
        writeConfig.getParquetCompressionCodec(), writeConfig.getParquetBlockSize(),
        writeConfig.getParquetPageSize(), (int) writeConfig.getParquetMaxFileSize(), DEFAULT_IS_DICTIONARY_ENABLED,
        DEFAULT_IS_VALIDATING_ENABLED, DEFAULT_WRITER_VERSION,
        writeSupport.getHadoopConf());
  }

  private Path makeNewPath(String partitionPath) {
    this.currFileId = UUID.randomUUID().toString();
    Path path = FSUtils.getPartitionPath(writeConfig.getBasePath(), partitionPath);
    try {
      metaClient.getFs().mkdirs(path); // create a new partition as needed.
      HoodiePartitionMetadata partitionMetadata = new HoodiePartitionMetadata(metaClient.getFs(), instantTime,
          new Path(writeConfig.getBasePath()), FSUtils.getPartitionPath(writeConfig.getBasePath(), partitionPath));
      partitionMetadata.trySave(new Long(taskId).intValue());
    } catch (IOException e) {
      throw new HoodieIOException("Failed to make dir " + path, e);
    }
    String writeToken = partitionId + "-" + taskId + "-" + epochId;
    return new Path(path.toString(), FSUtils.makeDataFileName(instantTime, writeToken, currFileId));
  }

  public void close() throws IOException {
    writer.close();
    EncodableWriteStatus encodableWriteStatus = new EncodableWriteStatus();
    encodableWriteStatus.setPartitionPath(lastKnownPartitionPath);
    encodableWriteStatus.setInsertRecordsWritten(currInsertRecordsWritten);
    encodableWriteStatus.setRecordsWritten(currRecordsWritten);
    encodableWriteStatus.setPath(currFilePath.getName());
    encodableWriteStatus.setEndTime(currTimer.endTimer());

    HoodieWriteStat stat = new HoodieWriteStat();
    stat.setPartitionPath(encodableWriteStatus.getPartitionPath());
    stat.setNumWrites(encodableWriteStatus.getRecordsWritten());
    stat.setNumDeletes(0);
    stat.setNumInserts(encodableWriteStatus.getInsertRecordsWritten());
    stat.setPrevCommit(HoodieWriteStat.NULL_COMMIT);
    stat.setFileId(encodableWriteStatus.getFileId());
    if (currFilePath != null) {
      Path fullPath = new Path(FSUtils.getPartitionPath(writeConfig.getBasePath(), lastKnownPartitionPath), currFilePath);
      stat.setPath(fullPath.toString());
      long fileSizeInBytes = FSUtils.getFileSize(metaClient.getFs(), fullPath);
      stat.setTotalWriteBytes(fileSizeInBytes);
      stat.setFileSizeInBytes(fileSizeInBytes);
    } else {
      stat.setTotalWriteBytes(0);
      stat.setFileSizeInBytes(0);
    }
    stat.setTotalWriteErrors(encodableWriteStatus.getFailedRowsSize());
    HoodieWriteStat.RuntimeStats runtimeStats = new HoodieWriteStat.RuntimeStats();
    runtimeStats.setTotalCreateTime(encodableWriteStatus.getEndTime());
    stat.setRuntimeStats(runtimeStats);
    encodableWriteStatus.setStat(stat);
    writeStatusList.add(encodableWriteStatus);
  }
}
