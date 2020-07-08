package org.apache.hudi.io;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hudi.client.HoodieInternalWriteStatus;
import org.apache.hudi.client.model.HoodieInternalRow;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodiePartitionMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieInsertException;
import org.apache.hudi.io.storage.HoodieInternalRowParquetWriter;
import org.apache.hudi.io.storage.HoodieParquetConfig;
import org.apache.hudi.table.HoodieTable;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.sql.catalyst.InternalRow;
import org.apache.spark.sql.types.StructType;

import java.io.IOException;
import java.io.Serializable;
import java.util.concurrent.atomic.AtomicLong;

public class HoodieRowCreateHandle implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LogManager.getLogger(HoodieRowCreateHandle.class);

    private final String instantTime;
    private final int taskPartitionId;
    private final long taskId;
    private final long taskEpochId;
    private final HoodieTable table;
    private final HoodieWriteConfig writeConfig;

    private final HoodieInternalRowParquetWriter fileWriter;
    private final String partitionPath;
    private final Path path;
    private final String fileId;
    private final FileSystem fs;
    private long recordsWritten = 0;
    private final HoodieTimer currTimer;

    private static AtomicLong SEQGEN = new AtomicLong(1);

    public HoodieRowCreateHandle(HoodieTable table, HoodieWriteConfig writeConfig, String partitionPath,
                                 String fileId, String instantTime, int taskPartitionId, long taskId, long taskEpochId,
                                 StructType structType) {
        this.partitionPath = partitionPath;
        this.table = table;
        this.writeConfig = writeConfig;
        this.instantTime = instantTime;
        this.taskPartitionId = taskPartitionId;
        this.taskId = taskId;
        this.taskEpochId = taskEpochId;
        this.fileId  = fileId;
        this.currTimer = new HoodieTimer();
        currTimer.startTimer();
        this.fs = table.getMetaClient().getFs();
        this.path = makeNewPath(partitionPath);
        try {
            HoodiePartitionMetadata partitionMetadata = new HoodiePartitionMetadata(fs, instantTime,
                    new Path(writeConfig.getBasePath()), FSUtils.getPartitionPath(writeConfig.getBasePath(),
                    partitionPath));
            partitionMetadata.trySave(taskPartitionId);
            createMarkerFile(partitionPath);
            this.fileWriter = new HoodieInternalRowParquetWriter(path, structType,
                    table.getMetaClient().getHadoopConf(), writeConfig, new HoodieParquetConfig(null,
                    writeConfig.getParquetCompressionCodec(), writeConfig.getParquetBlockSize(),
                    writeConfig.getParquetPageSize(), writeConfig.getParquetMaxFileSize(),
                    table.getMetaClient().getHadoopConf(), writeConfig.getParquetCompressionRatio()));
        } catch (IOException e) {
            throw new HoodieInsertException("Failed to initialize file writer for path " + path, e);
        }
        LOG.info("New handle created for partition :" + partitionPath + " with fileId " + fileId);
    }

    public void write(InternalRow record) throws IOException {
        String partitionPath = record.getUTF8String(
                HoodieRecord.HOODIE_META_COLUMNS_NAME_TO_POS.get(HoodieRecord.PARTITION_PATH_METADATA_FIELD)).toString();
        String seqId = HoodieRecord.generateSequenceId(instantTime, taskPartitionId, SEQGEN.getAndIncrement());
        recordsWritten++;
        String recordKey = record.getUTF8String(
                HoodieRecord.HOODIE_META_COLUMNS_NAME_TO_POS.get(HoodieRecord.RECORD_KEY_METADATA_FIELD)).toString();
        HoodieInternalRow internalRow = new HoodieInternalRow(instantTime, seqId, recordKey, partitionPath,
                path.getName(), record);
        fileWriter.writeRow(recordKey, internalRow);
    }

    public Path makeNewPath(String partitionPath) {
        Path path = FSUtils.getPartitionPath(writeConfig.getBasePath(), partitionPath);
        try {
            fs.mkdirs(path); // create a new partition as needed.
        } catch (IOException e) {
            throw new HoodieIOException("Failed to make dir " + path, e);
        }
        return new Path(path.toString(), FSUtils.makeDataFileName(instantTime, getWriteToken(), fileId,
                table.getMetaClient().getTableConfig().getBaseFileFormat().getFileExtension()));
    }

    /**
     * Creates an empty marker file corresponding to storage writer path.
     *
     * @param partitionPath Partition path
     */
    protected void createMarkerFile(String partitionPath) {
        Path markerPath = makeNewMarkerPath(partitionPath);
        try {
            LOG.info("Creating Marker Path=" + markerPath);
            fs.create(markerPath, false).close();
        } catch (IOException e) {
            throw new HoodieException("Failed to create marker file " + markerPath, e);
        }
    }

    /**
     * THe marker path will be <base-path>/.hoodie/.temp/<instant_ts>/2019/04/25/filename.
     */
    private Path makeNewMarkerPath(String partitionPath) {
        Path markerRootPath = new Path(table.getMetaClient().getMarkerFolderPath(instantTime));
        Path path = FSUtils.getPartitionPath(markerRootPath, partitionPath);
        try {
            fs.mkdirs(path); // create a new partition as needed.
        } catch (IOException e) {
            throw new HoodieIOException("Failed to make dir " + path, e);
        }
        return new Path(path.toString(), FSUtils.makeMarkerFile(instantTime, getWriteToken(), fileId));
    }

    private String getWriteToken() {
        return taskPartitionId + "-" + taskId + "-" + taskEpochId;
    }

    public HoodieInternalWriteStatus close() throws IOException {
        fileWriter.close();
        HoodieInternalWriteStatus writeStatus = new HoodieInternalWriteStatus(!table.getIndex().isImplicitWithStorage(),
                writeConfig.getWriteStatusFailureFraction());;
        writeStatus.setPartitionPath(partitionPath);
        writeStatus.setTotalRecords(recordsWritten);
        writeStatus.setPath(path.getName());
        writeStatus.setEndTime(currTimer.endTimer());

        HoodieWriteStat stat = new HoodieWriteStat();
        stat.setPartitionPath(writeStatus.getPartitionPath());
        stat.setNumWrites(writeStatus.getTotalRecords());
        stat.setNumDeletes(0);
        stat.setNumInserts(recordsWritten);
        stat.setPrevCommit(HoodieWriteStat.NULL_COMMIT);
        stat.setFileId(writeStatus.getFileId());

        stat.setPath(path.toString());
        long fileSizeInBytes = FSUtils.getFileSize(table.getMetaClient().getFs(), path);
        stat.setTotalWriteBytes(fileSizeInBytes);
        stat.setFileSizeInBytes(fileSizeInBytes);

        stat.setTotalWriteErrors(writeStatus.getFailedRowsSize());
        HoodieWriteStat.RuntimeStats runtimeStats = new HoodieWriteStat.RuntimeStats();
        runtimeStats.setTotalCreateTime(writeStatus.getEndTime());
        stat.setRuntimeStats(runtimeStats);
        writeStatus.setStat(stat);
        return writeStatus;
    }

    public boolean canWrite() {
        return fileWriter.canWrite();
    }

    public String getFileName() {
        return path.getName();
    }
}
