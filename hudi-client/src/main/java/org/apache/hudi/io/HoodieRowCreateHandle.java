package org.apache.hudi.io;

import org.apache.hadoop.fs.Path;
import org.apache.hudi.client.HoodieInternalWriteStatus;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.io.storage.HoodieInternalRowParquetWriter;
import org.apache.hudi.io.storage.HoodieRowParquetWriteSupport;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class HoodieRowCreateHandle implements Serializable {

    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LogManager.getLogger(HoodieRowCreateHandle.class);

    private final String instantTime;
    private final int taskPartitionId;
    private final long taskId;
    private final long taskEpochId;
    private final HoodieTableMetaClient metaClient;
    private final HoodieWriteConfig writeConfig;
    private final StructType structType;
    private final HoodieInternalWriteStatus writeStatus;

    private HoodieInternalRowParquetWriter writer;
    private final String partitionPath;
    private final String fileId;

    private static AtomicLong SEQGEN = new AtomicLong(1);

    public HoodieRowCreateHandle(HoodieTableMetaClient metaClient, HoodieWriteConfig writeConfig, String partitionPath,
                                 String fileId, String instantTime, int taskPartitionId, long taskId, long taskEpochId,
                                 StructType structType) {
        this.partitionPath = partitionPath;
        this.metaClient = metaClient;
        this.writeConfig = writeConfig;
        this.instantTime = instantTime;
        this.taskPartitionId = taskPartitionId;
        this.taskId = taskId;
        this.taskEpochId = taskEpochId;
        this.structType = structType;
        this.fileId  = fileId;
        this.writeStatus = new HoodieInternalWriteStatus(false, writeConfig.getWriteStatusFailureFraction());
    }

    
}
