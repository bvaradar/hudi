package org.apache.hudi.io.storage;

import org.apache.spark.sql.catalyst.InternalRow;

import java.io.IOException;

public interface HoodieInternalRowFileWriter extends AutoCloseable {

    boolean canWrite();

    void writeRow(String key, InternalRow row) throws IOException;
}
