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

package org.apache.hudi.io.storage;

import org.apache.hudi.common.bloom.BloomFilter;
import org.apache.hudi.common.bloom.BloomFilterFactory;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.table.HoodieTable;

import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.types.StructType;

import static org.apache.hudi.common.model.HoodieFileFormat.PARQUET;

import java.io.IOException;

public class HoodieInternalRowFileWriterFactory {

  public static HoodieInternalRowFileWriter getInternalRowFileWriter(
      Path path, HoodieTable hoodieTable, HoodieWriteConfig config, StructType schema)
      throws IOException {
    final String extension = FSUtils.getFileExtension(path.getName());
    if (PARQUET.getFileExtension().equals(extension)) {
      return newParquetInternalRowFileWriter(path, config, schema, hoodieTable);
    }
    throw new UnsupportedOperationException(extension + " format not supported yet.");
  }

  private static HoodieInternalRowFileWriter newParquetInternalRowFileWriter(
      Path path, HoodieWriteConfig writeConfig, StructType structType, HoodieTable table)
      throws IOException {
    BloomFilter filter = BloomFilterFactory.createBloomFilter(
            writeConfig.getBloomFilterNumEntries(),
            writeConfig.getBloomFilterFPP(),
            writeConfig.getDynamicBloomFilterMaxNumEntries(),
            writeConfig.getBloomFilterType());
    HoodieRowParquetWriteSupport writeSupport =
            new HoodieRowParquetWriteSupport(table.getHadoopConf(), structType, filter);
    return new HoodieInternalRowParquetWriter(
        path, new HoodieRowParquetConfig(
            writeSupport,
            writeConfig.getParquetCompressionCodec(),
            writeConfig.getParquetBlockSize(),
            writeConfig.getParquetPageSize(),
            writeConfig.getParquetMaxFileSize(),
            writeSupport.getHadoopConf(),
            writeConfig.getParquetCompressionRatio()));
  }
}
