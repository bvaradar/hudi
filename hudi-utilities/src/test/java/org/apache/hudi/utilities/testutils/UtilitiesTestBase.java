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

package org.apache.hudi.utilities.testutils;

import org.apache.hudi.common.HoodieTestDataGenerator;
import org.apache.hudi.common.TestRawTripPayload;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.testutils.HoodieTestUtils;
import org.apache.hudi.common.testutils.minicluster.HdfsTestService;
import org.apache.hudi.common.util.CollectionUtils;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.hive.HiveSyncConfig;
import org.apache.hudi.hive.HoodieHiveClient;
import org.apache.hudi.hive.testutils.HiveTestService;
import org.apache.hudi.utilities.UtilHelpers;
import org.apache.hudi.utilities.sources.TestDataSource;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.fasterxml.jackson.dataformat.csv.CsvSchema.Builder;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.service.server.HiveServer2;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetFileWriter.Mode;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Abstract test that provides a dfs & spark contexts.
 *
 */
public class UtilitiesTestBase {

  protected static String dfsBasePath;
  protected static HdfsTestService hdfsTestService;
  protected static MiniDFSCluster dfsCluster;
  protected static DistributedFileSystem dfs;
  protected transient JavaSparkContext jsc = null;
  protected transient SparkSession sparkSession = null;
  protected transient SQLContext sqlContext;
  protected static HiveServer2 hiveServer;
  protected static HiveTestService hiveTestService;
  private static ObjectMapper mapper = new ObjectMapper();

  @BeforeAll
  public static void initClass() throws Exception {
    initClass(false);
  }

  public static void initClass(boolean startHiveService) throws Exception {
    hdfsTestService = new HdfsTestService();
    dfsCluster = hdfsTestService.start(true);
    dfs = dfsCluster.getFileSystem();
    dfsBasePath = dfs.getWorkingDirectory().toString();
    dfs.mkdirs(new Path(dfsBasePath));
    if (startHiveService) {
      hiveTestService = new HiveTestService(hdfsTestService.getHadoopConf());
      hiveServer = hiveTestService.start();
      clearHiveDb();
    }
  }

  @AfterAll
  public static void cleanupClass() {
    if (hdfsTestService != null) {
      hdfsTestService.stop();
    }
    if (hiveServer != null) {
      hiveServer.stop();
    }
    if (hiveTestService != null) {
      hiveTestService.stop();
    }
  }

  @BeforeEach
  public void setup() throws Exception {
    TestDataSource.initDataGen();
    jsc = UtilHelpers.buildSparkContext(this.getClass().getName() + "-hoodie", "local[2]");
    sqlContext = new SQLContext(jsc);
    sparkSession = SparkSession.builder().config(jsc.getConf()).getOrCreate();
  }

  @AfterEach
  public void teardown() throws Exception {
    TestDataSource.resetDataGen();
    if (jsc != null) {
      jsc.stop();
    }
  }

  /**
   * Helper to get hive sync config.
   * 
   * @param basePath
   * @param tableName
   * @return
   */
  protected static HiveSyncConfig getHiveSyncConfig(String basePath, String tableName) {
    HiveSyncConfig hiveSyncConfig = new HiveSyncConfig();
    hiveSyncConfig.jdbcUrl = "jdbc:hive2://127.0.0.1:9999/";
    hiveSyncConfig.hiveUser = "";
    hiveSyncConfig.hivePass = "";
    hiveSyncConfig.databaseName = "testdb1";
    hiveSyncConfig.tableName = tableName;
    hiveSyncConfig.basePath = basePath;
    hiveSyncConfig.assumeDatePartitioning = false;
    hiveSyncConfig.usePreApacheInputFormat = false;
    hiveSyncConfig.partitionFields = CollectionUtils.createImmutableList("datestr");
    return hiveSyncConfig;
  }

  /**
   * Initialize Hive DB.
   * 
   * @throws IOException
   */
  private static void clearHiveDb() throws IOException {
    HiveConf hiveConf = new HiveConf();
    // Create Dummy hive sync config
    HiveSyncConfig hiveSyncConfig = getHiveSyncConfig("/dummy", "dummy");
    hiveConf.addResource(hiveServer.getHiveConf());
    HoodieTableMetaClient.initTableType(dfs.getConf(), hiveSyncConfig.basePath, HoodieTableType.COPY_ON_WRITE,
        hiveSyncConfig.tableName, null);
    HoodieHiveClient client = new HoodieHiveClient(hiveSyncConfig, hiveConf, dfs);
    client.updateHiveSQL("drop database if exists " + hiveSyncConfig.databaseName);
    client.updateHiveSQL("create database " + hiveSyncConfig.databaseName);
    client.close();
  }

  public static class Helpers {

    // to get hold of resources bundled with jar
    private static ClassLoader classLoader = Helpers.class.getClassLoader();

    public static String readFile(String testResourcePath) throws IOException {
      BufferedReader reader =
          new BufferedReader(new InputStreamReader(classLoader.getResourceAsStream(testResourcePath)));
      StringBuffer sb = new StringBuffer();
      String line;
      while ((line = reader.readLine()) != null) {
        sb.append(line + "\n");
      }
      return sb.toString();
    }

    public static void copyToDFS(String testResourcePath, FileSystem fs, String targetPath) throws IOException {
      PrintStream os = new PrintStream(fs.create(new Path(targetPath), true));
      os.print(readFile(testResourcePath));
      os.flush();
      os.close();
    }

    public static void savePropsToDFS(TypedProperties props, FileSystem fs, String targetPath) throws IOException {
      String[] lines = props.keySet().stream().map(k -> String.format("%s=%s", k, props.get(k))).toArray(String[]::new);
      saveStringsToDFS(lines, fs, targetPath);
    }

    public static void saveStringsToDFS(String[] lines, FileSystem fs, String targetPath) throws IOException {
      PrintStream os = new PrintStream(fs.create(new Path(targetPath), true));
      for (String l : lines) {
        os.println(l);
      }
      os.flush();
      os.close();
    }

    /**
     * Converts the json records into CSV format and writes to a file.
     *
     * @param hasHeader  whether the CSV file should have a header line.
     * @param sep  the column separator to use.
     * @param lines  the records in JSON format.
     * @param fs  {@link FileSystem} instance.
     * @param targetPath  File path.
     * @throws IOException
     */
    public static void saveCsvToDFS(
        boolean hasHeader, char sep,
        String[] lines, FileSystem fs, String targetPath) throws IOException {
      Builder csvSchemaBuilder = CsvSchema.builder();

      ArrayNode arrayNode = mapper.createArrayNode();
      Arrays.stream(lines).forEachOrdered(
          line -> {
            try {
              arrayNode.add(mapper.readValue(line, ObjectNode.class));
            } catch (IOException e) {
              throw new HoodieIOException(
                  "Error converting json records into CSV format: " + e.getMessage());
            }
          });
      arrayNode.get(0).fieldNames().forEachRemaining(csvSchemaBuilder::addColumn);
      ObjectWriter csvObjWriter = new CsvMapper()
          .writerFor(JsonNode.class)
          .with(csvSchemaBuilder.setUseHeader(hasHeader).setColumnSeparator(sep).build());
      PrintStream os = new PrintStream(fs.create(new Path(targetPath), true));
      csvObjWriter.writeValue(os, arrayNode);
      os.flush();
      os.close();
    }

    public static void saveParquetToDFS(List<GenericRecord> records, Path targetFile) throws IOException {
      try (ParquetWriter<GenericRecord> writer = AvroParquetWriter.<GenericRecord>builder(targetFile)
          .withSchema(HoodieTestDataGenerator.AVRO_SCHEMA)
          .withConf(HoodieTestUtils.getDefaultHadoopConf())
          .withWriteMode(Mode.OVERWRITE)
          .build()) {
        for (GenericRecord record : records) {
          writer.write(record);
        }
      }
    }

    public static TypedProperties setupSchemaOnDFS() throws IOException {
      return setupSchemaOnDFS("source.avsc");
    }

    public static TypedProperties setupSchemaOnDFS(String filename) throws IOException {
      UtilitiesTestBase.Helpers.copyToDFS("delta-streamer-config/" + filename, dfs, dfsBasePath + "/" + filename);
      TypedProperties props = new TypedProperties();
      props.setProperty("hoodie.deltastreamer.schemaprovider.source.schema.file", dfsBasePath + "/" + filename);
      return props;
    }

    public static GenericRecord toGenericRecord(HoodieRecord hoodieRecord) {
      try {
        Option<IndexedRecord> recordOpt = hoodieRecord.getData().getInsertValue(HoodieTestDataGenerator.AVRO_SCHEMA);
        return (GenericRecord) recordOpt.get();
      } catch (IOException e) {
        return null;
      }
    }

    public static List<GenericRecord> toGenericRecords(List<HoodieRecord> hoodieRecords) {
      List<GenericRecord> records = new ArrayList<GenericRecord>();
      for (HoodieRecord hoodieRecord : hoodieRecords) {
        records.add(toGenericRecord(hoodieRecord));
      }
      return records;
    }

    public static String toJsonString(HoodieRecord hr) {
      try {
        return ((TestRawTripPayload) hr.getData()).getJsonData();
      } catch (IOException ioe) {
        return null;
      }
    }

    public static String[] jsonifyRecords(List<HoodieRecord> records) {
      return records.stream().map(Helpers::toJsonString).toArray(String[]::new);
    }
  }
}
