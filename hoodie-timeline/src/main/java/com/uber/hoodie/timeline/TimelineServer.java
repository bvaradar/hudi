/*
 * Copyright (c) 2018 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *          http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.uber.hoodie.timeline;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.uber.hoodie.common.SerializableConfiguration;
import com.uber.hoodie.common.table.view.FileSystemViewManager;
import com.uber.hoodie.common.table.view.FileSystemViewStorageConfig;
import com.uber.hoodie.common.table.view.FileSystemViewStorageType;
import com.uber.hoodie.common.util.FSUtils;
import io.javalin.Javalin;
import java.io.IOException;
import java.io.Serializable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * A stand alone timeline server exposing File-System View interfaces to clients
 */
public class TimelineServer {

  private static volatile Logger log = LogManager.getLogger(TimelineServer.class);

  private final int serverPort;
  private Configuration conf;
  private transient FileSystem fs;
  private transient Javalin app = null;
  private transient FileSystemViewManager fsViewsManager;

  public int getServerPort() {
    return serverPort;
  }

  public TimelineServer(int serverPort, FileSystemViewManager globalFileSystemViewManager) throws IOException {
    this.conf = FSUtils.prepareHadoopConf(new Configuration());
    this.fs = FileSystem.get(conf);
    this.serverPort = serverPort;
    this.fsViewsManager = globalFileSystemViewManager;
  }

  public TimelineServer(Config config) throws IOException {
    this(config.serverPort, buildFileSystemViewManager(config,
        new SerializableConfiguration(FSUtils.prepareHadoopConf(new Configuration()))));
  }

  public static class Config implements Serializable {

    @Parameter(names = {"--server-port", "-p"}, description = " Server Port")
    public Integer serverPort = 26754;

    @Parameter(names = {"--view-storage", "-st"}, description = "View Storage Type. Defaut - SPILLABLE_DISK")
    public FileSystemViewStorageType viewStorageType = FileSystemViewStorageType.SPILLABLE_DISK;

    @Parameter(names = {"--max-view-mem-per-table", "-mv"},
        description = "Maximum view memory per table in MB to be used for storing file-groups."
            + " Overflow file-groups will be spilled to disk. Used for SPILLABLE_DISK storage type")
    public Integer maxViewMemPerTableInMB = 2048;

    @Parameter(names = {"--mem-overhead-fraction-pending-compaction", "-cf"},
        description = "Memory Fraction of --max-view-mem-per-table to be allocated for managing pending compaction"
            + " storage. Overflow entries will be spilled to disk. Used for SPILLABLE_DISK storage type")
    public Double memFractionForCompactionPerTable = 0.001;

    @Parameter(names = {"--base-store-path", "-sp"},
        description = "Directory where spilled view entries will be stored. Used for SPILLABLE_DISK storage type")
    public String baseStorePathForFileGroups = FileSystemViewStorageConfig.DEFAULT_VIEW_SPILLABLE_DIR;

    @Parameter(names = {"--rocksdb-path", "-rp"},
        description = "Root directory for RocksDB")
    public String rocksDBPath = FileSystemViewStorageConfig.DEFAULT_ROCKSDB_BASE_PATH_PROP;

    @Parameter(names = {"--reset-rocksdb-path", "-rr"},
        description = "reset root directory for RocksDB")
    public Boolean resetRocksDBOnInstantiation = false;

    @Parameter(names = {"--help", "-h"})
    public Boolean help = false;
  }

  public void startService() throws IOException {
    app = Javalin.create();
    FileSystemViewHandler router = new FileSystemViewHandler(app, conf, fsViewsManager);
    app.get("/", ctx -> ctx.result("Hello World"));
    router.register();
    app.start(serverPort);
    log.info("Starting Timeline server on port :" + serverPort);
  }

  public void run() throws IOException {
    startService();
  }

  public static FileSystemViewManager buildFileSystemViewManager(Config config, SerializableConfiguration conf) {
    switch (config.viewStorageType) {
      case MEMORY:
        FileSystemViewStorageConfig.Builder inMemConfBuilder = FileSystemViewStorageConfig.newBuilder();
        inMemConfBuilder.withStorageType(FileSystemViewStorageType.MEMORY);
        return FileSystemViewManager.createViewManager(conf, inMemConfBuilder.build());
      case SPILLABLE_DISK: {
        FileSystemViewStorageConfig.Builder spillableConfBuilder = FileSystemViewStorageConfig.newBuilder();
        spillableConfBuilder.withStorageType(FileSystemViewStorageType.SPILLABLE_DISK)
            .withBaseStoreDir(config.baseStorePathForFileGroups)
            .withMaxMemoryForView(config.maxViewMemPerTableInMB * 1024 * 1024L)
            .withMemFractionForPendingCompaction(config.memFractionForCompactionPerTable);
        return FileSystemViewManager.createViewManager(conf, spillableConfBuilder.build());
      }
      case EMBEDDED_KV_STORE: {
        FileSystemViewStorageConfig.Builder spillableConfBuilder = FileSystemViewStorageConfig.newBuilder();
        spillableConfBuilder.withStorageType(FileSystemViewStorageType.EMBEDDED_KV_STORE)
            .withRocksDBPath(config.rocksDBPath)
            .withResetOnInstantiation(config.resetRocksDBOnInstantiation);
        return FileSystemViewManager.createViewManager(conf, spillableConfBuilder.build());
      }
      default:
        throw new IllegalArgumentException("Invalid view manager storage type :" + config.viewStorageType);
    }
  }

  public void close() {
    this.app.stop();
    this.app = null;
    this.fsViewsManager.close();
  }

  public Configuration getConf() {
    return conf;
  }

  public FileSystem getFs() {
    return fs;
  }

  public static void main(String[] args) throws Exception {
    final Config cfg = new Config();
    JCommander cmd = new JCommander(cfg, args);
    if (cfg.help || args.length == 0) {
      cmd.usage();
      System.exit(1);
    }

    Configuration conf = FSUtils.prepareHadoopConf(new Configuration());
    FileSystemViewManager viewManager = buildFileSystemViewManager(cfg, new SerializableConfiguration(conf));
    TimelineServer service = new TimelineServer(cfg.serverPort, viewManager);
    service.run();
  }
}
