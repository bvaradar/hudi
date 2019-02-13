/*
 *  Copyright (c) 2018 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.uber.hoodie.func;

import com.uber.hoodie.common.SerializableConfiguration;
import com.uber.hoodie.common.model.HoodieRecordPayload;
import com.uber.hoodie.common.table.view.FileSystemViewManager;
import com.uber.hoodie.common.table.view.FileSystemViewStorageConfig;
import com.uber.hoodie.common.table.view.FileSystemViewStorageType;
import com.uber.hoodie.common.util.NetworkUtils;
import com.uber.hoodie.common.util.collection.Pair;
import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.hoodie.timeline.TimelineServer;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Timeline Service that runs in Driver
 */
public class EmbeddedTimelineServer {

  private static Logger logger = LogManager.getLogger(EmbeddedTimelineServer.class);

  private final int serverPort;
  private String hostAddr;
  private final SerializableConfiguration hadoopConf;
  private final HoodieWriteConfig config;
  private transient FileSystemViewManager viewManager;
  private transient TimelineServer server;

  public EmbeddedTimelineServer(Configuration hadoopConf, HoodieWriteConfig config) {
    Pair<String, Integer> hostPort = NetworkUtils.getAvailableServerPortWithHostname();
    this.hostAddr = hostPort.getKey();
    this.serverPort = hostPort.getRight();
    this.config = config;
    this.hadoopConf = new SerializableConfiguration(hadoopConf);
    this.viewManager = createViewManager();
  }

  public <T extends HoodieRecordPayload> boolean preloadNeededPartitions(JavaSparkContext jsc,
      List<String> partitions) {
    // Pre populate File System View of the table
    logger.info("Pre-populating file system view in driver. Total Partitions :" + partitions.size());
    try {
      new ForkJoinPool(config.getMaxFileStatusListerThreads()).submit(() ->
          partitions.parallelStream().forEach(
              partition -> {
                // logger.info("Finding latest slices in partition " + partition);
                long numFileSlices =
                    viewManager.getFileSystemView(config.getBasePath()).getLatestFileSlices(partition).count();
                logger.info("Found " + numFileSlices + " latest slices in partition " + partition);
              }))
          .get();
    } catch (InterruptedException | ExecutionException e) {
      logger.error("Got exception when pre populating file system view", e);
      return false;
    }
    logger.info("Pre-population of File system view finished for partitions :" + partitions.size());
    setHostAddrFromSparkConf(jsc.getConf());
    return true;
  }

  private FileSystemViewManager createViewManager() {
    FileSystemViewStorageConfig sConf = buildStorageConfigForServer();
    return FileSystemViewManager.createViewManager(hadoopConf, sConf);
  }

  private FileSystemViewStorageConfig buildStorageConfigForServer() {
    // Using passed-in configs to build view storage configs
    FileSystemViewStorageConfig.Builder builder =
        FileSystemViewStorageConfig.newBuilder().fromProperties(
            config.getClientSpecifiedViewStorageConfig().getProps());
    FileSystemViewStorageType storageType = builder.build().getStorageType();
    if (storageType.equals(FileSystemViewStorageType.REMOTE_ONLY)
        || storageType.equals(FileSystemViewStorageType.REMOTE_FIRST)) {
      // Reset to default if set to Remote
      builder.withStorageType(FileSystemViewStorageType.MEMORY);
    }
    return builder.build();
  }

  public void startServer() throws IOException {
    logger.info("Starting embedded timeline server on port " + serverPort);
    server = new TimelineServer(serverPort, viewManager);
    server.startService();
  }

  private void setHostAddrFromSparkConf(SparkConf sparkConf) {
    String hostAddr = sparkConf.get("spark.driver.host", null);
    if (hostAddr != null) {
      logger.info("Overriding hostIp to (" + hostAddr + ") found in spark-conf. It was " + this.hostAddr);
      this.hostAddr = hostAddr;
    } else {
      logger.warn("Unable to find driver bind address from spark config");
    }
  }

  /**
   * Retrieves proper view storage configs for remote clients to access this service
   * @return
   */
  public FileSystemViewStorageConfig getRemoteFileSystemViewConfig() {
    return FileSystemViewStorageConfig.newBuilder().withStorageType(FileSystemViewStorageType.REMOTE_FIRST)
            .withRemoteServerHost(hostAddr).withRemoteServerPort(serverPort).build();
  }

  public void stop() {
    if (null != server) {
      this.server.close();
      this.server = null;
      this.viewManager = null;
    }
  }
}