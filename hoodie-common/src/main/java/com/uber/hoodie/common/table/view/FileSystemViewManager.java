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

package com.uber.hoodie.common.table.view;

import com.uber.hoodie.common.SerializableConfiguration;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.table.HoodieTimeline;
import com.uber.hoodie.common.table.HoodieView;
import com.uber.hoodie.common.util.Functions.Function2;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * A container that can potentially hold one or more dataset's
 * file-system views. There is one view for each dataset. This is a view built against a timeline containing completed
 * actions. In an embedded timeline-server mode, this typically holds only one dataset's view.
 * In a stand-alone server mode, this can hold more than one dataset's views.
 */
public class FileSystemViewManager {
  private static Logger logger = LogManager.getLogger(FileSystemViewManager.class);

  private final SerializableConfiguration conf;
  // The View Storage config used to store file-system views
  private final FileSystemViewStorageConfig viewConfig;
  // Map from Base-Path to View
  private final ConcurrentHashMap<String, HoodieView> globalViewMap;
  // Factory Map to create file-system views
  private final Function2<String, FileSystemViewStorageConfig, HoodieView> viewCreator;

  public FileSystemViewManager(SerializableConfiguration conf, FileSystemViewStorageConfig viewConfig,
      Function2<String, FileSystemViewStorageConfig, HoodieView> viewCreator) {
    this.conf = conf;
    this.viewConfig = viewConfig;
    this.globalViewMap = new ConcurrentHashMap<>();
    this.viewCreator = viewCreator;
  }

  /**
   * Drops reference to File-System Views. Future calls to view results in creating a new view
   * @param basePath
   */
  public void clearFileSystemView(String basePath) {
    HoodieView view = globalViewMap.remove(basePath);
    if (view != null) {
      view.close();
    }
  }

  /**
   * Main API to get the file-system view for the base-path
   * @param basePath
   * @return
   */
  public HoodieView getFileSystemView(String basePath) {
    return globalViewMap.computeIfAbsent(basePath,
        (path) -> viewCreator.apply(path, viewConfig));
  }

  /**
   * Closes all views opened
   */
  public void close() {
    this.globalViewMap.values().stream().forEach(v -> v.close());
    this.globalViewMap.clear();
  }

  // FACTORY METHODS FOR CREATING FILE-SYSTEM VIEWS

  /**
   * Create RocksDB based file System view for a dataset
   * @param conf Hadoop Configuration
   * @param viewConf  View Storage Configuration
   * @param basePath  Base Path of dataset
   * @return
   */
  private static RocksDbBasedFileSystemView createRocksDBBasedFileSystemView(SerializableConfiguration conf,
      FileSystemViewStorageConfig viewConf, String basePath) {
    HoodieTableMetaClient metaClient = new HoodieTableMetaClient(conf.get(), basePath, true);
    HoodieTimeline timeline = metaClient.getActiveTimeline().filterCompletedAndCompactionInstants();
    return new RocksDbBasedFileSystemView(metaClient, timeline, viewConf);
  }

  /**
   * Create a spillable Map based file System view for a dataset
   * @param conf Hadoop Configuration
   * @param viewConf  View Storage Configuration
   * @param basePath  Base Path of dataset
   * @return
   */
  private static SpillableMapBasedFileSystemView createSpillableMapBasedFileSystemView(SerializableConfiguration conf,
      FileSystemViewStorageConfig viewConf, String basePath) {
    logger.warn("Creating SpillableMap based view for basePath " + basePath);
    HoodieTableMetaClient metaClient = new HoodieTableMetaClient(conf.get(), basePath, true);
    HoodieTimeline timeline = metaClient.getActiveTimeline().filterCompletedAndCompactionInstants();
    return new SpillableMapBasedFileSystemView(metaClient, timeline, viewConf);
  }


  /**
   * Create an in-memory file System view for a dataset
   * @param conf Hadoop Configuration
   * @param viewConf  View Storage Configuration
   * @param basePath  Base Path of dataset
   * @return
   */
  private static HoodieTableFileSystemView createInMemoryFileSystemView(SerializableConfiguration conf,
      FileSystemViewStorageConfig viewConf, String basePath) {
    logger.warn("Creating InMemory based view for basePath " + basePath);
    HoodieTableMetaClient metaClient = new HoodieTableMetaClient(conf.get(), basePath, true);
    HoodieTimeline timeline = metaClient.getActiveTimeline().filterCompletedAndCompactionInstants();
    return new HoodieTableFileSystemView(metaClient, timeline);
  }

  /**
   * Create a remote file System view for a dataset
   * @param conf Hadoop Configuration
   * @param viewConf  View Storage Configuration
   * @param metaClient  Hoodie Table MetaClient for the dataset.
   * @return
   */
  private static RemoteHoodieTableFileSystemView createRemoteFileSystemView(SerializableConfiguration conf,
      FileSystemViewStorageConfig viewConf, HoodieTableMetaClient metaClient) {
    logger.info("Creating remote view for basePath " + metaClient.getBasePath() + ". Server="
        + viewConf.getRemoteViewServerHost() + ":" + viewConf.getRemoteViewServerPort());
    return new RemoteHoodieTableFileSystemView(viewConf.getRemoteViewServerHost(),
        viewConf.getRemoteViewServerPort(), metaClient);
  }

  /**
   * Main Factory method for building file-system views
   * @param conf  Hadoop Configuration
   * @param config View Storage Configuration
   * @return
   */
  public static FileSystemViewManager createViewManager(
      final SerializableConfiguration conf, final FileSystemViewStorageConfig config) {
    logger.info("Creating View Manager with storage type :" + config.getStorageType());
    switch (config.getStorageType()) {
      case EMBEDDED_KV_STORE:
        logger.info("Creating embedded rocks-db based Table View");
        return new FileSystemViewManager(conf, config,
            (basePath, viewConf) -> createRocksDBBasedFileSystemView(conf, viewConf, basePath));
      case SPILLABLE_DISK:
        logger.info("Creating Spillable Disk based Table View");
        return new FileSystemViewManager(conf, config,
            (basePath, viewConf) -> createSpillableMapBasedFileSystemView(conf, viewConf, basePath));
      case MEMORY:
        logger.info("Creating in-memory based Table View");
        return new FileSystemViewManager(conf, config,
            (basePath, viewConfig) -> createInMemoryFileSystemView(conf, viewConfig, basePath));
      case REMOTE_ONLY:
        logger.info("Creating remote only table view");
        return new FileSystemViewManager(conf, config,
            (basePath, viewConfig) -> createRemoteFileSystemView(conf, viewConfig,
                new HoodieTableMetaClient(conf.get(), basePath)));
      case REMOTE_FIRST:
        logger.info("Creating remote first table view");
        return new FileSystemViewManager(conf, config, (basePath, viewConfig) -> {
          RemoteHoodieTableFileSystemView remoteFileSystemView =
              createRemoteFileSystemView(conf, viewConfig, new HoodieTableMetaClient(conf.get(), basePath));
          HoodieView secondaryView = null;
          switch (viewConfig.getSecondaryStorageType()) {
            case MEMORY:
              secondaryView = createInMemoryFileSystemView(conf, viewConfig, basePath);
              break;
            case EMBEDDED_KV_STORE:
              secondaryView = createRocksDBBasedFileSystemView(conf, viewConfig, basePath);
              break;
            case SPILLABLE_DISK:
              secondaryView = createSpillableMapBasedFileSystemView(conf, viewConfig, basePath);
              break;
            default:
              throw new IllegalArgumentException("Secondary Storage type can only be in-memory or spillable. Was :"
                  + viewConfig.getSecondaryStorageType());
          }
          return new PriorityBasedFileSystemViewDelegator(remoteFileSystemView, secondaryView);
        });
      default:
        throw new IllegalArgumentException("Unknown file system view type :" + config.getStorageType());
    }
  }
}
