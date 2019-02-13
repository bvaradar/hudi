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

import com.uber.hoodie.common.model.CompactionOperation;
import com.uber.hoodie.common.model.HoodieFileGroup;
import com.uber.hoodie.common.model.HoodieFileGroupId;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.table.HoodieTimeline;
import com.uber.hoodie.common.util.DefaultSizeEstimator;
import com.uber.hoodie.common.util.collection.ExternalSpillableMap;
import com.uber.hoodie.common.util.collection.Pair;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.hadoop.fs.FileStatus;

/**
 * Table FileSystemView implementation where view is stored in spillable disk using fixed memory
 */
public class SpillableMapBasedFileSystemView extends HoodieTableFileSystemView {

  private final long maxMemoryForFileGroupMap;
  private final long maxMemoryForPendingCompaction;
  private final String baseStoreDir;

  public SpillableMapBasedFileSystemView(HoodieTableMetaClient metaClient,
      HoodieTimeline visibleActiveTimeline, FileSystemViewStorageConfig config) {
    this.maxMemoryForFileGroupMap = config.getMaxMemoryForFileGroupMap();
    this.maxMemoryForPendingCompaction = config.getMaxMemoryForPendingCompaction();
    this.baseStoreDir = config.getBaseStoreDir();
    init(metaClient, visibleActiveTimeline);
  }

  public SpillableMapBasedFileSystemView(HoodieTableMetaClient metaClient,
      HoodieTimeline visibleActiveTimeline, FileStatus[] fileStatuses, FileSystemViewStorageConfig config) {
    this(metaClient, visibleActiveTimeline, config);
    addFilesToView(fileStatuses);
  }

  @Override
  protected Map<String, List<HoodieFileGroup>> createPartitionToFileGroups() {
    try {
      log.info("Creating Partition To File groups map using external spillable Map. Max Mem="
          + maxMemoryForFileGroupMap + ", BaseDir=" + baseStoreDir);
      new File(baseStoreDir).mkdirs();
      return (Map<String, List<HoodieFileGroup>>)
          (new ExternalSpillableMap<>(maxMemoryForFileGroupMap, baseStoreDir, new DefaultSizeEstimator(),
              new DefaultSizeEstimator<>()));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  protected Map<HoodieFileGroupId, Pair<String, CompactionOperation>> createFileIdToPendingCompactionMap(
      Map<HoodieFileGroupId, Pair<String, CompactionOperation>> fgIdToPendingCompaction) {
    try {
      log.info("Creating Pending Compaction map using external spillable Map. Max Mem="
          + maxMemoryForPendingCompaction + ", BaseDir=" + baseStoreDir);
      new File(baseStoreDir).mkdirs();
      Map<HoodieFileGroupId, Pair<String, CompactionOperation>> pendingMap =
          new ExternalSpillableMap<>(maxMemoryForPendingCompaction, baseStoreDir, new DefaultSizeEstimator(),
              new DefaultSizeEstimator<>());
      pendingMap.putAll(fgIdToPendingCompaction);
      return pendingMap;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public Stream<HoodieFileGroup> getAllFileGroups() {
    return ((ExternalSpillableMap)partitionToFileGroupsMap).valueStream()
        .flatMap(fg -> ((List<HoodieFileGroup>)fg).stream());
  }
}
