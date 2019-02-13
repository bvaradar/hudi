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

import com.google.common.base.Preconditions;
import com.uber.hoodie.config.DefaultHoodieConfig;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

/**
 * File System View Storage Configurations
 */
public class FileSystemViewStorageConfig extends DefaultHoodieConfig {

  //Property Names
  public static final String FILESYSTEM_VIEW_STORAGE_TYPE = "hoodie.view.type";
  public static final String FILESYSTEM_SECONDARY_VIEW_STORAGE_TYPE = "hoodie.view.secondary.type";
  public static final String FILESYSTEM_VIEW_REMOTE_HOST = "hoodie.view.remote.host";
  public static final String FILESYSTEM_VIEW_REMOTE_PORT = "hoodie.view.remote.port";
  public static final String FILESYSTEM_VIEW_SPILLABLE_DIR = "hoodie.view.spillable.dir";
  public static final String FILESYSTEM_VIEW_SPILLABLE_MEM = "hoodie.view.spillable.mem";
  public static final String FILESYSTEM_VIEW_PENDING_COMPACTION_MEM_FRACTION =
      "hoodie.view.spillable.compaction.mem.fraction";
  private static final String ROCKSDB_BASE_PATH_PROP = "hoodie.view.rocksdb.base.path";
  private static final String ROCKSDB_RESET_ON_INSTANTIATION = "hoodie.view.rocksdb.reset_on_start";

  public static final FileSystemViewStorageType DEFAULT_VIEW_STORAGE_TYPE = FileSystemViewStorageType.MEMORY;
  public static final FileSystemViewStorageType DEFAULT_SECONDARY_VIEW_STORAGE_TYPE = FileSystemViewStorageType.MEMORY;
  public static final String DEFAULT_ROCKSDB_BASE_PATH_PROP = "/tmp/hoodie_timeline_rocksdb";
  public static final String DEFAULT_ROCKSDB_RESET_ON_INSTANTIATION = "false";

  public static final String DEFUALT_REMOTE_VIEW_SERVER_HOST = "localhost";
  public static final Integer DEFAULT_REMOTE_VIEW_SERVER_PORT = 26754;

  public static final String DEFAULT_VIEW_SPILLABLE_DIR = "/tmp/view_map/";
  private static final Double DEFAULT_MEM_FRACTION_FOR_PENDING_COMPACTION = 0.01;
  private static final Long DEFAULT_MAX_MEMORY_FOR_VIEW = 100 * 1024 * 1024L; // 100 MB

  public static FileSystemViewStorageConfig.Builder newBuilder() {
    return new Builder();
  }

  private FileSystemViewStorageConfig(Properties props) {
    super(props);
  }

  public FileSystemViewStorageType getStorageType() {
    return FileSystemViewStorageType.valueOf(props.getProperty(FILESYSTEM_VIEW_STORAGE_TYPE));
  }

  public String getRemoteViewServerHost() {
    return props.getProperty(FILESYSTEM_VIEW_REMOTE_HOST);
  }

  public Integer getRemoteViewServerPort() {
    return Integer.parseInt(props.getProperty(FILESYSTEM_VIEW_REMOTE_PORT));
  }

  public long getMaxMemoryForFileGroupMap() {
    long totalMemory = Long.parseLong(props.getProperty(FILESYSTEM_VIEW_SPILLABLE_MEM));
    return totalMemory - getMaxMemoryForPendingCompaction();
  }

  public long getMaxMemoryForPendingCompaction() {
    long totalMemory = Long.parseLong(props.getProperty(FILESYSTEM_VIEW_SPILLABLE_MEM));
    long reservedForPendingComaction = new Double(totalMemory * Double.parseDouble(
        props.getProperty(FILESYSTEM_VIEW_PENDING_COMPACTION_MEM_FRACTION))).longValue();
    return reservedForPendingComaction;
  }

  public String getBaseStoreDir() {
    return props.getProperty(FILESYSTEM_VIEW_SPILLABLE_DIR);
  }

  public FileSystemViewStorageType getSecondaryStorageType() {
    return FileSystemViewStorageType.valueOf(props.getProperty(FILESYSTEM_SECONDARY_VIEW_STORAGE_TYPE));
  }

  public String getRocksdbBasePath() {
    return props.getProperty(ROCKSDB_BASE_PATH_PROP);
  }

  public boolean isResetOnInstantiation() {
    return Boolean.valueOf(props.getProperty(ROCKSDB_RESET_ON_INSTANTIATION));
  }

  public static class Builder {

    private final Properties props = new Properties();

    public Builder fromFile(File propertiesFile) throws IOException {
      FileReader reader = new FileReader(propertiesFile);
      try {
        props.load(reader);
        return this;
      } finally {
        reader.close();
      }
    }

    public Builder fromProperties(Properties props) {
      this.props.putAll(props);
      return this;
    }

    public Builder withStorageType(FileSystemViewStorageType storageType) {
      props.setProperty(FILESYSTEM_VIEW_STORAGE_TYPE, storageType.name());
      return this;
    }

    public Builder withSecondaryStorageType(FileSystemViewStorageType storageType) {
      props.setProperty(FILESYSTEM_SECONDARY_VIEW_STORAGE_TYPE, storageType.name());
      return this;
    }

    public Builder withRemoteServerHost(String remoteServerHost) {
      props.setProperty(FILESYSTEM_VIEW_REMOTE_HOST, remoteServerHost);
      return this;
    }

    public Builder withRemoteServerPort(Integer remoteServerPort) {
      props.setProperty(FILESYSTEM_VIEW_REMOTE_PORT, remoteServerPort.toString());
      return this;
    }

    public Builder withMaxMemoryForView(Long maxMemoryForView) {
      props.setProperty(FILESYSTEM_VIEW_SPILLABLE_MEM, maxMemoryForView.toString());
      return this;
    }

    public Builder withMemFractionForPendingCompaction(Double memFractionForPendingCompaction) {
      props.setProperty(FILESYSTEM_VIEW_PENDING_COMPACTION_MEM_FRACTION, memFractionForPendingCompaction.toString());
      return this;
    }

    public Builder withBaseStoreDir(String baseStorePath) {
      props.setProperty(FILESYSTEM_VIEW_SPILLABLE_DIR, baseStorePath);
      return this;
    }

    public Builder withRocksDBPath(String basePath) {
      props.setProperty(ROCKSDB_BASE_PATH_PROP, basePath);
      return this;
    }

    public Builder withResetOnInstantiation(boolean resetOnInstantiation) {
      props.setProperty(ROCKSDB_RESET_ON_INSTANTIATION, String.valueOf(resetOnInstantiation));
      return this;
    }

    public FileSystemViewStorageConfig build() {
      setDefaultOnCondition(props, !props.containsKey(FILESYSTEM_VIEW_STORAGE_TYPE),
          FILESYSTEM_VIEW_STORAGE_TYPE, DEFAULT_VIEW_STORAGE_TYPE.name());
      setDefaultOnCondition(props, !props.containsKey(FILESYSTEM_SECONDARY_VIEW_STORAGE_TYPE),
          FILESYSTEM_SECONDARY_VIEW_STORAGE_TYPE, DEFAULT_SECONDARY_VIEW_STORAGE_TYPE.name());
      setDefaultOnCondition(props, !props.containsKey(FILESYSTEM_VIEW_REMOTE_HOST),
          FILESYSTEM_VIEW_REMOTE_HOST, DEFUALT_REMOTE_VIEW_SERVER_HOST);
      setDefaultOnCondition(props, !props.containsKey(FILESYSTEM_VIEW_REMOTE_PORT),
          FILESYSTEM_VIEW_REMOTE_PORT, DEFAULT_REMOTE_VIEW_SERVER_PORT.toString());

      setDefaultOnCondition(props, !props.containsKey(FILESYSTEM_VIEW_SPILLABLE_DIR),
          FILESYSTEM_VIEW_SPILLABLE_DIR, DEFAULT_VIEW_SPILLABLE_DIR);
      setDefaultOnCondition(props, !props.containsKey(FILESYSTEM_VIEW_SPILLABLE_MEM),
          FILESYSTEM_VIEW_SPILLABLE_MEM, DEFAULT_MAX_MEMORY_FOR_VIEW.toString());
      setDefaultOnCondition(props, !props.containsKey(FILESYSTEM_VIEW_PENDING_COMPACTION_MEM_FRACTION),
          FILESYSTEM_VIEW_PENDING_COMPACTION_MEM_FRACTION, DEFAULT_MEM_FRACTION_FOR_PENDING_COMPACTION.toString());

      setDefaultOnCondition(props, !props.containsKey(ROCKSDB_BASE_PATH_PROP),
          ROCKSDB_BASE_PATH_PROP, DEFAULT_ROCKSDB_BASE_PATH_PROP);
      setDefaultOnCondition(props, !props.containsKey(ROCKSDB_RESET_ON_INSTANTIATION),
          ROCKSDB_RESET_ON_INSTANTIATION, DEFAULT_ROCKSDB_RESET_ON_INSTANTIATION);

      // Validations
      FileSystemViewStorageType.valueOf(props.getProperty(FILESYSTEM_VIEW_STORAGE_TYPE));
      FileSystemViewStorageType.valueOf(props.getProperty(FILESYSTEM_SECONDARY_VIEW_STORAGE_TYPE));
      Preconditions.checkArgument(Integer.parseInt(props.getProperty(FILESYSTEM_VIEW_REMOTE_PORT)) > 0);
      return new FileSystemViewStorageConfig(props);
    }
  }

}
