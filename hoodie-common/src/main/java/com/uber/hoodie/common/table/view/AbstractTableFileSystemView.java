/*
 *  Copyright (c) 2018 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.uber.hoodie.common.table.view;

import com.google.common.base.Preconditions;
import com.uber.hoodie.common.model.CompactionOperation;
import com.uber.hoodie.common.model.FileSlice;
import com.uber.hoodie.common.model.HoodieDataFile;
import com.uber.hoodie.common.model.HoodieFileGroup;
import com.uber.hoodie.common.model.HoodieFileGroupId;
import com.uber.hoodie.common.model.HoodieLogFile;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.table.HoodieTimeline;
import com.uber.hoodie.common.table.HoodieView;
import com.uber.hoodie.common.table.timeline.HoodieInstant;
import com.uber.hoodie.common.util.CompactionUtils;
import com.uber.hoodie.common.util.FSUtils;
import com.uber.hoodie.common.util.Option;
import com.uber.hoodie.common.util.collection.Pair;
import com.uber.hoodie.exception.HoodieIOException;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

/**
 * Common thread-safe implementation for multiple TableFileSystemView Implementations.
 * Provides uniform handling of
 *   (a) Loading file-system views from underlying file-system
 *   (b) Pending compaction operations and changing file-system views based on that
 *   (c) Thread-safety in loading and managing file system views for this dataset.
 *   (d) resetting file-system views
 * The actual mechanism of fetching file slices from different view storages is delegated to sub-classes.
 */
public abstract class AbstractTableFileSystemView implements HoodieView, Serializable {

  protected static Logger log = LogManager.getLogger(HoodieTableFileSystemView.class);

  protected HoodieTableMetaClient metaClient;

  // This is the commits that will be visible for all views extending this view
  protected HoodieTimeline visibleActiveTimeline;

  // Used to concurrently load and populate partition views
  private ConcurrentHashMap<String, Boolean> addedPartitions = new ConcurrentHashMap<>();

  // Locks to control concurrency. Sync operations use write-lock blocking all fetch operations.
  // For the common-case, we allow concurrent read of single or multiple partitions
  private final ReentrantReadWriteLock globalLock = new ReentrantReadWriteLock();
  private final ReadLock readLock = globalLock.readLock();
  private final WriteLock writeLock = globalLock.writeLock();

  private String getPartitionPathFromFilePath(String fullPath) {
    return FSUtils.getRelativePartitionPath(new Path(metaClient.getBasePath()), new Path(fullPath).getParent());
  }

  /**
   * Inisitalize the view.
   */
  protected void init(HoodieTableMetaClient metaClient, HoodieTimeline visibleActiveTimeline) {
    this.metaClient = metaClient;
    this.visibleActiveTimeline = visibleActiveTimeline;
    // Load Pending Compaction Operations
    resetPendingCompactionOperations(
        CompactionUtils.getAllPendingCompactionOperations(metaClient).values()
            .stream().map(e -> Pair.of(e.getKey(),
            CompactionOperation.convertFromAvroRecordInstance(e.getValue()))));
  }

  /**
   * Adds the provided statuses into the file system view, and also caches it inside this object.
   */
  protected List<HoodieFileGroup> addFilesToView(FileStatus[] statuses) {
    List<HoodieFileGroup> fileGroups = buildFileGroups(statuses, visibleActiveTimeline, true);
    // Make building FileGroup Map efficient for both InMemory and DiskBased stuctures.
    fileGroups.stream().collect(Collectors.groupingBy(HoodieFileGroup::getPartitionPath)).entrySet()
        .forEach(entry -> {
          String partition = entry.getKey();
          if (!isPartitionAvailableInStore(partition)) {
            storePartitionView(partition, entry.getValue());
          }
        });
    return fileGroups;
  }

  /**
   * Build FileGroups from passed in file-status
   */
  protected List<HoodieFileGroup> buildFileGroups(FileStatus[] statuses, HoodieTimeline timeline,
      boolean addPendingCompactionFileSlice) {
    return buildFileGroups(convertFileStatusesToDataFiles(statuses), convertFileStatusesToLogFiles(statuses), timeline,
        addPendingCompactionFileSlice);
  }

  protected List<HoodieFileGroup> buildFileGroups(Stream<HoodieDataFile> dataFileStream,
      Stream<HoodieLogFile> logFileStream, HoodieTimeline timeline, boolean addPendingCompactionFileSlice) {

    Map<Pair<String, String>, List<HoodieDataFile>> dataFiles = dataFileStream
        .collect(Collectors.groupingBy((dataFile) -> {
          String partitionPathStr = getPartitionPathFromFilePath(dataFile.getPath());
          return Pair.of(partitionPathStr, dataFile.getFileId());
        }));

    Map<Pair<String, String>, List<HoodieLogFile>> logFiles = logFileStream
        .collect(Collectors.groupingBy((logFile) -> {
          String partitionPathStr = FSUtils.getRelativePartitionPath(
              new Path(metaClient.getBasePath()),
              logFile.getPath().getParent());
          return Pair.of(partitionPathStr, logFile.getFileId());
        }));

    Set<Pair<String, String>> fileIdSet = new HashSet<>(dataFiles.keySet());
    fileIdSet.addAll(logFiles.keySet());

    List<HoodieFileGroup> fileGroups = new ArrayList<>();
    fileIdSet.forEach(pair -> {
      String fileId = pair.getValue();
      HoodieFileGroup group = new HoodieFileGroup(pair.getKey(), fileId, timeline);
      if (dataFiles.containsKey(pair)) {
        dataFiles.get(pair).forEach(group::addDataFile);
      }
      if (logFiles.containsKey(pair)) {
        logFiles.get(pair).forEach(group::addLogFile);
      }
      if (addPendingCompactionFileSlice) {
        addPendingCompactionSliceIfNeeded(group);
      }
      fileGroups.add(group);
    });

    return fileGroups;
  }

  /**
   * Add a new file-slice if compaction is pending
   */
  private void addPendingCompactionSliceIfNeeded(HoodieFileGroup group) {
    Option<Pair<String, CompactionOperation>> pendingCompaction =
        getPendingCompactionOperationWithInstant(group.getFileGroupId());
    if (pendingCompaction.isPresent()) {
      // If there is no delta-commit after compaction request, this step would ensure a new file-slice appears
      // so that any new ingestion uses the correct base-instant
      group.addNewFileSliceAtInstant(pendingCompaction.get().getKey());
    }
  }

  /**
   * Clears the partition Map only
   */
  public void reset() {
    reset(visibleActiveTimeline);
  }

  /**
   * Resets the partition-path
   */
  void reset(HoodieTimeline timeline) {
    try {
      writeLock.lock();

      addedPartitions.clear();
      resetViewState();

      // Initialize with new Hoodie timeline.
      init(metaClient, timeline);
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * Allows all view metadata to be reset by subclasses
   */
  protected abstract void resetViewState();

  /**
   * Allows lazily loading the partitions if needed
   *
   * @param partition partition to be loaded if not present
   */
  private void ensurePartitionLoadedCorrectly(String partition) {

    Preconditions.checkArgument(!isClosed(), "View is already closed");

    // ensure we list files only once even in the face of concurrency
    addedPartitions.computeIfAbsent(partition, (partitionPathStr) -> {
      if (!isPartitionAvailableInStore(partitionPathStr)) {
        // Not loaded yet
        try {
          log.info("Building file system view for partition (" + partitionPathStr + ")");

          // Create the path if it does not exist already
          Path partitionPath = new Path(metaClient.getBasePath(), partitionPathStr);
          FSUtils.createPathIfNotExists(metaClient.getFs(), partitionPath);
          FileStatus[] statuses = metaClient.getFs().listStatus(partitionPath);
          log.info("#files found in partition (" + partitionPathStr + ") is :" + statuses.length);
          List<HoodieFileGroup> groups = addFilesToView(statuses);

          if (groups.isEmpty()) {
            storePartitionView(partitionPathStr, new ArrayList<>());
          }
        } catch (IOException e) {
          throw new HoodieIOException("Failed to list data files in partition " + partitionPathStr, e);
        }
      } else {
        log.debug("View already built for Partition :" + partitionPathStr + ", FOUND is ");
      }
      return true;
    });
  }

  /**
   * Helper to convert file-status to data-files
   *
   * @param statuses List of FIle-Status
   */
  private Stream<HoodieDataFile> convertFileStatusesToDataFiles(FileStatus[] statuses) {
    Predicate<FileStatus> roFilePredicate = fileStatus ->
        fileStatus.getPath().getName()
            .contains(metaClient.getTableConfig().getROFileFormat().getFileExtension());
    return Arrays.stream(statuses).filter(roFilePredicate).map(HoodieDataFile::new);
  }

  /**
   * Helper to convert file-status to log-files
   *
   * @param statuses List of FIle-Status
   */
  private Stream<HoodieLogFile> convertFileStatusesToLogFiles(FileStatus[] statuses) {
    Predicate<FileStatus> rtFilePredicate = fileStatus ->
        fileStatus.getPath().getName()
            .contains(metaClient.getTableConfig().getRTFileFormat().getFileExtension());
    return Arrays.stream(statuses).filter(rtFilePredicate).map(HoodieLogFile::new);
  }

  /**
   * With async compaction, it is possible to see partial/complete data-files due to inflight-compactions, Ignore those
   * data-files
   *
   * @param dataFile Data File
   */
  protected boolean isDataFileDueToPendingCompaction(HoodieDataFile dataFile) {
    final String partitionPath = getPartitionPathFromFilePath(dataFile.getPath());

    Option<Pair<String, CompactionOperation>> compactionWithInstantTime =
        getPendingCompactionOperationWithInstant(new HoodieFileGroupId(partitionPath, dataFile.getFileId()));
    return (compactionWithInstantTime.isPresent()) && (null != compactionWithInstantTime.get().getKey())
        && dataFile.getCommitTime().equals(compactionWithInstantTime.get().getKey());
  }

  /**
   * Returns true if the file-group is under pending-compaction and the file-slice' baseInstant matches compaction
   * Instant
   *
   * @param fileSlice File Slice
   */
  protected boolean isFileSliceAfterPendingCompaction(FileSlice fileSlice) {
    Option<Pair<String, CompactionOperation>> compactionWithInstantTime =
        getPendingCompactionOperationWithInstant(fileSlice.getFileGroupId());
    return (compactionWithInstantTime.isPresent())
        && fileSlice.getBaseInstantTime().equals(compactionWithInstantTime.get().getKey());
  }

  /**
   * With async compaction, it is possible to see partial/complete data-files due to inflight-compactions, Ignore those
   * data-files
   *
   * @param fileSlice File Slice
   */
  protected FileSlice filterDataFileAfterPendingCompaction(FileSlice fileSlice) {
    if (isFileSliceAfterPendingCompaction(fileSlice)) {
      // Data file is filtered out of the file-slice as the corresponding compaction
      // instant not completed yet.
      FileSlice transformed = new FileSlice(fileSlice.getPartitionPath(),
          fileSlice.getBaseInstantTime(), fileSlice.getFileId());
      fileSlice.getLogFiles().forEach(transformed::addLogFile);
      return transformed;
    }
    return fileSlice;
  }

  @Override
  public final Stream<Pair<String, CompactionOperation>> getPendingCompactionOperations() {
    try {
      readLock.lock();
      return fetchPendingCompactionOperations();
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public final Stream<HoodieDataFile> getLatestDataFiles(String partitionStr) {
    try {
      readLock.lock();
      String partitionPath = formatPartitionKey(partitionStr);
      ensurePartitionLoadedCorrectly(partitionPath);
      return fetchLatestDataFiles(partitionPath);
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public final Stream<HoodieDataFile> getLatestDataFiles() {
    try {
      readLock.lock();
      return fetchLatestDataFiles();
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public final Stream<HoodieDataFile> getLatestDataFilesBeforeOrOn(String partitionStr, String maxCommitTime) {
    try {
      readLock.lock();
      String partitionPath = formatPartitionKey(partitionStr);
      ensurePartitionLoadedCorrectly(partitionPath);
      return fetchAllStoredFileGroups(partitionPath)
          .map(fileGroup -> fileGroup.getAllDataFiles()
              .filter(dataFile ->
                  HoodieTimeline.compareTimestamps(dataFile.getCommitTime(),
                      maxCommitTime,
                      HoodieTimeline.LESSER_OR_EQUAL))
              .filter(df -> !isDataFileDueToPendingCompaction(df))
              .findFirst())
          .filter(Optional::isPresent)
          .map(Optional::get);
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public final Option<HoodieDataFile> getLatestDataFileOn(String partitionStr, String instantTime, String fileId) {
    try {
      readLock.lock();
      String partitionPath = formatPartitionKey(partitionStr);
      HoodieFileGroupId fgId = new HoodieFileGroupId(partitionPath, fileId);
      ensurePartitionLoadedCorrectly(partitionPath);
      return Option.fromJavaOptional(fetchAllStoredFileGroups(partitionPath)
          .filter(fileGroup -> fileGroup.getFileGroupId().equals(fgId))
          .map(fileGroup -> fileGroup.getAllDataFiles()
              .filter(dataFile ->
                  HoodieTimeline.compareTimestamps(dataFile.getCommitTime(),
                      instantTime,
                      HoodieTimeline.EQUAL))
              .filter(df -> !isDataFileDueToPendingCompaction(df))
              .findFirst())
          .filter(Optional::isPresent)
          .map(Optional::get).findFirst());
    } finally {
      readLock.unlock();
    }
  }

  /**
   * Get Latest data file for a partition and file-Id
   */
  public final Option<HoodieDataFile> getLatestDataFile(String partitionStr, String fileId) {
    try {
      readLock.lock();
      String partitionPath = formatPartitionKey(partitionStr);
      ensurePartitionLoadedCorrectly(partitionPath);
      return fetchLatestDataFile(partitionPath, fileId);
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public final Stream<HoodieDataFile> getLatestDataFilesInRange(List<String> commitsToReturn) {
    try {
      readLock.lock();
      return fetchAllStoredFileGroups().map(fileGroup -> {
        return fileGroup.getAllDataFiles()
            .filter(dataFile -> commitsToReturn.contains(dataFile.getCommitTime())
                && !isDataFileDueToPendingCompaction(dataFile))
            .findFirst();
      }).filter(Optional::isPresent).map(Optional::get);
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public final Stream<HoodieDataFile> getAllDataFiles(String partitionStr) {
    try {
      readLock.lock();
      String partitionPath = formatPartitionKey(partitionStr);
      ensurePartitionLoadedCorrectly(partitionPath);
      return fetchAllDataFiles(partitionPath)
          .filter(df -> visibleActiveTimeline.containsOrBeforeTimelineStarts(df.getCommitTime()))
          .filter(df -> !isDataFileDueToPendingCompaction(df));
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public final Stream<FileSlice> getLatestFileSlices(String partitionStr) {
    try {
      readLock.lock();
      String partitionPath = formatPartitionKey(partitionStr);
      ensurePartitionLoadedCorrectly(partitionPath);
      return fetchLatestFileSlices(partitionPath).map(fs -> filterDataFileAfterPendingCompaction(fs));
    } finally {
      readLock.unlock();
    }
  }

  /**
   * Get Latest File Slice for a given fileId in a given partition
   */
  public final Option<FileSlice> getLatestFileSlice(String partitionStr, String fileId) {
    try {
      readLock.lock();
      String partitionPath = formatPartitionKey(partitionStr);
      ensurePartitionLoadedCorrectly(partitionPath);
      return fetchLatestFileSlice(partitionPath, fileId);
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public final Stream<FileSlice> getLatestUnCompactedFileSlices(String partitionStr) {
    try {
      readLock.lock();
      String partitionPath = formatPartitionKey(partitionStr);
      ensurePartitionLoadedCorrectly(partitionPath);
      return fetchAllStoredFileGroups(partitionPath)
          .map(fileGroup -> {
            FileSlice fileSlice = fileGroup.getLatestFileSlice().get();
            // if the file-group is under compaction, pick the latest before compaction instant time.
            Option<Pair<String, CompactionOperation>> compactionWithInstantPair =
                getPendingCompactionOperationWithInstant(fileSlice.getFileGroupId());
            if (compactionWithInstantPair.isPresent()) {
              String compactionInstantTime = compactionWithInstantPair.get().getLeft();
              return fileGroup.getLatestFileSliceBefore(compactionInstantTime);
            }
            return Optional.of(fileSlice);
          })
          .map(Optional::get);
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public final Stream<FileSlice> getLatestFileSlicesBeforeOrOn(String partitionStr, String maxCommitTime) {
    try {
      readLock.lock();
      String partitionPath = formatPartitionKey(partitionStr);
      ensurePartitionLoadedCorrectly(partitionPath);
      return fetchLatestFileSlicesBeforeOrOn(partitionPath, maxCommitTime)
          .map(fs -> filterDataFileAfterPendingCompaction(fs));
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public final Stream<FileSlice> getLatestMergedFileSlicesBeforeOrOn(String partitionStr, String maxInstantTime) {
    try {
      readLock.lock();
      String partition = formatPartitionKey(partitionStr);
      ensurePartitionLoadedCorrectly(partition);
      return fetchAllStoredFileGroups(partition)
          .map(fileGroup -> {
            Optional<FileSlice> fileSlice = fileGroup.getLatestFileSliceBeforeOrOn(maxInstantTime);
            // if the file-group is under construction, pick the latest before compaction instant time.
            if (fileSlice.isPresent()) {
              fileSlice = Optional.of(fetchMergedFileSlice(fileGroup, fileSlice.get()));
            }
            return fileSlice;
          })
          .filter(Optional::isPresent)
          .map(Optional::get);
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public final Stream<FileSlice> getLatestFileSliceInRange(List<String> commitsToReturn) {
    try {
      readLock.lock();
      return fetchLatestFileSliceInRange(commitsToReturn);
    } finally {
      readLock.unlock();
    }
  }

  @Override
  public final Stream<FileSlice> getAllFileSlices(String partitionStr) {
    try {
      readLock.lock();
      String partition = formatPartitionKey(partitionStr);
      ensurePartitionLoadedCorrectly(partition);
      return fetchAllFileSlices(partition);
    } finally {
      readLock.unlock();
    }
  }

  /**
   * Ensure there is consistency in handling trailing slash in partition-path. Always trim it which is what is done in
   * other places.
   */
  private String formatPartitionKey(String partitionStr) {
    return partitionStr.endsWith("/") ? partitionStr.substring(0, partitionStr.length() - 1) : partitionStr;
  }

  @Override
  public final Stream<HoodieFileGroup> getAllFileGroups(String partitionStr) {
    try {
      readLock.lock();
      // Ensure there is consistency in handling trailing slash in partition-path. Always trim it which is what is done
      // in other places.
      String partition = formatPartitionKey(partitionStr);
      ensurePartitionLoadedCorrectly(partition);
      return fetchAllStoredFileGroups(partition);
    } finally {
      readLock.unlock();
    }
  }

  // Fetch APIs to be implemented by concrete sub-classes

  /**
   * Check if there is an outstanding compaction scheduled for this file
   *
   * @param fgId File-Group Id
   * @return true if there is a pending compaction, false otherwise
   */
  protected abstract boolean isPendingCompactionScheduledForFileId(HoodieFileGroupId fgId);

  /**
   * resets the pending compaction operation and overwrite with the new list
   *
   * @param operations Pending Compaction Operations
   */
  abstract void resetPendingCompactionOperations(Stream<Pair<String, CompactionOperation>> operations);

  /**
   * Add pending compaction operations to store
   *
   * @param operations Pending compaction operations to be added
   */
  abstract void addPendingCompactionOperations(Stream<Pair<String, CompactionOperation>> operations);

  /**
   * Remove pending compaction operations from store
   *
   * @param operations Pending compaction operations to be removed
   */
  abstract void removePendingCompactionOperations(Stream<Pair<String, CompactionOperation>> operations);

  /**
   * Return pending compaction operation for a file-group
   *
   * @param fileGroupId File-Group Id
   */
  protected abstract Option<Pair<String, CompactionOperation>> getPendingCompactionOperationWithInstant(
      HoodieFileGroupId fileGroupId);

  /**
   * Fetch all pending compaction operations
   */
  abstract Stream<Pair<String, CompactionOperation>> fetchPendingCompactionOperations();

  /**
   * Checks if partition is pre-loaded and available in store
   *
   * @param partitionPath Partition Path
   */
  abstract boolean isPartitionAvailableInStore(String partitionPath);

  /**
   * Add a complete partition view to store
   *
   * @param partitionPath Partition Path
   * @param fileGroups File Groups for the partition path
   */
  abstract void storePartitionView(String partitionPath, List<HoodieFileGroup> fileGroups);

  /**
   * Fetch all file-groups stored for a partition-path
   *
   * @param partitionPath Partition path for which the file-groups needs to be retrieved.
   * @return file-group stream
   */
  abstract Stream<HoodieFileGroup> fetchAllStoredFileGroups(String partitionPath);

  /**
   * Fetch all Stored file-groups across all partitions loaded
   *
   * @return file-group stream
   */
  abstract Stream<HoodieFileGroup> fetchAllStoredFileGroups();

  /**
   * Check if the view is already closed
   */
  abstract boolean isClosed();

  /**
   * Default implementation for fetching latest file-slice in commit range
   *
   * @param commitsToReturn Commits
   */
  Stream<FileSlice> fetchLatestFileSliceInRange(List<String> commitsToReturn) {
    return fetchAllStoredFileGroups().map(fileGroup -> fileGroup.getLatestFileSliceInRange(commitsToReturn))
        .map(Optional::get);
  }

  /**
   * Default implementation for fetching all file-slices for a partition-path
   *
   * @param partitionPath Partition path
   * @return file-slice stream
   */
  Stream<FileSlice> fetchAllFileSlices(String partitionPath) {
    return fetchAllStoredFileGroups(partitionPath)
        .map(HoodieFileGroup::getAllFileSlices)
        .flatMap(sliceList -> sliceList);
  }

  /**
   * Default implementation for fetching latest data-files for the partition-path
   */
  Stream<HoodieDataFile> fetchLatestDataFiles(final String partitionPath) {
    return fetchAllStoredFileGroups(partitionPath)
        .map(this::getLatestDataFile)
        .filter(Optional::isPresent)
        .map(Optional::get);
  }


  protected Optional<HoodieDataFile> getLatestDataFile(HoodieFileGroup fileGroup) {
    return fileGroup.getAllDataFiles().filter(df -> !isDataFileDueToPendingCompaction(df)).findFirst();
  }

  /**
   * Default implementation for fetching latest data-files across all partitions
   */
  Stream<HoodieDataFile> fetchLatestDataFiles() {
    return fetchAllStoredFileGroups()
        .map(this::getLatestDataFile)
        .filter(Optional::isPresent)
        .map(Optional::get);
  }

  /**
   * Default implementation for fetching all data-files for a partition
   *
   * @param partitionPath partition-path
   */
  Stream<HoodieDataFile> fetchAllDataFiles(String partitionPath) {
    return fetchAllStoredFileGroups(partitionPath)
        .map(HoodieFileGroup::getAllDataFiles)
        .flatMap(dataFileList -> dataFileList);
  }

  /**
   * Default implementation for fetching latest file-slices for a partition path
   */
  Stream<FileSlice> fetchLatestFileSlices(String partitionPath) {
    return fetchAllStoredFileGroups(partitionPath)
        .map(HoodieFileGroup::getLatestFileSlice)
        .filter(Optional::isPresent)
        .map(Optional::get);
  }

  /**
   * Default implementation for fetching latest file-slices for a partition path as of instant
   *
   * @param partitionPath Partition Path
   * @param maxCommitTime Instant Time
   */
  Stream<FileSlice> fetchLatestFileSlicesBeforeOrOn(String partitionPath,
      String maxCommitTime) {
    return fetchAllStoredFileGroups(partitionPath)
        .map(fileGroup -> fileGroup.getLatestFileSliceBeforeOrOn(maxCommitTime))
        .filter(Optional::isPresent)
        .map(Optional::get);
  }

  /**
   * Helper to merge last 2 file-slices. These 2 file-slices do not have compaction done yet.
   *
   * @param lastSlice Latest File slice for a file-group
   * @param penultimateSlice Penultimate file slice for a file-group in commit timeline order
   */
  private static FileSlice mergeCompactionPendingFileSlices(FileSlice lastSlice, FileSlice penultimateSlice) {
    FileSlice merged = new FileSlice(penultimateSlice.getPartitionPath(),
        penultimateSlice.getBaseInstantTime(), penultimateSlice.getFileId());
    if (penultimateSlice.getDataFile().isPresent()) {
      merged.setDataFile(penultimateSlice.getDataFile().get());
    }
    // Add Log files from penultimate and last slices
    penultimateSlice.getLogFiles().forEach(merged::addLogFile);
    lastSlice.getLogFiles().forEach(merged::addLogFile);
    return merged;
  }

  /**
   * If the file-slice is because of pending compaction instant, this method merges the file-slice with the one before
   * the compaction instant time
   *
   * @param fileGroup File Group for which the file slice belongs to
   * @param fileSlice File Slice which needs to be merged
   */
  private FileSlice fetchMergedFileSlice(HoodieFileGroup fileGroup, FileSlice fileSlice) {
    // if the file-group is under construction, pick the latest before compaction instant time.
    Option<Pair<String, CompactionOperation>> compactionOpWithInstant =
        getPendingCompactionOperationWithInstant(fileGroup.getFileGroupId());
    if (compactionOpWithInstant.isPresent()) {
      String compactionInstantTime = compactionOpWithInstant.get().getKey();
      if (fileSlice.getBaseInstantTime().equals(compactionInstantTime)) {
        Optional<FileSlice> prevFileSlice = fileGroup.getLatestFileSliceBefore(compactionInstantTime);
        if (prevFileSlice.isPresent()) {
          return mergeCompactionPendingFileSlices(fileSlice, prevFileSlice.get());
        }
      }
    }
    return fileSlice;
  }

  /**
   * Default implementation for fetching latest data-file
   * @param partitionPath Partition path
   * @param fileId File Id
   * @return Data File if present
   */
  protected Option<HoodieDataFile> fetchLatestDataFile(String partitionPath, String fileId) {
    return Option.fromJavaOptional(fetchLatestDataFiles(partitionPath)
        .filter(fs -> fs.getFileId().equals(fileId)).findFirst());
  }

  /**
   * Default implementation for fetching file-slice
   * @param partitionPath Partition path
   * @param fileId File Id
   * @return File Slice if present
   */
  protected Option<FileSlice> fetchLatestFileSlice(String partitionPath, String fileId) {
    return Option.fromJavaOptional(fetchLatestFileSlices(partitionPath)
        .filter(fs -> fs.getFileId().equals(fileId)).findFirst());
  }

  @Override
  public Option<HoodieInstant> getLastInstant() {
    return Option.fromJavaOptional(visibleActiveTimeline.lastInstant());
  }

  @Override
  public HoodieTimeline getTimeline() {
    return visibleActiveTimeline;
  }

  @Override
  public void sync() {
    HoodieTimeline oldTimeline = getTimeline();
    HoodieTimeline newTimeline = metaClient.reloadActiveTimeline().filterCompletedAndCompactionInstants();
    try {
      writeLock.lock();
      runSync(oldTimeline, newTimeline);
    } finally {
      writeLock.unlock();
    }
  }

  /**
   * Performs complete reset of file-system view. Subsequent partition view calls will load file slices against latest
   * timeline
   *
   * @param oldTimeline Old Hoodie Timeline
   * @param newTimeline New Hoodie Timeline
   */
  protected void runSync(HoodieTimeline oldTimeline, HoodieTimeline newTimeline) {
    visibleActiveTimeline = newTimeline;
    reset(newTimeline);
  }
}
