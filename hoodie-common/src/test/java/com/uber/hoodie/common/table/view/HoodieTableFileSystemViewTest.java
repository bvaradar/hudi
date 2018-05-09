/*
 *  Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.uber.hoodie.common.model.FileSlice;
import com.uber.hoodie.common.model.HoodieDataFile;
import com.uber.hoodie.common.model.HoodieFileGroup;
import com.uber.hoodie.common.model.HoodieLogFile;
import com.uber.hoodie.common.model.HoodieTestUtils;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.table.HoodieTimeline;
import com.uber.hoodie.common.table.TableFileSystemView;
import com.uber.hoodie.common.table.timeline.HoodieActiveTimeline;
import com.uber.hoodie.common.table.timeline.HoodieInstant;
import com.uber.hoodie.common.table.timeline.HoodieInstant.State;
import com.uber.hoodie.common.util.FSUtils;
import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

@SuppressWarnings("ResultOfMethodCallIgnored")
public class HoodieTableFileSystemViewTest {

  private HoodieTableMetaClient metaClient;
  private String basePath;
  private TableFileSystemView fsView;
  private TableFileSystemView.ReadOptimizedView roView;
  private TableFileSystemView.RealtimeView rtView;

  @Before
  public void init() throws IOException {
    metaClient = HoodieTestUtils.initOnTemp();
    basePath = metaClient.getBasePath();
    fsView = new HoodieTableFileSystemView(metaClient,
        metaClient.getActiveTimeline().getCommitTimeline().filterCompletedInstants());
    roView = (TableFileSystemView.ReadOptimizedView) fsView;
    rtView = (TableFileSystemView.RealtimeView) fsView;
  }

  private void refreshFsView(FileStatus[] statuses) {
    metaClient = new HoodieTableMetaClient(metaClient.getHadoopConf(), basePath, true);
    if (statuses != null) {
      fsView = new HoodieTableFileSystemView(metaClient,
          metaClient.getActiveTimeline().getCommitTimeline().filterCompletedInstants(), statuses);
    } else {
      fsView = new HoodieTableFileSystemView(metaClient,
          metaClient.getActiveTimeline().getCommitTimeline().filterCompletedInstants());
    }
    roView = (TableFileSystemView.ReadOptimizedView) fsView;
    rtView = (TableFileSystemView.RealtimeView) fsView;
  }

  /**
   * Test case for view generation on a file group where
   * the only file-slice does not have data-file. This is the case where upserts directly go to log-files
   */
  @Test
  public void testViewForFileSlicesWithNoBaseFile() throws Exception {
    String partitionPath = "2016/05/01";
    new File(basePath + "/" + partitionPath).mkdirs();
    String fileId = UUID.randomUUID().toString();

    // Create 2 log files but no data file for the latest file-slice for the file-group
    String instantTime1 = "1";
    String deltaInstantTime1 = "2";
    String deltaInstantTime2 = "3";
    String fileName1 = FSUtils.makeLogFileName(fileId, HoodieLogFile.DELTA_EXTENSION, instantTime1, 0);
    String fileName2 = FSUtils.makeLogFileName(fileId, HoodieLogFile.DELTA_EXTENSION, instantTime1, 1);
    new File(basePath + "/" + partitionPath + "/" + fileName1).createNewFile();
    new File(basePath + "/" + partitionPath + "/" + fileName2).createNewFile();
    HoodieActiveTimeline commitTimeline = metaClient.getActiveTimeline();
    HoodieInstant instant1 = new HoodieInstant(true, HoodieTimeline.COMMIT_ACTION, instantTime1);
    HoodieInstant deltaInstant2 = new HoodieInstant(true, HoodieTimeline.DELTA_COMMIT_ACTION, deltaInstantTime1);
    HoodieInstant deltaInstant3 = new HoodieInstant(true, HoodieTimeline.DELTA_COMMIT_ACTION, deltaInstantTime2);

    commitTimeline.saveAsComplete(instant1, Optional.empty());
    commitTimeline.saveAsComplete(deltaInstant2, Optional.empty());
    commitTimeline.saveAsComplete(deltaInstant3, Optional.empty());

    refreshFsView(null);

    List<HoodieDataFile> dataFiles = roView.getLatestDataFiles().collect(Collectors.toList());
    assertTrue("No data file expected", dataFiles.isEmpty());
    List<FileSlice> fileSliceList = rtView.getLatestFileSlices(partitionPath).collect(Collectors.toList());
    assertEquals(1, fileSliceList.size());
    FileSlice fileSlice = fileSliceList.get(0);
    assertEquals("File-Id must be set correctly", fileId, fileSlice.getFileId());
    assertFalse("Data file for base instant must be present", fileSlice.getDataFile().isPresent());
    assertEquals("Base Instant for file-group set correctly", instantTime1, fileSlice.getBaseCommitTime());
    assertEquals("Base Instant must be the one used for log-append",
        instantTime1, fileSlice.getBaseInstantForLogAppend());
    assertFalse("No outstanding compaction", fileSlice.getOutstandingCompactionInstant().isPresent());
    List<HoodieLogFile> logFiles = fileSlice.getLogFiles().collect(Collectors.toList());
    assertEquals("Correct number of log-files shows up in file-slice", 2, logFiles.size());
    assertEquals("Log File Order check", fileName2, logFiles.get(0).getFileName());
    assertEquals("Log File Order check", fileName1, logFiles.get(1).getFileName());
  }

  @Test
  public void testViewForFileSlicesWithNoBaseFileAndRequestedCompaction() throws Exception {
    testViewForFileSlicesWithAsyncCompaction(true, false);
  }

  @Test
  public void testViewForFileSlicesWithBaseFileAndRequestedCompaction() throws Exception {
    testViewForFileSlicesWithAsyncCompaction(false, false);
  }

  @Test
  public void testViewForFileSlicesWithNoBaseFileAndInflightCompaction() throws Exception {
    testViewForFileSlicesWithAsyncCompaction(true, true);
  }

  @Test
  public void testViewForFileSlicesWithBaseFileAndInflightCompaction() throws Exception {
    testViewForFileSlicesWithAsyncCompaction(false, true);
  }

  /**
   * Helper method to test Views in the presence of concurrent compaction
   * @param skipCreatingDataFile if set, first File Slice will not have data-file set. This would
   *                             simulate inserts going directly to log files
   * @param isCompactionInFlight if set, compaction was inflight (running) when view was tested first time,
   *                             otherwise compaction was in requested state
   * @throws Exception
   */
  private void testViewForFileSlicesWithAsyncCompaction(boolean skipCreatingDataFile,
      boolean isCompactionInFlight) throws Exception {
    String partitionPath = "2016/05/01";
    new File(basePath + "/" + partitionPath).mkdirs();
    String fileId = UUID.randomUUID().toString();

    // if skipCreatingDataFile, then instantTime1 below acts like delta-commit, otherwise it is base-commit
    String instantTime1 = "1";
    String deltaInstantTime1 = "2";
    String deltaInstantTime2 = "3";

    String dataFileName = null;
    if (!skipCreatingDataFile) {
      dataFileName = FSUtils.makeDataFileName(instantTime1, 1, fileId);
      new File(basePath + "/" + partitionPath + "/" + dataFileName).createNewFile();
    }
    String fileName1 = FSUtils.makeLogFileName(fileId, HoodieLogFile.DELTA_EXTENSION, instantTime1, 0);
    String fileName2 = FSUtils.makeLogFileName(fileId, HoodieLogFile.DELTA_EXTENSION, instantTime1, 1);
    new File(basePath + "/" + partitionPath + "/" + fileName1).createNewFile();
    new File(basePath + "/" + partitionPath + "/" + fileName2).createNewFile();
    HoodieActiveTimeline commitTimeline = metaClient.getActiveTimeline();
    HoodieInstant instant1 = new HoodieInstant(true, HoodieTimeline.COMMIT_ACTION, instantTime1);
    HoodieInstant deltaInstant2 = new HoodieInstant(true, HoodieTimeline.DELTA_COMMIT_ACTION, deltaInstantTime1);
    HoodieInstant deltaInstant3 = new HoodieInstant(true, HoodieTimeline.DELTA_COMMIT_ACTION, deltaInstantTime2);

    commitTimeline.saveAsComplete(instant1, Optional.empty());
    commitTimeline.saveAsComplete(deltaInstant2, Optional.empty());
    commitTimeline.saveAsComplete(deltaInstant3, Optional.empty());

    // Fake delta-ingestion after compaction-requested
    String compactionRequestedTime = "4";
    String compactDataFileName = FSUtils.makeDataFileName(compactionRequestedTime, 1, fileId);
    HoodieInstant compactionInstant = null;
    if (isCompactionInFlight) {
      // Create a Data-file but this should be skipped by view
      new File(basePath + "/" + partitionPath + "/" + compactDataFileName).createNewFile();
      compactionInstant = new HoodieInstant(State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION, compactionRequestedTime);
      commitTimeline.saveToInflight(compactionInstant, Optional.empty());
    } else {
      compactionInstant = new HoodieInstant(State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, compactionRequestedTime);
      commitTimeline.saveToRequested(compactionInstant, Optional.empty());
    }
    String deltaInstantTime4 = "5";
    String deltaInstantTime5 = "6";
    String fileName3 = FSUtils.makeLogFileName(fileId, HoodieLogFile.DELTA_EXTENSION, compactionRequestedTime, 0);
    String fileName4 = FSUtils.makeLogFileName(fileId, HoodieLogFile.DELTA_EXTENSION, compactionRequestedTime, 1);
    new File(basePath + "/" + partitionPath + "/" + fileName3).createNewFile();
    new File(basePath + "/" + partitionPath + "/" + fileName4).createNewFile();
    HoodieInstant deltaInstant4 = new HoodieInstant(true, HoodieTimeline.DELTA_COMMIT_ACTION, deltaInstantTime4);
    HoodieInstant deltaInstant5 = new HoodieInstant(true, HoodieTimeline.DELTA_COMMIT_ACTION, deltaInstantTime5);
    commitTimeline.saveAsComplete(deltaInstant4, Optional.empty());
    commitTimeline.saveAsComplete(deltaInstant5, Optional.empty());
    refreshFsView(null);

    List<HoodieDataFile> dataFiles = roView.getAllDataFiles(partitionPath).collect(Collectors.toList());
    if (skipCreatingDataFile) {
      assertTrue("No data file expected", dataFiles.isEmpty());
    } else {
      assertEquals("One data-file is expected as there is only one file-group", 1, dataFiles.size());
      assertEquals("Expect only valid data-file", dataFileName, dataFiles.get(0).getFileName());
    }

    List<FileSlice> fileSliceList = rtView.getLatestFileSlices(partitionPath).collect(Collectors.toList());
    assertEquals("Expect file-slice to be merged", 1, fileSliceList.size());
    FileSlice fileSlice = fileSliceList.get(0);
    assertEquals(fileId, fileSlice.getFileId());
    if (!skipCreatingDataFile) {
      assertEquals("Data file must be present", dataFileName, fileSlice.getDataFile().get().getFileName());
    } else {
      assertFalse("No data-file expected as it was not created", fileSlice.getDataFile().isPresent());
    }
    assertEquals("First instant must be base instant", instantTime1, fileSlice.getBaseCommitTime());
    assertEquals("Compaction requested instant must be used for subsequent log-append",
        compactionRequestedTime, fileSlice.getBaseInstantForLogAppend());
    assertTrue("Expect outstanding compaction instant to be present for this file-slice",
        fileSlice.getOutstandingCompactionInstant().isPresent());
    assertEquals("Valid Compaction Instant set",
        compactionRequestedTime, fileSlice.getOutstandingCompactionInstant().get());
    List<HoodieLogFile> logFiles = fileSlice.getLogFiles().collect(Collectors.toList());
    assertEquals("Log files must include those after compaction request", 4, logFiles.size());
    assertEquals("Log File Order check", fileName4, logFiles.get(0).getFileName());
    assertEquals("Log File Order check", fileName3, logFiles.get(1).getFileName());
    assertEquals("Log File Order check", fileName2, logFiles.get(2).getFileName());
    assertEquals("Log File Order check", fileName1, logFiles.get(3).getFileName());

    // Not simulate Compaction completing - Check the view
    if (!isCompactionInFlight) {
      // For inflight compaction, we already create a data-file to test concurrent inflight case.
      // If we skipped creating data file corresponding to compaction commit, create it now
      new File(basePath + "/" + partitionPath + "/" + compactDataFileName).createNewFile();
    }
    compactionInstant = new HoodieInstant(State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION, compactionRequestedTime);
    if (isCompactionInFlight) {
      commitTimeline.deleteInflight(compactionInstant);
    }
    commitTimeline.saveAsComplete(compactionInstant, Optional.empty());
    refreshFsView(null);
    // populate the cache
    roView.getAllDataFiles(partitionPath);

    dataFiles = roView.getLatestDataFiles(partitionPath).collect(Collectors.toList());
    assertEquals("Expect only one data-files in latest view as there is only one file-group", 1, dataFiles.size());
    assertEquals("Data Filename must match", compactDataFileName, dataFiles.get(0).getFileName());
    fileSliceList = rtView.getLatestFileSlices(partitionPath).collect(Collectors.toList());
    assertEquals("Only one latest file-slice in the partition", 1, fileSliceList.size());
    fileSlice = fileSliceList.get(0);
    assertEquals("Check file-Id is set correctly", fileId, fileSlice.getFileId());
    assertEquals("Check data-filename is set correctly",
        compactDataFileName, fileSlice.getDataFile().get().getFileName());
    assertEquals("Ensure base-instant is now compaction request instant",
        compactionRequestedTime, fileSlice.getBaseCommitTime());
    assertEquals("Ensure base-instant is now compaction request instant",
        compactionRequestedTime, fileSlice.getBaseInstantForLogAppend());
    assertFalse("No outstanding compaction", fileSlice.getOutstandingCompactionInstant().isPresent());
    logFiles = fileSlice.getLogFiles().collect(Collectors.toList());
    assertEquals("Only log-files after compaction request shows up", 2, logFiles.size());
    assertEquals("Log File Order check", fileName4, logFiles.get(0).getFileName());
    assertEquals("Log File Order check", fileName3, logFiles.get(1).getFileName());
  }

  @Test
  public void testGetLatestDataFilesForFileId() throws IOException {
    String partitionPath = "2016/05/01";
    new File(basePath + "/" + partitionPath).mkdirs();
    String fileId = UUID.randomUUID().toString();

    assertFalse("No commit, should not find any data file",
        roView.getLatestDataFiles(partitionPath).filter(dfile -> dfile.getFileId().equals(fileId)).findFirst()
            .isPresent());

    // Only one commit, but is not safe
    String commitTime1 = "1";
    String fileName1 = FSUtils.makeDataFileName(commitTime1, 1, fileId);
    new File(basePath + "/" + partitionPath + "/" + fileName1).createNewFile();
    refreshFsView(null);
    assertFalse("No commit, should not find any data file",
        roView.getLatestDataFiles(partitionPath).filter(dfile -> dfile.getFileId().equals(fileId)).findFirst()
            .isPresent());

    // Make this commit safe
    HoodieActiveTimeline commitTimeline = metaClient.getActiveTimeline();
    HoodieInstant instant1 = new HoodieInstant(true, HoodieTimeline.COMMIT_ACTION, commitTime1);
    commitTimeline.saveAsComplete(instant1, Optional.empty());
    refreshFsView(null);
    assertEquals("", fileName1,
        roView.getLatestDataFiles(partitionPath).filter(dfile -> dfile.getFileId().equals(fileId)).findFirst().get()
            .getFileName());

    // Do another commit, but not safe
    String commitTime2 = "2";
    String fileName2 = FSUtils.makeDataFileName(commitTime2, 1, fileId);
    new File(basePath + "/" + partitionPath + "/" + fileName2).createNewFile();
    refreshFsView(null);
    assertEquals("", fileName1,
        roView.getLatestDataFiles(partitionPath).filter(dfile -> dfile.getFileId().equals(fileId)).findFirst().get()
            .getFileName());

    // Make it safe
    HoodieInstant instant2 = new HoodieInstant(true, HoodieTimeline.COMMIT_ACTION, commitTime2);
    commitTimeline.saveAsComplete(instant2, Optional.empty());
    refreshFsView(null);
    assertEquals("", fileName2,
        roView.getLatestDataFiles(partitionPath).filter(dfile -> dfile.getFileId().equals(fileId)).findFirst().get()
            .getFileName());
  }

  @Test
  public void testStreamLatestVersionInPartition() throws IOException {
    // Put some files in the partition
    String fullPartitionPath = basePath + "/2016/05/01/";
    new File(fullPartitionPath).mkdirs();
    String commitTime1 = "1";
    String commitTime2 = "2";
    String commitTime3 = "3";
    String commitTime4 = "4";
    String fileId1 = UUID.randomUUID().toString();
    String fileId2 = UUID.randomUUID().toString();
    String fileId3 = UUID.randomUUID().toString();
    String fileId4 = UUID.randomUUID().toString();

    new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime1, 1, fileId1)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime4, 1, fileId1)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeLogFileName(fileId1, HoodieLogFile.DELTA_EXTENSION, commitTime4, 0))
        .createNewFile();
    new File(fullPartitionPath + FSUtils.makeLogFileName(fileId1, HoodieLogFile.DELTA_EXTENSION, commitTime4, 1))
        .createNewFile();
    new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime1, 1, fileId2)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime2, 1, fileId2)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime3, 1, fileId2)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeLogFileName(fileId2, HoodieLogFile.DELTA_EXTENSION, commitTime3, 0))
        .createNewFile();
    new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime3, 1, fileId3)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime4, 1, fileId3)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeLogFileName(fileId4, HoodieLogFile.DELTA_EXTENSION, commitTime4, 0))
        .createNewFile();

    new File(basePath + "/.hoodie/" + commitTime1 + ".commit").createNewFile();
    new File(basePath + "/.hoodie/" + commitTime2 + ".commit").createNewFile();
    new File(basePath + "/.hoodie/" + commitTime3 + ".commit").createNewFile();
    new File(basePath + "/.hoodie/" + commitTime4 + ".commit").createNewFile();

    // Now we list the entire partition
    FileStatus[] statuses = metaClient.getFs().listStatus(new Path(fullPartitionPath));
    assertEquals(11, statuses.length);
    refreshFsView(null);

    // Check files as of lastest commit.
    List<FileSlice> allSlices = rtView.getAllFileSlices("2016/05/01").collect(Collectors.toList());
    assertEquals(8, allSlices.size());
    Map<String, Long> fileSliceMap = allSlices.stream().collect(
        Collectors.groupingBy(slice -> slice.getFileId(), Collectors.counting()));
    assertEquals(2, fileSliceMap.get(fileId1).longValue());
    assertEquals(3, fileSliceMap.get(fileId2).longValue());
    assertEquals(2, fileSliceMap.get(fileId3).longValue());
    assertEquals(1, fileSliceMap.get(fileId4).longValue());

    List<HoodieDataFile> dataFileList = roView.getLatestDataFilesBeforeOrOn("2016/05/01", commitTime4)
        .collect(Collectors.toList());
    assertEquals(3, dataFileList.size());
    Set<String> filenames = Sets.newHashSet();
    for (HoodieDataFile status : dataFileList) {
      filenames.add(status.getFileName());
    }
    assertTrue(filenames.contains(FSUtils.makeDataFileName(commitTime4, 1, fileId1)));
    assertTrue(filenames.contains(FSUtils.makeDataFileName(commitTime3, 1, fileId2)));
    assertTrue(filenames.contains(FSUtils.makeDataFileName(commitTime4, 1, fileId3)));

    filenames = Sets.newHashSet();
    List<HoodieLogFile> logFilesList = rtView.getLatestFileSlicesBeforeOrOn("2016/05/01", commitTime4)
        .map(slice -> slice.getLogFiles()).flatMap(logFileList -> logFileList)
        .collect(Collectors.toList());
    assertEquals(logFilesList.size(), 4);
    for (HoodieLogFile logFile : logFilesList) {
      filenames.add(logFile.getFileName());
    }
    assertTrue(filenames.contains(FSUtils.makeLogFileName(fileId1, HoodieLogFile.DELTA_EXTENSION, commitTime4, 0)));
    assertTrue(filenames.contains(FSUtils.makeLogFileName(fileId1, HoodieLogFile.DELTA_EXTENSION, commitTime4, 1)));
    assertTrue(filenames.contains(FSUtils.makeLogFileName(fileId2, HoodieLogFile.DELTA_EXTENSION, commitTime3, 0)));
    assertTrue(filenames.contains(FSUtils.makeLogFileName(fileId4, HoodieLogFile.DELTA_EXTENSION, commitTime4, 0)));

    // Reset the max commit time
    List<HoodieDataFile> dataFiles = roView.getLatestDataFilesBeforeOrOn("2016/05/01", commitTime3)
        .collect(Collectors.toList());
    assertEquals(dataFiles.size(), 3);
    filenames = Sets.newHashSet();
    for (HoodieDataFile status : dataFiles) {
      filenames.add(status.getFileName());
    }
    assertTrue(filenames.contains(FSUtils.makeDataFileName(commitTime1, 1, fileId1)));
    assertTrue(filenames.contains(FSUtils.makeDataFileName(commitTime3, 1, fileId2)));
    assertTrue(filenames.contains(FSUtils.makeDataFileName(commitTime3, 1, fileId3)));

    logFilesList = rtView.getLatestFileSlicesBeforeOrOn("2016/05/01", commitTime3).map(slice -> slice.getLogFiles())
        .flatMap(logFileList -> logFileList).collect(Collectors.toList());
    assertEquals(logFilesList.size(), 1);
    assertTrue(logFilesList.get(0).getFileName()
        .equals(FSUtils.makeLogFileName(fileId2, HoodieLogFile.DELTA_EXTENSION, commitTime3, 0)));
  }

  @Test
  public void testStreamEveryVersionInPartition() throws IOException {
    // Put some files in the partition
    String fullPartitionPath = basePath + "/2016/05/01/";
    new File(fullPartitionPath).mkdirs();
    String commitTime1 = "1";
    String commitTime2 = "2";
    String commitTime3 = "3";
    String commitTime4 = "4";
    String fileId1 = UUID.randomUUID().toString();
    String fileId2 = UUID.randomUUID().toString();
    String fileId3 = UUID.randomUUID().toString();

    new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime1, 1, fileId1)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime4, 1, fileId1)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime1, 1, fileId2)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime2, 1, fileId2)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime3, 1, fileId2)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime3, 1, fileId3)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime4, 1, fileId3)).createNewFile();

    new File(basePath + "/.hoodie/" + commitTime1 + ".commit").createNewFile();
    new File(basePath + "/.hoodie/" + commitTime2 + ".commit").createNewFile();
    new File(basePath + "/.hoodie/" + commitTime3 + ".commit").createNewFile();
    new File(basePath + "/.hoodie/" + commitTime4 + ".commit").createNewFile();

    // Now we list the entire partition
    FileStatus[] statuses = metaClient.getFs().listStatus(new Path(fullPartitionPath));
    assertEquals(7, statuses.length);

    refreshFsView(null);
    List<HoodieFileGroup> fileGroups = fsView.getAllFileGroups("2016/05/01").collect(Collectors.toList());
    assertEquals(3, fileGroups.size());

    for (HoodieFileGroup fileGroup : fileGroups) {
      String fileId = fileGroup.getId();
      Set<String> filenames = Sets.newHashSet();
      fileGroup.getAllDataFiles().forEach(dataFile -> {
        assertEquals("All same fileId should be grouped", fileId, dataFile.getFileId());
        filenames.add(dataFile.getFileName());
      });
      if (fileId.equals(fileId1)) {
        assertEquals(filenames, Sets.newHashSet(FSUtils.makeDataFileName(commitTime1, 1, fileId1),
            FSUtils.makeDataFileName(commitTime4, 1, fileId1)));
      } else if (fileId.equals(fileId2)) {
        assertEquals(filenames, Sets.newHashSet(FSUtils.makeDataFileName(commitTime1, 1, fileId2),
            FSUtils.makeDataFileName(commitTime2, 1, fileId2), FSUtils.makeDataFileName(commitTime3, 1, fileId2)));
      } else {
        assertEquals(filenames, Sets.newHashSet(FSUtils.makeDataFileName(commitTime3, 1, fileId3),
            FSUtils.makeDataFileName(commitTime4, 1, fileId3)));
      }
    }
  }

  @Test
  public void streamLatestVersionInRange() throws IOException {
    // Put some files in the partition
    String fullPartitionPath = basePath + "/2016/05/01/";
    new File(fullPartitionPath).mkdirs();
    String commitTime1 = "1";
    String commitTime2 = "2";
    String commitTime3 = "3";
    String commitTime4 = "4";
    String fileId1 = UUID.randomUUID().toString();
    String fileId2 = UUID.randomUUID().toString();
    String fileId3 = UUID.randomUUID().toString();

    new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime1, 1, fileId1)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeLogFileName(fileId1, HoodieLogFile.DELTA_EXTENSION, commitTime1, 0))
        .createNewFile();
    new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime3, 1, fileId1)).createNewFile();

    new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime1, 1, fileId2)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime2, 1, fileId2)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime3, 1, fileId2)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeLogFileName(fileId2, HoodieLogFile.DELTA_EXTENSION, commitTime4, 0))
        .createNewFile();

    new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime3, 1, fileId3)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime4, 1, fileId3)).createNewFile();

    new File(basePath + "/.hoodie/" + commitTime1 + ".commit").createNewFile();
    new File(basePath + "/.hoodie/" + commitTime2 + ".commit").createNewFile();
    new File(basePath + "/.hoodie/" + commitTime3 + ".commit").createNewFile();
    new File(basePath + "/.hoodie/" + commitTime4 + ".commit").createNewFile();

    // Now we list the entire partition
    FileStatus[] statuses = metaClient.getFs().listStatus(new Path(fullPartitionPath));
    assertEquals(9, statuses.length);

    refreshFsView(statuses);
    List<HoodieDataFile> dataFiles = roView.getLatestDataFilesInRange(Lists.newArrayList(commitTime2, commitTime3))
        .collect(Collectors.toList());
    assertEquals(3, dataFiles.size());
    Set<String> filenames = Sets.newHashSet();
    for (HoodieDataFile status : dataFiles) {
      filenames.add(status.getFileName());
    }
    assertTrue(filenames.contains(FSUtils.makeDataFileName(commitTime3, 1, fileId2)));
    assertTrue(filenames.contains(FSUtils.makeDataFileName(commitTime3, 1, fileId3)));

    List<FileSlice> slices = rtView.getLatestFileSliceInRange(Lists.newArrayList(commitTime3, commitTime4))
        .collect(Collectors.toList());
    assertEquals(3, slices.size());
    for (FileSlice slice : slices) {
      if (slice.getFileId().equals(fileId1)) {
        assertEquals(slice.getBaseCommitTime(), commitTime3);
        assertTrue(slice.getDataFile().isPresent());
        assertEquals(slice.getLogFiles().count(), 0);
      } else if (slice.getFileId().equals(fileId2)) {
        assertEquals(slice.getBaseCommitTime(), commitTime4);
        assertFalse(slice.getDataFile().isPresent());
        assertEquals(slice.getLogFiles().count(), 1);
      } else if (slice.getFileId().equals(fileId3)) {
        assertEquals(slice.getBaseCommitTime(), commitTime4);
        assertTrue(slice.getDataFile().isPresent());
        assertEquals(slice.getLogFiles().count(), 0);
      }
    }
  }

  @Test
  public void streamLatestVersionsBefore() throws IOException {
    // Put some files in the partition
    String partitionPath = "2016/05/01/";
    String fullPartitionPath = basePath + "/" + partitionPath;
    new File(fullPartitionPath).mkdirs();
    String commitTime1 = "1";
    String commitTime2 = "2";
    String commitTime3 = "3";
    String commitTime4 = "4";
    String fileId1 = UUID.randomUUID().toString();
    String fileId2 = UUID.randomUUID().toString();
    String fileId3 = UUID.randomUUID().toString();

    new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime1, 1, fileId1)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime4, 1, fileId1)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime1, 1, fileId2)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime2, 1, fileId2)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime3, 1, fileId2)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime3, 1, fileId3)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime4, 1, fileId3)).createNewFile();

    new File(basePath + "/.hoodie/" + commitTime1 + ".commit").createNewFile();
    new File(basePath + "/.hoodie/" + commitTime2 + ".commit").createNewFile();
    new File(basePath + "/.hoodie/" + commitTime3 + ".commit").createNewFile();
    new File(basePath + "/.hoodie/" + commitTime4 + ".commit").createNewFile();

    // Now we list the entire partition
    FileStatus[] statuses = metaClient.getFs().listStatus(new Path(fullPartitionPath));
    assertEquals(7, statuses.length);

    refreshFsView(null);
    List<HoodieDataFile> dataFiles = roView.getLatestDataFilesBeforeOrOn(partitionPath, commitTime2)
        .collect(Collectors.toList());
    assertEquals(2, dataFiles.size());
    Set<String> filenames = Sets.newHashSet();
    for (HoodieDataFile status : dataFiles) {
      filenames.add(status.getFileName());
    }
    assertTrue(filenames.contains(FSUtils.makeDataFileName(commitTime1, 1, fileId1)));
    assertTrue(filenames.contains(FSUtils.makeDataFileName(commitTime2, 1, fileId2)));
  }

  @Test
  public void streamLatestVersions() throws IOException {
    // Put some files in the partition
    String partitionPath = "2016/05/01/";
    String fullPartitionPath = basePath + "/" + partitionPath;
    new File(fullPartitionPath).mkdirs();
    String commitTime1 = "1";
    String commitTime2 = "2";
    String commitTime3 = "3";
    String commitTime4 = "4";
    String fileId1 = UUID.randomUUID().toString();
    String fileId2 = UUID.randomUUID().toString();
    String fileId3 = UUID.randomUUID().toString();

    new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime1, 1, fileId1)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeLogFileName(fileId1, HoodieLogFile.DELTA_EXTENSION, commitTime1, 0))
        .createNewFile();
    new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime4, 1, fileId1)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeLogFileName(fileId1, HoodieLogFile.DELTA_EXTENSION, commitTime4, 0))
        .createNewFile();

    new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime1, 1, fileId2)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime2, 1, fileId2)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeLogFileName(fileId2, HoodieLogFile.DELTA_EXTENSION, commitTime2, 0))
        .createNewFile();
    new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime3, 1, fileId2)).createNewFile();

    new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime3, 1, fileId3)).createNewFile();
    new File(fullPartitionPath + FSUtils.makeDataFileName(commitTime4, 1, fileId3)).createNewFile();

    new File(basePath + "/.hoodie/" + commitTime1 + ".commit").createNewFile();
    new File(basePath + "/.hoodie/" + commitTime2 + ".commit").createNewFile();
    new File(basePath + "/.hoodie/" + commitTime3 + ".commit").createNewFile();
    new File(basePath + "/.hoodie/" + commitTime4 + ".commit").createNewFile();

    // Now we list the entire partition
    FileStatus[] statuses = metaClient.getFs().listStatus(new Path(fullPartitionPath));
    assertEquals(10, statuses.length);

    refreshFsView(statuses);

    List<HoodieFileGroup> fileGroups = fsView.getAllFileGroups(partitionPath).collect(Collectors.toList());
    assertEquals(3, fileGroups.size());
    for (HoodieFileGroup fileGroup : fileGroups) {
      List<FileSlice> slices = fileGroup.getAllFileSlices().collect(Collectors.toList());
      if (fileGroup.getId().equals(fileId1)) {
        assertEquals(2, slices.size());
        assertEquals(commitTime4, slices.get(0).getBaseCommitTime());
        assertEquals(commitTime1, slices.get(1).getBaseCommitTime());
      } else if (fileGroup.getId().equals(fileId2)) {
        assertEquals(3, slices.size());
        assertEquals(commitTime3, slices.get(0).getBaseCommitTime());
        assertEquals(commitTime2, slices.get(1).getBaseCommitTime());
        assertEquals(commitTime1, slices.get(2).getBaseCommitTime());
      } else if (fileGroup.getId().equals(fileId3)) {
        assertEquals(2, slices.size());
        assertEquals(commitTime4, slices.get(0).getBaseCommitTime());
        assertEquals(commitTime3, slices.get(1).getBaseCommitTime());
      }
    }

    List<HoodieDataFile> statuses1 = roView.getLatestDataFiles().collect(Collectors.toList());
    assertEquals(3, statuses1.size());
    Set<String> filenames = Sets.newHashSet();
    for (HoodieDataFile status : statuses1) {
      filenames.add(status.getFileName());
    }
    assertTrue(filenames.contains(FSUtils.makeDataFileName(commitTime4, 1, fileId1)));
    assertTrue(filenames.contains(FSUtils.makeDataFileName(commitTime3, 1, fileId2)));
    assertTrue(filenames.contains(FSUtils.makeDataFileName(commitTime4, 1, fileId3)));
  }
}
