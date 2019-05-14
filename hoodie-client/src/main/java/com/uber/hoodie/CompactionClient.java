/*
 * Copyright (c) 2019 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
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

package com.uber.hoodie;

import com.codahale.metrics.Timer;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Preconditions;
import com.uber.hoodie.avro.model.HoodieCompactionPlan;
import com.uber.hoodie.common.model.HoodieCommitMetadata;
import com.uber.hoodie.common.model.HoodieRecordPayload;
import com.uber.hoodie.common.model.HoodieWriteStat;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.table.HoodieTimeline;
import com.uber.hoodie.common.table.timeline.HoodieActiveTimeline;
import com.uber.hoodie.common.table.timeline.HoodieInstant;
import com.uber.hoodie.common.table.timeline.HoodieInstant.State;
import com.uber.hoodie.common.util.AvroUtils;
import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.hoodie.exception.HoodieCommitException;
import com.uber.hoodie.exception.HoodieCompactionException;
import com.uber.hoodie.exception.HoodieIOException;
import com.uber.hoodie.metrics.HoodieMetrics;
import com.uber.hoodie.table.HoodieTable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.text.ParseException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Compaction Client - Supports API to schedule and run compaction
 * @param <T>
 */
public class CompactionClient<T extends HoodieRecordPayload> extends AbstractHoodieClient {

  private static Logger logger = LogManager.getLogger(CompactionClient.class);

  private transient Timer.Context compactionTimer;
  private final transient HoodieMetrics metrics;

  protected CompactionClient(JavaSparkContext jsc, HoodieWriteConfig clientConfig) {
    super(jsc, clientConfig);
    this.metrics = new HoodieMetrics(config, config.getTableName());
  }

  /**
   * Schedules a new compaction instant
   */
  public Optional<String> scheduleCompaction(Optional<Map<String, String>> extraMetadata) throws IOException {
    String instantTime = HoodieActiveTimeline.createNewCommitTime();
    logger.info("Generate a new instant time " + instantTime);
    boolean notEmpty = scheduleCompactionAtInstant(instantTime, extraMetadata);
    return notEmpty ? Optional.of(instantTime) : Optional.empty();
  }

  /**
   * Schedules a new compaction instant with passed-in instant time
   *
   * @param instantTime Compaction Instant Time
   * @param extraMetadata Extra Metadata to be stored
   */
  public boolean scheduleCompactionAtInstant(String instantTime, Optional<Map<String, String>> extraMetadata)
      throws IOException {
    HoodieTableMetaClient metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(),
        config.getBasePath(), true);
    // if there are inflight writes, their instantTime must not be less than that of compaction instant time
    metaClient.getActiveTimeline().filterInflightsExcludingCompaction().firstInstant().ifPresent(earliestInflight -> {
      Preconditions.checkArgument(
          HoodieTimeline.compareTimestamps(earliestInflight.getTimestamp(), instantTime, HoodieTimeline.GREATER),
          "Earliest write inflight instant time must be later "
              + "than compaction time. Earliest :" + earliestInflight + ", Compaction scheduled at " + instantTime);
    });
    // Committed and pending compaction instants should have strictly lower timestamps
    List<HoodieInstant> conflictingInstants =
        metaClient.getActiveTimeline().getCommitsAndCompactionTimeline().getInstants().filter(instant ->
            HoodieTimeline.compareTimestamps(instant.getTimestamp(), instantTime,
                HoodieTimeline.GREATER_OR_EQUAL)).collect(Collectors.toList());
    Preconditions.checkArgument(conflictingInstants.isEmpty(),
        "Following instants have timestamps >= compactionInstant (" + instantTime + ") Instants :"
            + conflictingInstants);
    HoodieTable<T> table = HoodieTable.getHoodieTable(metaClient, config, jsc);
    HoodieCompactionPlan workload = table.scheduleCompaction(jsc, instantTime);
    if (workload != null && (workload.getOperations() != null) && (!workload.getOperations().isEmpty())) {
      extraMetadata.ifPresent(workload::setExtraMetadata);
      HoodieInstant compactionInstant =
          new HoodieInstant(State.REQUESTED, HoodieTimeline.COMPACTION_ACTION, instantTime);
      metaClient.getActiveTimeline().saveToCompactionRequested(compactionInstant,
          AvroUtils.serializeCompactionPlan(workload));
      return true;
    }
    return false;
  }

  /**
   * Performs Compaction for the workload stored in instant-time
   *
   * @param compactionInstantTime Compaction Instant Time
   */
  public JavaRDD<WriteStatus> compact(String compactionInstantTime) throws IOException {
    return compact(compactionInstantTime, config.shouldAutoCommit());
  }

  /**
   * Commit a compaction operation. Allow passing additional meta-data to be stored in commit instant file.
   */
  public void commitCompaction(String compactionInstantTime, JavaRDD<WriteStatus> writeStatuses,
      Optional<Map<String, String>> extraMetadata) throws IOException {
    HoodieTableMetaClient metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(),
        config.getBasePath(), true);
    HoodieTable<T> table = HoodieTable.getHoodieTable(metaClient, config, jsc);
    HoodieActiveTimeline timeline = metaClient.getActiveTimeline();
    HoodieCompactionPlan compactionPlan = AvroUtils.deserializeCompactionPlan(
        timeline.getInstantAuxiliaryDetails(HoodieTimeline.getCompactionRequestedInstant(compactionInstantTime)).get());
    // Merge extra meta-data passed by user with the one already in inflight compaction
    Optional<Map<String, String>> mergedMetaData = extraMetadata.map(m -> {
      Map<String, String> merged = new HashMap<>();
      Map<String, String> extraMetaDataFromInstantFile = compactionPlan.getExtraMetadata();
      if (extraMetaDataFromInstantFile != null) {
        merged.putAll(extraMetaDataFromInstantFile);
      }
      // Overwrite/Merge with the user-passed meta-data
      merged.putAll(m);
      return Optional.of(merged);
    }).orElseGet(() -> Optional.ofNullable(compactionPlan.getExtraMetadata()));
    commitCompaction(writeStatuses, table, compactionInstantTime, true, mergedMetaData);
  }

  /**
   * Compaction specific private methods
   */

  /**
   * Ensures compaction instant is in expected state and performs Compaction for the workload stored in instant-time
   *
   * @param compactionInstantTime Compaction Instant Time
   */
  private JavaRDD<WriteStatus> compact(String compactionInstantTime, boolean autoCommit) throws IOException {
    // Create a Hoodie table which encapsulated the commits and files visible
    HoodieTableMetaClient metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(),
        config.getBasePath(), true);
    HoodieTable<T> table = HoodieTable.getHoodieTable(metaClient, config, jsc);
    HoodieTimeline pendingCompactionTimeline = metaClient.getActiveTimeline().filterPendingCompactionTimeline();
    HoodieInstant inflightInstant = HoodieTimeline.getCompactionInflightInstant(compactionInstantTime);
    if (pendingCompactionTimeline.containsInstant(inflightInstant)) {
      //inflight compaction - Needs to rollback first deleting new parquet files before we run compaction.
      rollbackInflightCompaction(inflightInstant, table);
      // refresh table
      metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(), config.getBasePath(), true);
      table = HoodieTable.getHoodieTable(metaClient, config, jsc);
      pendingCompactionTimeline = metaClient.getActiveTimeline().filterPendingCompactionTimeline();
    }

    HoodieInstant instant = HoodieTimeline.getCompactionRequestedInstant(compactionInstantTime);
    if (pendingCompactionTimeline.containsInstant(instant)) {
      return runCompaction(instant, metaClient.getActiveTimeline(), autoCommit);
    } else {
      throw new IllegalStateException("No Compaction request available at " + compactionInstantTime
          + " to run compaction");
    }
  }

  /**
   * Perform compaction operations as specified in the compaction commit file
   *
   * @param compactionInstant Compacton Instant time
   * @param activeTimeline Active Timeline
   * @param autoCommit Commit after compaction
   * @return RDD of Write Status
   */
  private JavaRDD<WriteStatus> runCompaction(
      HoodieInstant compactionInstant, HoodieActiveTimeline activeTimeline, boolean autoCommit) throws IOException {
    HoodieCompactionPlan compactionPlan = AvroUtils.deserializeCompactionPlan(
        activeTimeline.getInstantAuxiliaryDetails(compactionInstant).get());
    // Mark instant as compaction inflight
    activeTimeline.transitionCompactionRequestedToInflight(compactionInstant);
    compactionTimer = metrics.getCompactionCtx();
    // Create a Hoodie table which encapsulated the commits and files visible
    HoodieTableMetaClient metaClient = new HoodieTableMetaClient(jsc.hadoopConfiguration(),
        config.getBasePath(), true);
    HoodieTable<T> table = HoodieTable.getHoodieTable(metaClient, config, jsc);
    JavaRDD<WriteStatus> statuses = table.compact(jsc, compactionInstant.getTimestamp(), compactionPlan);
    // Force compaction action
    statuses.persist(config.getWriteStatusStorageLevel());
    // pass extra-metada so that it gets stored in commit file automatically
    commitCompaction(statuses, table, compactionInstant.getTimestamp(), autoCommit,
        Optional.ofNullable(compactionPlan.getExtraMetadata()));
    return statuses;
  }

  /**
   * Commit Compaction and track metrics
   *
   * @param compactedStatuses Compaction Write status
   * @param table Hoodie Table
   * @param compactionCommitTime Compaction Commit Time
   * @param autoCommit Auto Commit
   * @param extraMetadata Extra Metadata to store
   */
  protected void commitCompaction(JavaRDD<WriteStatus> compactedStatuses, HoodieTable<T> table,
      String compactionCommitTime, boolean autoCommit, Optional<Map<String, String>> extraMetadata) {
    if (autoCommit) {
      HoodieCommitMetadata metadata =
          doCompactionCommit(compactedStatuses, table.getMetaClient(), compactionCommitTime, extraMetadata);
      if (compactionTimer != null) {
        long durationInMs = metrics.getDurationInMs(compactionTimer.stop());
        try {
          metrics.updateCommitMetrics(HoodieActiveTimeline.COMMIT_FORMATTER.parse(compactionCommitTime).getTime(),
              durationInMs, metadata, HoodieActiveTimeline.COMPACTION_ACTION);
        } catch (ParseException e) {
          throw new HoodieCommitException(
              "Commit time is not of valid format.Failed to commit compaction " + config.getBasePath()
                  + " at time " + compactionCommitTime, e);
        }
      }
      logger.info("Compacted successfully on commit " + compactionCommitTime);
    } else {
      logger.info("Compaction did not run for commit " + compactionCommitTime);
    }
  }

  private HoodieCommitMetadata doCompactionCommit(JavaRDD<WriteStatus> writeStatuses,
      HoodieTableMetaClient metaClient, String compactionCommitTime, Optional<Map<String, String>> extraMetadata) {
    List<HoodieWriteStat> updateStatusMap = writeStatuses.map(WriteStatus::getStat)
        .collect();

    HoodieCommitMetadata metadata = new HoodieCommitMetadata(true);
    for (HoodieWriteStat stat : updateStatusMap) {
      metadata.addWriteStat(stat.getPartitionPath(), stat);
    }

    // Copy extraMetadata
    extraMetadata.ifPresent(m -> {
      m.entrySet().stream().forEach(e -> {
        metadata.addMetadata(e.getKey(), e.getValue());
      });
    });

    logger.info("Compaction finished with result " + metadata);

    logger.info("Committing Compaction " + compactionCommitTime);
    HoodieActiveTimeline activeTimeline = metaClient.getActiveTimeline();

    try {
      activeTimeline.transitionCompactionInflightToComplete(
          new HoodieInstant(State.INFLIGHT, HoodieTimeline.COMPACTION_ACTION, compactionCommitTime),
          Optional.of(metadata.toJsonString().getBytes(StandardCharsets.UTF_8)));
    } catch (IOException e) {
      throw new HoodieCompactionException(
          "Failed to commit " + metaClient.getBasePath() + " at time " + compactionCommitTime, e);
    }
    return metadata;
  }

  /**
   * Rollback failed compactions. Inflight rollbacks for compactions revert the .inflight file to the .requested file
   *
   * @param inflightInstant Inflight Compaction Instant
   * @param table Hoodie Table
   */
  @VisibleForTesting
  void rollbackInflightCompaction(HoodieInstant inflightInstant, HoodieTable table) throws IOException {
    table.rollback(jsc, inflightInstant.getTimestamp(), false);
    // Revert instant state file
    table.getActiveTimeline().revertCompactionInflightToRequested(inflightInstant);
  }

  /**
   * Performs a compaction operation on a dataset, serially before or after an insert/upsert action.
   */
  protected Optional<String> forceCompact(Optional<Map<String, String>> extraMetadata) throws IOException {
    Optional<String> compactionInstantTimeOpt = scheduleCompaction(extraMetadata);
    compactionInstantTimeOpt.ifPresent(compactionInstantTime -> {
      try {
        compact(compactionInstantTime, true);
      } catch (IOException ioe) {
        throw new HoodieIOException(ioe.getMessage(), ioe);
      }
    });
    return compactionInstantTimeOpt;
  }
}
