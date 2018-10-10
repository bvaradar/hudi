package com.uber.hoodie.utilities.sources.helpers;

import com.google.common.base.Preconditions;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.table.HoodieTimeline;
import com.uber.hoodie.common.table.timeline.HoodieInstant;
import com.uber.hoodie.common.util.collection.Pair;
import java.util.Optional;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.ReduceFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;


/**
 * Helper for Hudi Incremental Source. Has APIs to
 *   (a) calculate begin and end instant time for incrementally pulling from Hudi source
 *   (b) Find max seen instant to be set as checkpoint for next fetch.
 */
public class IncrSourceHelper {

  /**
   * Get a timestamp which is the next value in a descending sequence
   *
   * @param timestamp Timestamp
   */
  private static String getStrictlyLowerTimestamp(String timestamp) {
    long ts = Long.parseLong(timestamp);
    Long lower = ts - 1;
    return "" + lower;
  }

  /**
   * Find begin and end instants to be set for the next fetch
   *
   * @param jssc Java Spark Context
   * @param srcBasePath Base path of Hudi source table
   * @param numInstantsPerFetch Max Instants per fetch
   * @param beginInstant Last Checkpoint String
   * @param readLatestOnMissingBeginInstant when begin instant is missing, allow reading from latest committed instant
   * @return begin and end instants
   */
  public static Pair<String, Optional<String>> calculateBeginAndEndInstants(
      JavaSparkContext jssc, String srcBasePath, int numInstantsPerFetch, Optional<String> beginInstant,
      boolean readLatestOnMissingBeginInstant) {
    HoodieTableMetaClient srcMetaClient = new HoodieTableMetaClient(jssc.hadoopConfiguration(),
        srcBasePath, true);

    final HoodieTimeline activeCommitTimeline =
        srcMetaClient.getActiveTimeline().getCommitTimeline().filterCompletedInstants();

    String beginInstantTime = beginInstant.orElseGet(() -> {
      if (readLatestOnMissingBeginInstant) {
        Optional<HoodieInstant> lastInstant = activeCommitTimeline.lastInstant();
        return lastInstant.map(hoodieInstant -> getStrictlyLowerTimestamp(hoodieInstant.getTimestamp())).orElse("000");
      } else {
        throw new IllegalArgumentException("Missing begin instant for incremental pull. For reading from latest "
            + "committed instant set hoodie.deltastreamer.source.hoodie.read_latest_on_midding_ckpt to true");
      }
    });

    if (numInstantsPerFetch > 0) {
      Optional<HoodieInstant> nthInstant =
          activeCommitTimeline.findInstantsAfter(beginInstantTime, numInstantsPerFetch)
              .getInstants().findFirst();
      return Pair.of(beginInstantTime, nthInstant.map(instant -> instant.getTimestamp()));
    }
    // If no upper-bound provided, IncrRelation will automatically use the latest version
    return Pair.of(beginInstantTime, Optional.empty());
  }

  /**
   * Validate instant time seen in the incoming row
   *
   * @param row Input Row
   * @param instantTime Hoodie Instant time of the row
   * @param sinceInstant begin instant of the batch
   * @param endInstant end instant of the batch
   */
  private static void validateInstantTime(Row row,
      String instantTime, String sinceInstant, Optional<String> endInstant) {
    Preconditions.checkNotNull(instantTime);
    Preconditions.checkArgument(HoodieTimeline.compareTimestamps(instantTime,
        sinceInstant, HoodieTimeline.GREATER),
        "Instant time(_hoodie_commit_time) in row (" + row + ") was : " + instantTime
            + "but expected to be between " + sinceInstant + "(excl) - "
            + endInstant + "(incl)");
    endInstant.ifPresent(eInstant -> {
      Preconditions.checkArgument(HoodieTimeline.compareTimestamps(instantTime,
          eInstant, HoodieTimeline.LESSER_OR_EQUAL),
          "Instant time(_hoodie_commit_time) in row (" + row + ") was : " + instantTime
              + "but expected to be between " + sinceInstant + "(excl) - "
              + eInstant + "(incl)");
    });
  }

  /**
   * Validate while trying to get the max seen instant time
   *
   * @param inputSrc Input Hudi dataset (field __hoodie_commit_time expected)
   * @param sinceInstant begin instant for the batch
   * @param endInstant End Instant for the batch
   * @return max seen instant if dataset is not empty
   */
  public static Optional<String> validateAndGetMaxSeenInstant(Dataset<Row> inputSrc, String sinceInstant,
      String endInstant) {
    if (inputSrc.rdd().isEmpty()) {
      return Optional.empty();
    }
    String maxSeenInstant =
        inputSrc.map((MapFunction<Row, String>) ((Row row) -> {
          // _hoodie_instant_time
          String instantTime = row.getString(0);
          validateInstantTime(row, instantTime, sinceInstant, Optional.ofNullable(endInstant));
          return instantTime;
        }), Encoders.STRING()).reduce(
            (ReduceFunction<String>) (val1, val2) -> HoodieTimeline.compareTimestamps(val1, val2,
                HoodieTimeline.GREATER_OR_EQUAL) ? val1 : val2);
    return Optional.of(maxSeenInstant);
  }
}
