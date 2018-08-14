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
 *
 */

package com.uber.hoodie.index.bloom;

import com.google.common.annotations.VisibleForTesting;
import com.uber.hoodie.common.model.HoodieKey;
import com.uber.hoodie.common.model.HoodieRecordPayload;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.util.FSUtils;
import com.uber.hoodie.config.HoodieWriteConfig;
import com.uber.hoodie.exception.HoodieIOException;
import com.uber.hoodie.table.HoodieTable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

/**
 * This filter will only work with hoodie dataset since it will only load partitions
 * with .hoodie_partition_metadata file in it.
 */
public class HoodieGlobalBloomIndex<T extends HoodieRecordPayload> extends HoodieBloomIndex<T> {

  public HoodieGlobalBloomIndex(HoodieWriteConfig config) {
    super(config);
  }

  /**
   * Load all involved files as <Partition, filename> pair RDD from all partitions in the table.
   */
  @Override
  @VisibleForTesting
  List<Tuple2<String, BloomIndexFileInfo>> loadInvolvedFiles(List<String> partitions, final JavaSparkContext jsc,
                                                             final HoodieTable hoodieTable) {
    HoodieTableMetaClient metaClient = hoodieTable.getMetaClient();
    try {
      List<String> allPartitionPaths = FSUtils
          .getAllPartitionPaths(metaClient.getFs(), metaClient.getBasePath(),
              config.shouldAssumeDatePartitioning());
      return super.loadInvolvedFiles(allPartitionPaths, jsc, hoodieTable);
    } catch (IOException e) {
      throw new HoodieIOException("Failed to load all partitions", e);
    }
  }

  /**
   * For each incoming record, produce N output records, 1 each for each file against which the
   * record's key needs to be checked. For datasets, where the keys have a definite insert order
   * (e.g: timestamp as prefix), the number of files to be compared gets cut down a lot from range
   * pruning.
   *
   * Sub-partition to ensure the records can be looked up against files & also prune
   * file<=>record comparisons based on recordKey
   * ranges in the index info.
   * the partition path of the incoming record (partitionRecordKeyPairRDD._2()) will be ignored
   * since the search scope should be bigger than that
   */
  @Override
  @VisibleForTesting
  JavaPairRDD<String, Tuple2<String, HoodieKey>> explodeRecordRDDWithFileComparisons(
      final Map<String, List<BloomIndexFileInfo>> partitionToFileIndexInfo,
      JavaPairRDD<String, String> partitionRecordKeyPairRDD) {
    List<Tuple2<String, BloomIndexFileInfo>> indexInfos =
        partitionToFileIndexInfo.entrySet().stream()
            .flatMap(e1 -> e1.getValue().stream()
                .map(e2 -> new Tuple2<>(e1.getKey(), e2)))
            .collect(Collectors.toList());

    return partitionRecordKeyPairRDD.map(partitionRecordKeyPair -> {
      String recordKey = partitionRecordKeyPair._2();

      List<Tuple2<String, Tuple2<String, HoodieKey>>> recordComparisons = new ArrayList<>();
      if (indexInfos != null) { // could be null, if there are no files in a given partition yet.
        // for each candidate file in partition, that needs to be compared.
        for (Tuple2<String, BloomIndexFileInfo> indexInfo : indexInfos) {
          if (shouldCompareWithFile(indexInfo._2(), recordKey)) {
            recordComparisons.add(
                new Tuple2<>(String.format("%s#%s", indexInfo._2().getFileName(), recordKey),
                    new Tuple2<>(indexInfo._2().getFileName(),
                        new HoodieKey(recordKey, indexInfo._1()))));
          }
        }
      }
      return recordComparisons;
    }).flatMapToPair(t -> t.iterator());
  }

}
