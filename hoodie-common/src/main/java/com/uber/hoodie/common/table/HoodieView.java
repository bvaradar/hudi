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

package com.uber.hoodie.common.table;

import com.uber.hoodie.common.model.CompactionOperation;
import com.uber.hoodie.common.table.timeline.HoodieInstant;
import com.uber.hoodie.common.util.Option;
import com.uber.hoodie.common.util.collection.Pair;
import java.util.stream.Stream;

/*
 * A consolidate file-system view interface exposing both realtime and read-optimized views along with
 * update operations.
 */
public interface HoodieView extends TableFileSystemView, TableFileSystemView.ReadOptimizedView,
    TableFileSystemView.RealtimeView {

  /**
   * Return Pending Compaction Operations
   *
   * @return Pair<Pair<InstantTime,CompactionOperation>>
   */
  Stream<Pair<String, CompactionOperation>> getPendingCompactionOperations();

  /**
   * Allow View to release resources and close
   */
  void close();

  /**
   * Reset View so that they can be refreshed
   */
  void reset();

  /**
   * Last Known Instant on which the view is built
   */
  Option<HoodieInstant> getLastInstant();

  /**
   * Last Known Instant on which the view is built
   */
  HoodieTimeline getTimeline();

  /**
   * Sync with File-System Hoodie Timeline
   */
  void sync();
}
