/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi.bootstrap;

import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.config.HoodieWriteConfig.BootstrapMode;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public abstract class BootstrapPartitionSelector implements Serializable {

  protected final HoodieWriteConfig writeConfig;

  public BootstrapPartitionSelector(HoodieWriteConfig writeConfig) {
    this.writeConfig = writeConfig;
  }

  /**
   * Classify partitions for the purpose of bootstrapping
   * @param partitions List of partitions with files present in each partitions
   * @return a partitions grouped by bootstrap mode
   */
  public abstract Map<BootstrapMode, List<Pair<String, List<String>>>> select(
      List<Pair<String, List<String>>> partitions);
}
