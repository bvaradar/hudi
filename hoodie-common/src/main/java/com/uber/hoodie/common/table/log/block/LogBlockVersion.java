/*
 * Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
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

package com.uber.hoodie.common.table.log.block;

import static com.uber.hoodie.common.table.log.block.HoodieLogBlock.version;

abstract class HoodieLogBlockVersion {

  private final int currentVersion;

  public final static int DEFAULT_VERSION = 0;

  HoodieLogBlockVersion(int version) {
    this.currentVersion = version;
  }

  int getVersion() {
    return currentVersion;
  }
}

/**
 * A set of feature flags associated with a data log block format.
 * Versions are changed when the log block format changes.
 * TODO(na) - Implement policies around major/minor versions
 */
final class HoodieAvroDataBlockVersion extends HoodieLogBlockVersion {

  HoodieAvroDataBlockVersion(int version) {
    super(version);
  }

  public boolean hasRecordCount() {
    switch (super.getVersion()) {
      case DEFAULT_VERSION:
        return true;
      default:
        return true;
    }
  }
}

/**
 * A set of feature flags associated with a command log block format.
 * Versions are changed when the log block format changes.
 * TODO(na) - Implement policies around major/minor versions
 */
final class HoodieCommandBlockVersion extends HoodieLogBlockVersion {

  HoodieCommandBlockVersion(int version) {
    super(version);
  }
}

/**
 * A set of feature flags associated with a delete log block format.
 * Versions are changed when the log block format changes.
 * TODO(na) - Implement policies around major/minor versions
 */
final class HoodieDeleteBlockVersion extends HoodieLogBlockVersion {

  HoodieDeleteBlockVersion(int version) {
    super(version);
  }
}