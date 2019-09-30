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

package org.apache.hudi.common.model;

import com.google.common.base.Preconditions;
import java.io.Serializable;
import java.util.Objects;

/**
 * Metadata Layout Version
 */
public class HoodieMetadataVersion implements Serializable, Comparable<HoodieMetadataVersion> {

  public static Integer VERSION_0 = 0; // pre 0.5.1  version format
  public static Integer VERSION_1 = 1; // current version

  public static Integer CURR_VERSION = VERSION_1;

  private Integer version;

  public HoodieMetadataVersion(Integer version) {
    Preconditions.checkArgument(version <= CURR_VERSION);
    Preconditions.checkArgument(version >= VERSION_0);
    this.version = version;
  }

  public boolean isPre051Format() {
    return version == VERSION_0;
  }

  public Integer getVersion() {
    return version;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    HoodieMetadataVersion that = (HoodieMetadataVersion) o;
    return Objects.equals(version, that.version);
  }

  @Override
  public int hashCode() {
    return Objects.hash(version);
  }

  @Override
  public int compareTo(HoodieMetadataVersion o) {
    return Integer.compare(version, o.version);
  }
}
