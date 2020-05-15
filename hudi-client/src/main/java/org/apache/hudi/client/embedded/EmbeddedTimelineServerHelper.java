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

package org.apache.hudi.client.embedded;

import org.apache.hudi.common.util.Option;
import org.apache.hudi.config.HoodieWriteConfig;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.IOException;

/**
 * Helper class to instantiate embedded timeline service.
 */
public class EmbeddedTimelineServerHelper {

  private static final Logger LOG = LogManager.getLogger(EmbeddedTimelineService.class);

  public static Option<EmbeddedTimelineService> createEmbeddedTimelineService(JavaSparkContext jsc,
      HoodieWriteConfig config) throws IOException {
    Option<EmbeddedTimelineService> timelineServer = Option.empty();
    if (config.isEmbeddedTimelineServerEnabled()) {
      // Run Embedded Timeline Server
      LOG.info("Starting Timeline service !!");
      timelineServer = Option.of(new EmbeddedTimelineService(jsc.hadoopConfiguration(), jsc.getConf(),
          config.getClientSpecifiedViewStorageConfig()));

      timelineServer.get().startServer();
      // Allow executor to find this newly instantiated timeline service
      config.setViewStorageConfig(timelineServer.get().getRemoteFileSystemViewConfig());
    }
    return timelineServer;
  }
}
