/*
 * Copyright (c) 2018 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
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

package com.uber.hoodie.timeline;

import static com.uber.hoodie.common.table.view.RemoteHoodieTableFileSystemView.*;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.uber.hoodie.common.table.HoodieTimeline;
import com.uber.hoodie.common.table.timeline.request.CompactionOpDTO;
import com.uber.hoodie.common.table.timeline.request.DataFileDTO;
import com.uber.hoodie.common.table.timeline.request.FileGroupDTO;
import com.uber.hoodie.common.table.timeline.request.FileSliceDTO;
import com.uber.hoodie.common.table.timeline.request.InstantDTO;
import com.uber.hoodie.common.table.timeline.request.TimelineDTO;
import com.uber.hoodie.common.table.view.FileSystemViewManager;
import com.uber.hoodie.timeline.handlers.DataFileHandler;
import com.uber.hoodie.timeline.handlers.FileSliceHandler;
import com.uber.hoodie.timeline.handlers.TimelineHandler;
import io.javalin.Context;
import io.javalin.Handler;
import io.javalin.Javalin;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.jetbrains.annotations.NotNull;

/**
 * Main REST Handler class that handles local view staleness and delegates calls to slice/data-file/timeline handlers
 */
public class FileSystemViewHandler {
  private static Logger logger = LogManager.getLogger(FileSystemViewHandler.class);

  private final FileSystemViewManager viewManager;
  private final Javalin app;
  private final Configuration conf;
  private final TimelineHandler instantHandler;
  private final FileSliceHandler sliceHandler;
  private final DataFileHandler dataFileHandler;
  private final ObjectMapper mapper;

  public FileSystemViewHandler(Javalin app, Configuration conf, FileSystemViewManager viewManager) throws IOException {
    this.viewManager = viewManager;
    this.app = app;
    this.conf = conf;
    this.instantHandler = new TimelineHandler(conf, viewManager);
    this.sliceHandler = new FileSliceHandler(conf, viewManager);
    this.dataFileHandler = new DataFileHandler(conf, viewManager);
    this.mapper = new ObjectMapper();
  }

  public void register() {
    registerDataFilesAPI();
    registerFileSlicesAPI();
    registerTimelineAPI();
  }

  /**
   * Determines if local view of dataset's timeline is behind that of client's view
   * @param ctx
   * @return
   */
  private boolean isLocalViewBehind(Context ctx) {
    String basePath = ctx.queryParam(BASEPATH_PARAM);
    String lastKnownInstantFromClient = ctx.queryParam(LAST_INSTANT_TS, HoodieTimeline.INVALID_INSTANT.getTimestamp());
    String timelineHashFromClient = ctx.queryParam(TIMELINE_HASH, "");
    HoodieTimeline localTimeline = viewManager.getFileSystemView(basePath).getTimeline()
        .filterCompletedAndCompactionInstants();
    if (logger.isDebugEnabled()) {
      logger.debug("Client [ LastTs=" + lastKnownInstantFromClient
          + ", TimelineHash=" + timelineHashFromClient + "], localTimeline="
          + localTimeline.getInstants().collect(Collectors.toList()));
    }

    if ((localTimeline.getInstants().count() == 0)
        && lastKnownInstantFromClient.equals(HoodieTimeline.INVALID_INSTANT.getTimestamp())) {
      return false;
    }

    String localTimelineHash = localTimeline.getTimelineHash();

    return (!localTimelineHash.equals(timelineHashFromClient))
        && (!localTimeline.containsOrBeforeTimelineStarts(lastKnownInstantFromClient));
  }

  /**
   * Syncs data-set view if local view is behind
   * @param ctx
   */
  private boolean syncIfLocalViewBehind(Context ctx) {
    if (isLocalViewBehind(ctx)) {
      String basePath = ctx.queryParam(BASEPATH_PARAM);
      String lastKnownInstantFromClient =
          ctx.queryParam(LAST_INSTANT_TS, HoodieTimeline.INVALID_INSTANT.getTimestamp());

      HoodieTimeline localTimeline = viewManager.getFileSystemView(basePath).getTimeline();
      logger.warn("Syncing view as client passed last known instant " + lastKnownInstantFromClient
          + " as last known instant but server has the folling timeline :"
          + localTimeline.getInstants().collect(Collectors.toList()));
      viewManager.getFileSystemView(basePath).sync();
      return true;
    }
    return false;
  }

  private void writeValueAsString(Context ctx, Object obj) throws JsonProcessingException {
    boolean prettyPrint = ctx.queryParam("pretty") != null ? true : false;
    String result = prettyPrint ? mapper.writerWithDefaultPrettyPrinter().writeValueAsString(obj)
        : mapper.writeValueAsString(obj);
    ctx.result(result);
  }

  /**
   * Register Timeline API calls
   */
  private void registerTimelineAPI() {
    app.get(LAST_INSTANT, new ViewHandler(ctx -> {
      List<InstantDTO> dtos = instantHandler.getLastInstant(ctx.queryParam(BASEPATH_PARAM));
      writeValueAsString(ctx, dtos);
    }, false));

    app.get(TIMELINE, new ViewHandler(ctx -> {
      TimelineDTO dto = instantHandler.getTimeline(ctx.queryParam(BASEPATH_PARAM));
      writeValueAsString(ctx, dto);
    }, false));
  }

  /**
   * Register Data-Files API calls
   */
  private void registerDataFilesAPI() {
    app.get(LATEST_PARTITION_DATA_FILES_URL, new ViewHandler(ctx -> {
      List<DataFileDTO> dtos =
          dataFileHandler.getLatestDataFiles(ctx.queryParam(BASEPATH_PARAM), ctx.queryParam(PARTITION_PARAM));
      writeValueAsString(ctx, dtos);
    }, true));

    app.get(LATEST_PARTITION_DATA_FILE_URL, new ViewHandler(ctx -> {
      List<DataFileDTO> dtos =
          dataFileHandler.getLatestDataFile(ctx.queryParam(BASEPATH_PARAM), ctx.queryParam(PARTITION_PARAM),
              ctx.queryParam(FILEID_PARAM));
      writeValueAsString(ctx, dtos);
    }, true));

    app.get(LATEST_ALL_DATA_FILES, new ViewHandler(ctx -> {
      List<DataFileDTO> dtos =
          dataFileHandler.getLatestDataFiles(ctx.queryParam(BASEPATH_PARAM));
      writeValueAsString(ctx, dtos);
    }, true));

    app.get(LATEST_DATA_FILES_BEFORE_ON_INSTANT_URL, new ViewHandler(ctx -> {
      List<DataFileDTO> dtos =
          dataFileHandler.getLatestDataFilesBeforeOrOn(ctx.queryParam(BASEPATH_PARAM), ctx.queryParam(PARTITION_PARAM),
              ctx.queryParam(MAX_INSTANT_PARAM));
      writeValueAsString(ctx, dtos);
    }, true));

    app.get(LATEST_DATA_FILE_ON_INSTANT_URL, new ViewHandler(ctx -> {
      List<DataFileDTO> dtos =
          dataFileHandler.getLatestDataFileOn(ctx.queryParam(BASEPATH_PARAM), ctx.queryParam(PARTITION_PARAM),
              ctx.queryParam(INSTANT_PARAM), ctx.queryParam(FILEID_PARAM));
      writeValueAsString(ctx, dtos);
    }, true));

    app.get(ALL_DATA_FILES, new ViewHandler(ctx -> {
      List<DataFileDTO> dtos =
          dataFileHandler.getAllDataFiles(ctx.queryParam(BASEPATH_PARAM), ctx.queryParam(PARTITION_PARAM));
      writeValueAsString(ctx, dtos);
    }, true));

    app.get(LATEST_DATA_FILES_RANGE_INSTANT_URL, new ViewHandler(ctx -> {
      List<DataFileDTO> dtos =
          dataFileHandler.getLatestDataFilesInRange(ctx.queryParam(BASEPATH_PARAM),
              Arrays.asList(ctx.queryParam(INSTANTS_PARAM).split(",")));
      writeValueAsString(ctx, dtos);
    }, true));
  }

  /**
   * Register File Slices API calls
   */
  private void registerFileSlicesAPI() {
    app.get(LATEST_PARTITION_SLICES_URL, new ViewHandler(ctx -> {
      List<FileSliceDTO> dtos =
          sliceHandler.getLatestFileSlices(ctx.queryParam(BASEPATH_PARAM), ctx.queryParam(PARTITION_PARAM));
      writeValueAsString(ctx, dtos);
    }, true));

    app.get(LATEST_PARTITION_SLICE_URL, new ViewHandler(ctx -> {
      List<FileSliceDTO> dtos =
          sliceHandler.getLatestFileSlice(ctx.queryParam(BASEPATH_PARAM), ctx.queryParam(PARTITION_PARAM),
              ctx.queryParam(FILEID_PARAM));
      writeValueAsString(ctx, dtos);
    }, true));

    app.get(LATEST_PARTITION_UNCOMPACTED_SLICES_URL, new ViewHandler(ctx -> {
      List<FileSliceDTO> dtos =
          sliceHandler.getLatestUnCompactedFileSlices(ctx.queryParam(BASEPATH_PARAM), ctx.queryParam(PARTITION_PARAM));
      writeValueAsString(ctx, dtos);
    }, true));

    app.get(ALL_SLICES_URL, new ViewHandler(ctx -> {
      List<FileSliceDTO> dtos =
          sliceHandler.getAllFileSlices(ctx.queryParam(BASEPATH_PARAM), ctx.queryParam(PARTITION_PARAM));
      writeValueAsString(ctx, dtos);
    }, true));

    app.get(LATEST_SLICES_RANGE_INSTANT_URL, new ViewHandler(ctx -> {
      List<FileSliceDTO> dtos =
          sliceHandler.getLatestFileSliceInRange(ctx.queryParam(BASEPATH_PARAM),
              Arrays.asList(ctx.queryParam(INSTANTS_PARAM).split(",")));
      ctx.result(mapper.writeValueAsString(dtos));
    }, true));

    app.get(LATEST_SLICES_MERGED_BEFORE_ON_INSTANT_URL, new ViewHandler(ctx -> {
      List<FileSliceDTO> dtos =
          sliceHandler.getLatestMergedFileSlicesBeforeOrOn(ctx.queryParam(BASEPATH_PARAM),
              ctx.queryParam(PARTITION_PARAM), ctx.queryParam(MAX_INSTANT_PARAM));
      writeValueAsString(ctx, dtos);
    }, true));

    app.get(LATEST_SLICES_BEFORE_ON_INSTANT_URL, new ViewHandler(ctx -> {
      List<FileSliceDTO> dtos =
          sliceHandler.getLatestFileSlicesBeforeOrOn(ctx.queryParam(BASEPATH_PARAM), ctx.queryParam(PARTITION_PARAM),
              ctx.queryParam(MAX_INSTANT_PARAM));
      writeValueAsString(ctx, dtos);
    }, true));

    app.get(PENDING_COMPACTION_OPS, new ViewHandler(ctx -> {
      List<CompactionOpDTO> dtos = sliceHandler.getPendingCompactionOperations(ctx.queryParam(BASEPATH_PARAM));
      writeValueAsString(ctx, dtos);
    }, true));

    app.get(ALL_FILEGROUPS_FOR_PARTITION_URL, new ViewHandler(ctx -> {
      List<FileGroupDTO> dtos = sliceHandler.getAllFileGroups(ctx.queryParam(BASEPATH_PARAM),
          ctx.queryParam(PARTITION_PARAM));
      writeValueAsString(ctx, dtos);
    }, true));

    app.post(REFRESH_DATASET, new ViewHandler(ctx -> {
      boolean success = sliceHandler.refreshDataset(ctx.queryParam(BASEPATH_PARAM));
      writeValueAsString(ctx, success);
    }, false));
  }

  /**
   * Used for logging and performing refresh check.
   */
  private class ViewHandler implements Handler {

    private final Handler handler;
    private final boolean performRefreshCheck;

    ViewHandler(Handler handler, boolean performRefreshCheck) {
      this.handler = handler;
      this.performRefreshCheck = performRefreshCheck;
    }

    @Override
    public void handle(@NotNull Context context) throws Exception {
      boolean success = true;
      long beginTs = System.currentTimeMillis();
      boolean synced = false;
      try {

        if (performRefreshCheck) {
          synced = syncIfLocalViewBehind(context);
        }

        handler.handle(context);

        if (performRefreshCheck) {
          String errMsg = "Last known instant from client was "
              + context.queryParam(LAST_INSTANT_TS, HoodieTimeline.INVALID_INSTANT.getTimestamp())
              + " but server has the following timeline "
              +  viewManager.getFileSystemView(context.queryParam(BASEPATH_PARAM))
              .getTimeline().getInstants().collect(Collectors.toList());
          Preconditions.checkArgument(!isLocalViewBehind(context), errMsg);
        }
      } catch (RuntimeException re) {
        success = false;
        logger.error("Got runtime exception servicing request " + context.queryString(), re);
        throw re;
      } finally {
        long endTs = System.currentTimeMillis();
        long timeTakenMillis = endTs - beginTs;
        logger.info(String.format("TimeTakenMillis=%d, Success=%s, Query=%s, Host=%s, synced=%s", timeTakenMillis,
            success, context.queryString(), context.host(), synced));
      }
    }
  }
}
