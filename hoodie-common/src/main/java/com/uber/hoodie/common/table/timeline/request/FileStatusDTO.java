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

package com.uber.hoodie.common.table.timeline.request;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.uber.hoodie.exception.HoodieException;
import java.io.IOException;
import org.apache.hadoop.fs.FileStatus;

@JsonIgnoreProperties(ignoreUnknown = true)
public class FileStatusDTO {

  @JsonProperty("path")
  FilePathDTO path;
  @JsonProperty("length")
  long length;
  @JsonProperty("isdir")
  boolean isdir;
  @JsonProperty("blockReplication")
  short blockReplication;
  @JsonProperty("blocksize")
  long blocksize;
  @JsonProperty("modificationTime")
  long modificationTime;
  @JsonProperty("accessTime")
  long accessTime;
  @JsonProperty("permission")
  FSPermissionDTO permission;
  @JsonProperty("owner")
  String owner;
  @JsonProperty("group")
  String group;
  @JsonProperty("symlink")
  FilePathDTO symlink;

  public static FileStatusDTO fromFileStatus(FileStatus fileStatus) {
    if (null == fileStatus) {
      return null;
    }

    FileStatusDTO dto = new FileStatusDTO();
    try {
      dto.path = FilePathDTO.fromPath(fileStatus.getPath());
      dto.length = fileStatus.getLen();
      dto.isdir = fileStatus.isDirectory();
      dto.blockReplication = fileStatus.getReplication();
      dto.blocksize = fileStatus.getBlockSize();
      dto.modificationTime = fileStatus.getModificationTime();
      dto.accessTime = fileStatus.getModificationTime();
      dto.permission = FSPermissionDTO.fromFsPermission(fileStatus.getPermission());
      dto.owner = fileStatus.getOwner();
      dto.group = fileStatus.getOwner();
      dto.symlink = fileStatus.isSymlink() ? FilePathDTO.fromPath(fileStatus.getSymlink()) : null;
    } catch (IOException ioe) {
      throw new HoodieException(ioe);
    }
    return dto;
  }

  public static FileStatus toFileStatus(FileStatusDTO dto) {
    if (null == dto) {
      return null;
    }

    return new FileStatus(dto.length, dto.isdir, dto.blockReplication, dto.blocksize, dto.modificationTime,
        dto.accessTime, FSPermissionDTO.fromFsPermissionDTO(dto.permission), dto.owner, dto.group,
        FilePathDTO.toPath(dto.symlink), FilePathDTO.toPath(dto.path));
  }
}
