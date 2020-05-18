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

package org.apache.hudi.utilities.keygen;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import org.apache.hudi.DataSourceUtils;
import org.apache.hudi.DataSourceWriteOptions;
import org.apache.hudi.avro.HoodieAvroUtils;
import org.apache.hudi.common.config.TypedProperties;
import org.apache.hudi.common.model.HoodieKey;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieKeyException;
import org.apache.hudi.exception.HoodieNotSupportedException;
import org.apache.hudi.keygen.SimpleKeyGenerator;
import org.apache.hudi.utilities.exception.HoodieDeltaStreamerException;

import org.apache.avro.generic.GenericRecord;

import java.io.Serializable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.TimeZone;

/**
 * Key generator, that relies on timestamps for partitioning field. Still picks record key by name.
 */
public class TimestampBasedKeyGenerator extends SimpleKeyGenerator {

  enum TimestampType implements Serializable {
    UNIX_TIMESTAMP, DATE_STRING, MIXED, EPOCHMILLISECONDS
  }

  private final TimestampType timestampType;

  private SimpleDateFormat inputDateFormat;

  private final String outputDateFormat;

  // TimeZone detailed settings reference
  // https://docs.oracle.com/javase/8/docs/api/java/util/TimeZone.html
  private final TimeZone timeZone;

  protected final boolean encodePartitionPath;

  /**
   * Supported configs.
   */
  static class Config {

    // One value from TimestampType above
    private static final String TIMESTAMP_TYPE_FIELD_PROP = "hoodie.deltastreamer.keygen.timebased.timestamp.type";
    private static final String TIMESTAMP_INPUT_DATE_FORMAT_PROP =
        "hoodie.deltastreamer.keygen.timebased.input.dateformat";
    private static final String TIMESTAMP_OUTPUT_DATE_FORMAT_PROP =
        "hoodie.deltastreamer.keygen.timebased.output.dateformat";
    private static final String TIMESTAMP_TIMEZONE_FORMAT_PROP =
            "hoodie.deltastreamer.keygen.timebased.timezone";
  }

  public TimestampBasedKeyGenerator(TypedProperties config) {
    super(config);
    DataSourceUtils.checkRequiredProperties(config,
        Arrays.asList(Config.TIMESTAMP_TYPE_FIELD_PROP, Config.TIMESTAMP_OUTPUT_DATE_FORMAT_PROP));
    this.timestampType = TimestampType.valueOf(config.getString(Config.TIMESTAMP_TYPE_FIELD_PROP));
    this.outputDateFormat = config.getString(Config.TIMESTAMP_OUTPUT_DATE_FORMAT_PROP);
    this.timeZone = TimeZone.getTimeZone(config.getString(Config.TIMESTAMP_TIMEZONE_FORMAT_PROP, "GMT"));

    if (timestampType == TimestampType.DATE_STRING || timestampType == TimestampType.MIXED) {
      DataSourceUtils.checkRequiredProperties(config,
          Collections.singletonList(Config.TIMESTAMP_INPUT_DATE_FORMAT_PROP));
      this.inputDateFormat = new SimpleDateFormat(config.getString(Config.TIMESTAMP_INPUT_DATE_FORMAT_PROP));
      this.inputDateFormat.setTimeZone(timeZone);
    }

    this.encodePartitionPath = config.getBoolean(DataSourceWriteOptions.URL_ENCODE_PARTITIONING_OPT_KEY(),
        Boolean.parseBoolean(DataSourceWriteOptions.DEFAULT_URL_ENCODE_PARTITIONING_OPT_VAL()));
  }

  @Override
  public HoodieKey getKey(GenericRecord record) {
    Object partitionVal = HoodieAvroUtils.getNestedFieldVal(record, partitionPathField, true);
    if (partitionVal == null) {
      partitionVal = 1L;
    }
    SimpleDateFormat partitionPathFormat = new SimpleDateFormat(outputDateFormat);
    partitionPathFormat.setTimeZone(timeZone);

    try {
      long unixTime;
      if (partitionVal instanceof Double) {
        unixTime = ((Double) partitionVal).longValue();
      } else if (partitionVal instanceof Float) {
        unixTime = ((Float) partitionVal).longValue();
      } else if (partitionVal instanceof Long) {
        unixTime = (Long) partitionVal;
      } else if (partitionVal instanceof CharSequence) {
        unixTime = inputDateFormat.parse(partitionVal.toString()).getTime() / 1000;
      } else {
        throw new HoodieNotSupportedException(
          "Unexpected type for partition field: " + partitionVal.getClass().getName());
      }
      Date timestamp = this.timestampType == TimestampType.EPOCHMILLISECONDS ? new Date(unixTime) : new Date(unixTime * 1000);

      String recordKey = HoodieAvroUtils.getNestedFieldValAsString(record, recordKeyField, true);
      if (recordKey == null || recordKey.isEmpty()) {
        throw new HoodieKeyException("recordKey value: \"" + recordKey + "\" for field: \"" + recordKeyField + "\" cannot be null or empty.");
      }

      String partitionPath = partitionPathFormat.format(timestamp);
      if (encodePartitionPath) {
        try {
          partitionPath = URLEncoder.encode(partitionPath, StandardCharsets.UTF_8.toString());
        } catch (UnsupportedEncodingException uoe) {
          throw new HoodieException(uoe.getMessage(), uoe);
        }
      }

      partitionPath = hiveStylePartitioning ? partitionPathField + "=" + partitionPath : partitionPath;
      return new HoodieKey(recordKey, partitionPath);
    } catch (ParseException pe) {
      throw new HoodieDeltaStreamerException("Unable to parse input partition field :" + partitionVal, pe);
    }
  }
}
