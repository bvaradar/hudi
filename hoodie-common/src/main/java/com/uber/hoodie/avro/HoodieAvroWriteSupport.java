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

package com.uber.hoodie.avro;

import com.uber.hoodie.common.BloomFilter;
import java.util.HashMap;
import org.apache.avro.Schema;
import org.apache.parquet.avro.AvroWriteSupport;
import org.apache.parquet.hadoop.api.WriteSupport;
import org.apache.parquet.schema.MessageType;

/**
 * Wrap AvroWriterSupport for plugging in the bloom filter.
 */
public class HoodieAvroWriteSupport extends AvroWriteSupport {

  private BloomFilter bloomFilter;
  private String minRecordKey;
  private String maxRecordKey;


  public static final String HOODIE_AVRO_BLOOM_FILTER_METADATA_KEY =
      "com.uber.hoodie.bloomfilter";
  public static final String HOODIE_MIN_RECORD_KEY_FOOTER = "hoodie_min_record_key";
  public static final String HOODIE_MAX_RECORD_KEY_FOOTER = "hoodie_max_record_key";


  public HoodieAvroWriteSupport(MessageType schema, Schema avroSchema, BloomFilter bloomFilter) {
    super(schema, avroSchema);
    this.bloomFilter = bloomFilter;
  }

  @Override
  public WriteSupport.FinalizedWriteContext finalizeWrite() {
    HashMap<String, String> extraMetaData = new HashMap<>();
    if (bloomFilter != null) {
      extraMetaData
          .put(HOODIE_AVRO_BLOOM_FILTER_METADATA_KEY, bloomFilter.serializeToString());
      if (minRecordKey != null && maxRecordKey != null) {
        extraMetaData.put(HOODIE_MIN_RECORD_KEY_FOOTER, minRecordKey);
        extraMetaData.put(HOODIE_MAX_RECORD_KEY_FOOTER, maxRecordKey);
      }
    }
    return new WriteSupport.FinalizedWriteContext(extraMetaData);
  }

  public void add(String recordKey) {
    if (bloomFilter != null) {
      this.bloomFilter.add(recordKey);
    }
    if (minRecordKey != null) {
      minRecordKey = minRecordKey.compareTo(recordKey) <= 0 ? minRecordKey : recordKey;
    } else {
      minRecordKey = recordKey;
    }

    if (maxRecordKey != null) {
      maxRecordKey = maxRecordKey.compareTo(recordKey) >= 0 ? maxRecordKey : recordKey;
    } else {
      maxRecordKey = recordKey;
    }
  }
}
