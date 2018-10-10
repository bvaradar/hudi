/*
 *  Copyright (c) 2017 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
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

package com.uber.hoodie.utilities.sources;

import com.uber.hoodie.common.util.TypedProperties;
import com.uber.hoodie.common.util.collection.ImmutablePair;
import com.uber.hoodie.common.util.collection.Pair;
import com.uber.hoodie.utilities.schema.SchemaProvider;
import com.uber.hoodie.utilities.sources.helpers.KafkaOffsetGen;
import com.uber.hoodie.utilities.sources.helpers.KafkaOffsetGen.CheckpointUtils;
import io.confluent.kafka.serializers.KafkaAvroDecoder;
import java.util.Optional;
import kafka.serializer.StringDecoder;
import org.apache.avro.generic.GenericRecord;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.apache.spark.streaming.kafka.OffsetRange;

/**
 * Reads avro serialized Kafka data, based on the confluent schema-registry
 */
public class AvroKafkaSource extends AvroSource {

  private static volatile Logger log = LogManager.getLogger(AvroKafkaSource.class);

  private final KafkaOffsetGen offsetGen;

  public AvroKafkaSource(TypedProperties props, JavaSparkContext sparkContext, SchemaProvider schemaProvider) {
    super(props, sparkContext, schemaProvider);
    offsetGen = new KafkaOffsetGen(props);
  }

  @Override
  public Pair<Optional<JavaRDD<GenericRecord>>, String> fetchNewData(Optional<String> lastCheckpointStr,
      long sourceLimit) {
    OffsetRange[] offsetRanges = offsetGen.getNextOffsetRanges(lastCheckpointStr, sourceLimit);
    long totalNewMsgs = CheckpointUtils.totalNewMessages(offsetRanges);
    if (totalNewMsgs <= 0) {
      return new ImmutablePair<>(Optional.empty(), lastCheckpointStr.isPresent() ? lastCheckpointStr.get() : "");
    } else {
      log.info("About to read " + totalNewMsgs + " from Kafka for topic :" + offsetGen.getTopicName());
    }
    JavaRDD<GenericRecord> newDataRDD = toRDD(offsetRanges);
    return new ImmutablePair<>(Optional.of(newDataRDD), KafkaOffsetGen.CheckpointUtils.offsetsToStr(offsetRanges));
  }

  private JavaRDD<GenericRecord> toRDD(OffsetRange[] offsetRanges) {
    JavaRDD<GenericRecord> recordRDD = KafkaUtils
        .createRDD(sparkContext, String.class, Object.class, StringDecoder.class, KafkaAvroDecoder.class,
            offsetGen.getKafkaParams(), offsetRanges).values().map(obj -> (GenericRecord) obj);
    return recordRDD;
  }
}
