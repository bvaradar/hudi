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
package org.apache.hudi

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql.SaveMode.Append
import org.apache.spark.sql.SaveMode.Overwrite
import org.apache.spark.sql.SparkSession


object  DagMirror {

  val user = sys.env("USER")
  val tableName = "test_table"
  val basePath = "/tmp/varadarb/hudi-test-suite/output_051"
  val inputPath = "/tmp/modi/hudi-test-suite/input/"
  var mode = Overwrite

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.getOrCreate()
    val hdfs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

    hdfs.delete(new Path(basePath), true)

    val statuses = hdfs.listStatus(new Path(inputPath))
    statuses.map(x => upsert(x.getPath.toString, spark))
  }

  def upsert(path: String, spark: SparkSession): Unit = {
    val opts = hudiWriteOpts(spark.sparkContext.applicationId)
    println("READING INPUT FROM " + path)
    val inputDF = spark.read.format("avro").load(path + "/*")
    inputDF.write.format("org.apache.hudi").options(opts).mode(mode).save(basePath)
    mode = Append
  }

  def hudiWriteOpts(applicationID: String): Map[String, String] = {
    val opts = Map(
      "hoodie.table.name" -> (tableName),
      "hoodie.datasource.write.precombine.field" -> "timestamp",
      "hoodie.datasource.write.recordkey.field" -> "_row_key",
      "hoodie.datasource.write.partitionpath.field" -> "timestamp",
      "hoodie.datasource.hive_sync.partition_fields" -> "_hoodie_partition_path",
      "hoodie.metrics.on" -> "true",
      "hoodie.metrics.graphite.metric.prefix" -> ("stats.dca1.gauges.hoodie.dev." + user +"_053"),
      "hoodie.datasource.write.keygenerator.class" -> "org.apache.hudi.utilities.keygen.TimestampBasedKeyGenerator",
      "hoodie.deltastreamer.keygen.timebased.timestamp.type" -> "UNIX_TIMESTAMP",
      "hoodie.deltastreamer.keygen.timebased.output.dateformat" -> "yyyy/MM/dd",
      "hoodie.embed.timeline.server" -> "true"
    )
    opts
  }

}
