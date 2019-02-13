/*
 *  Copyright (c) 2016 Uber Technologies, Inc. (hoodie-dev-group@uber.com)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *           http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.uber.hoodie.common.table.view;

import org.apache.commons.io.Charsets;
import org.rocksdb.Options;
import org.rocksdb.RocksIterator;
import org.rocksdb.TtlDB;

public class HelloWorldRocksDB {

  public static void main(String[] args) throws Exception {
    TtlDB.loadLibrary();
    Options options = new Options()
        .setCreateIfMissing(true);

    String tableName = "timeline_vbalaji_table";
    String loc = "/tmp/rocks.db";
    String key1 = "partition=1,fileId=vbalaji1";
    String key2 = "partition=1,fileId=vbalaji2";
    String val1 = "{\"name\":\"vbalaji\"}";
    TtlDB db = TtlDB.open(options, loc, 20, false);

    //ColumnFamilyHandle handle = db.createColumnFamily(new ColumnFamilyDescriptor(tableName.getBytes()));
    db.put(key1.getBytes(), val1.getBytes());
    db.put(key2.getBytes(), val1.getBytes());

    byte[] arr = db.get(key1.getBytes());
    if (arr != null) {
      System.out.println("Val 1 :" + new String(arr));
    } else {
      System.out.println(arr);
    }

    RocksIterator it = db.newIterator();
    String prefix = "partition=1";
    it.seek(prefix.getBytes());
    for (; it.isValid() && new String(it.key()).startsWith(prefix); it.next()) {
      String[] key = new String(it.key(), Charsets.UTF_8).split(",");
      String partition = key[0];
      String fileId = key[1];
      String value = new String(it.value(), Charsets.UTF_8);
      System.out.format("VALS ARE %s, %s, Value=%s%n", partition, fileId, value);
    }
    db.close();
  }
}
