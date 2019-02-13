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

package com.uber.hoodie.common.util;

import com.google.common.base.Preconditions;
import com.uber.hoodie.common.table.view.FileSystemViewStorageConfig;
import com.uber.hoodie.common.util.collection.Pair;
import com.uber.hoodie.exception.HoodieException;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.apache.commons.io.FileUtils;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

/**
 * Data access objects for storing and retrieving objects in Rocks DB.
 */
public class RocksDBDAO {
  protected static final transient Logger log = LogManager.getLogger(RocksDBDAO.class);

  private final FileSystemViewStorageConfig config;
  private final ConcurrentHashMap<String, ColumnFamilyHandle> managedHandlesMap = new ConcurrentHashMap<>();
  private final ConcurrentHashMap<String, ColumnFamilyDescriptor> managedDescriptorMap = new ConcurrentHashMap<>();

  private RocksDB rocksDB;
  private boolean closed = false;

  public RocksDBDAO(FileSystemViewStorageConfig config) {
    this.config = config;
    init();
  }

  /**
   * Initialized Rocks DB instance
   * @throws HoodieException
   */
  private void init() throws HoodieException {
    try {
      if (config.isResetOnInstantiation()) {
        log.warn("DELETING RocksDB persisted at " + config.getRocksdbBasePath());
        FileUtils.deleteDirectory(new File(config.getRocksdbBasePath()));
      }

      // If already present, loads the existing column-family handles
      final DBOptions dbOptions = new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true)
          .setWalDir(config.getRocksdbBasePath());
      final List<ColumnFamilyDescriptor> managedColumnFamilies = loadManagedColumnFamilies(dbOptions);
      final List<ColumnFamilyHandle> managedHandles = new ArrayList<>();
      rocksDB = RocksDB.open(dbOptions, config.getRocksdbBasePath(), managedColumnFamilies, managedHandles);
      Preconditions.checkArgument(managedHandles.size() == managedColumnFamilies.size(),
          "Unexpected number of handles are returned");
      for (int index = 0; index < managedHandles.size(); index++) {
        ColumnFamilyHandle handle = managedHandles.get(index);
        ColumnFamilyDescriptor descriptor = managedColumnFamilies.get(index);
        String familyNameFromHandle = new String(handle.getName());
        String familyNameFromDescriptor = new String(descriptor.getName());

        Preconditions.checkArgument(familyNameFromDescriptor.equals(familyNameFromHandle),
            "Family Handles not in order with descriptors");
        managedHandlesMap.put(familyNameFromHandle, handle);
        managedDescriptorMap.put(familyNameFromDescriptor, descriptor);
      }
    } catch (RocksDBException | IOException re) {
      log.error("Got exception opening rocks db instance ", re);
      throw new HoodieException(re);
    }
  }

  /**
   * Helper to load managed column family descriptors
   * @param dbOptions
   * @return
   * @throws RocksDBException
   */
  private List<ColumnFamilyDescriptor> loadManagedColumnFamilies(DBOptions dbOptions) throws RocksDBException {
    final List<ColumnFamilyDescriptor> managedColumnFamilies = new ArrayList<>();
    final Options options = new Options(dbOptions, new ColumnFamilyOptions());
    List<byte[]> existing = RocksDB.listColumnFamilies(options, config.getRocksdbBasePath());

    if (existing.isEmpty()) {
      log.info("No column family found. Loading default");
      managedColumnFamilies.add(getColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY));
    } else {
      log.info("Loading column families :" + existing.stream().map(String::new).collect(Collectors.toList()));
      managedColumnFamilies.addAll(existing.stream()
          .map(RocksDBDAO::getColumnFamilyDescriptor).collect(Collectors.toList()));
    }
    return managedColumnFamilies;
  }

  private static ColumnFamilyDescriptor getColumnFamilyDescriptor(byte[] columnFamilyName) {
    return new ColumnFamilyDescriptor(columnFamilyName, new ColumnFamilyOptions());
  }

  /**
   * Perform a batch write operation
   * @param handler
   */
  public void writeBatch(BatchHandler handler) {
    try {
      WriteBatch batch = new WriteBatch();
      handler.apply(batch);
      rocksDB.write(new WriteOptions(), batch);
    } catch (RocksDBException re) {
      throw new HoodieException(re);
    }
  }

  /**
   * Helper to add put operation in batch
   * @param batch  Batch Handle
   * @param columnFamilyName  Column Family
   * @param key  Key
   * @param value  Payload
   * @param <T>  Type of payload
   */
  public <T extends Serializable> void putInBatch(WriteBatch batch, String columnFamilyName, String key, T value) {
    byte[] payload = SerializationUtils.serialize(value);
    try {
      batch.put(managedHandlesMap.get(columnFamilyName), key.getBytes(), payload);
    } catch (RocksDBException e) {
      throw new HoodieException(e);
    }
  }

  /**
   * Perform single PUT on a column-family
   * @param columnFamilyName Column family name
   * @param key  Key
   * @param value  Payload
   * @param <T> Type of Payload
   */
  public <T extends Serializable> void put(String columnFamilyName, String key, T value) {
    byte[] payload = SerializationUtils.serialize(value);
    try {
      rocksDB.put(managedHandlesMap.get(columnFamilyName), key.getBytes(), payload);
    } catch (RocksDBException e) {
      throw new HoodieException(e);
    }
  }

  /**
   * Helper to add delete operation in batch
   * @param batch  Batch Handle
   * @param columnFamilyName  Column Family
   * @param key  Key
   */
  public void deleteInBatch(WriteBatch batch, String columnFamilyName, String key) {
    try {
      batch.delete(managedHandlesMap.get(columnFamilyName), key.getBytes());
    } catch (RocksDBException e) {
      throw new HoodieException(e);
    }
  }

  /**
   * Perform a single Delete operation
   * @param columnFamilyName Column Family name
   * @param key Key to be deleted
   */
  public void delete(String columnFamilyName, String key) {
    try {
      rocksDB.delete(managedHandlesMap.get(columnFamilyName), key.getBytes());
    } catch (RocksDBException e) {
      throw new HoodieException(e);
    }
  }

  /**
   * Retrieve a value for a given key in a column family
   * @param columnFamilyName Column Family Name
   * @param key Key to be retrieved
   * @param <T> Type of object stored.
   * @return
   */
  public <T extends Serializable> T get(String columnFamilyName, String key) {
    Preconditions.checkArgument(!closed);
    try {
      byte[] val = rocksDB.get(managedHandlesMap.get(columnFamilyName), key.getBytes());
      return val == null ? null : SerializationUtils.deserialize(val);
    } catch (RocksDBException e) {
      throw new HoodieException(e);
    }
  }

  /**
   * Perform a prefix search and return stream of key-value pairs retrieved
   * @param columnFamilyName Column Family Name
   * @param prefix Prefix Key
   * @param <T> Type of value stored
   * @return
   */
  public <T extends Serializable> Stream<Pair<String, T>> prefixSearch(String columnFamilyName, String prefix) {
    Preconditions.checkArgument(!closed);
    log.info("Prefix Search (query=" + prefix + ") on " + columnFamilyName);
    final RocksIterator it = rocksDB.newIterator(managedHandlesMap.get(columnFamilyName));
    it.seek(prefix.getBytes());
    Iterator<Pair<String, T>> iterator = new Iterator<Pair<String, T>>() {
      @Override
      public boolean hasNext() {
        return it.isValid() && new String(it.key()).startsWith(prefix);
      }

      @Override
      public Pair<String, T> next() {
        Pair<String, T> result = Pair.of(new String(it.key()), SerializationUtils.deserialize(it.value()));
        it.next();
        return result;
      }
    };
    Iterable<Pair<String, T>> iterable = () -> iterator;
    return StreamSupport.stream(iterable.spliterator(), false).onClose(() -> it.close());
  }

  /**
   * Perform a prefix delete and return stream of key-value pairs retrieved
   * @param columnFamilyName Column Family Name
   * @param prefix Prefix Key
   * @param <T> Type of value stored
   * @return
   */
  public <T extends Serializable> void prefixDelete(String columnFamilyName, String prefix) {
    Preconditions.checkArgument(!closed);
    log.info("Prefix DELETE (query=" + prefix + ") on " + columnFamilyName);
    final RocksIterator it = rocksDB.newIterator(managedHandlesMap.get(columnFamilyName));
    it.seek(prefix.getBytes());
    //Find first and last keys to be deleted
    String firstEntry = null;
    String lastEntry = null;
    while (it.isValid() && new String(it.key()).startsWith(prefix)) {
      String result = new String(it.key());
      it.next();
      if (firstEntry == null) {
        firstEntry = result;
      }
      lastEntry = result;
    }
    it.close();

    if (null != firstEntry) {
      try {
        // This will not delete the last entry
        rocksDB.deleteRange(managedHandlesMap.get(columnFamilyName), firstEntry.getBytes(),
            lastEntry.getBytes());
        //Delete the last entry
        rocksDB.delete(lastEntry.getBytes());
      } catch (RocksDBException e) {
        log.error("Got exception performing range delete");
        throw new HoodieException(e);
      }
    }
  }

  /**
   * Add a new column family to store
   * @param columnFamilyName Column family name
   */
  public void addColumnFamily(String columnFamilyName) {
    Preconditions.checkArgument(!closed);

    managedDescriptorMap.computeIfAbsent(columnFamilyName, colFamilyName -> {
      try {
        ColumnFamilyDescriptor descriptor = getColumnFamilyDescriptor(colFamilyName.getBytes());
        ColumnFamilyHandle handle = rocksDB.createColumnFamily(descriptor);
        managedHandlesMap.put(colFamilyName, handle);
        return descriptor;
      } catch (RocksDBException e) {
        throw new HoodieException(e);
      }
    });
  }

  /**
   * Note : Does not delete from underlying DB. Just closes the handle
   * @param columnFamilyName Column Family Name
   */
  public void dropColumnFamily(String columnFamilyName) {
    Preconditions.checkArgument(!closed);

    managedDescriptorMap.computeIfPresent(columnFamilyName, (colFamilyName, descriptor) -> {
      ColumnFamilyHandle handle = managedHandlesMap.get(colFamilyName);
      try {
        rocksDB.dropColumnFamily(handle);
        handle.close();
      } catch (RocksDBException e) {
        throw new HoodieException(e);
      }
      managedHandlesMap.remove(columnFamilyName);
      return null;
    });
  }

  /**
   * Close the DAO object
   */
  public synchronized void close() {
    if (!closed) {
      closed = true;
      managedHandlesMap.values().forEach(columnFamilyHandle -> {
        columnFamilyHandle.close();
      });
      managedHandlesMap.clear();
      managedDescriptorMap.clear();
      rocksDB.close();
    }
  }

  /**
   * Functional interface for stacking operation to Write batch
   */
  public interface BatchHandler {
    void apply(WriteBatch batch);
  }
}
