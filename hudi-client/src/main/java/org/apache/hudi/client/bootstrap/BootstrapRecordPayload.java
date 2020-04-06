package org.apache.hudi.client.bootstrap;

import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.common.util.Option;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.IndexedRecord;

import java.io.IOException;


public class BootstrapRecordPayload implements HoodieRecordPayload<BootstrapRecordPayload> {

  private final GenericRecord record;

  public BootstrapRecordPayload(GenericRecord record) {
    this.record = record;
  }

  @Override
  public BootstrapRecordPayload preCombine(BootstrapRecordPayload another) {
    return this;
  }

  @Override
  public Option<IndexedRecord> combineAndGetUpdateValue(IndexedRecord currentValue, Schema schema)
      throws IOException {
    return Option.ofNullable(record);
  }

  @Override
  public Option<IndexedRecord> getInsertValue(Schema schema) throws IOException {
    return Option.ofNullable(record);
  }

}
