package com.uber.hoodie.utilities.sources;

import com.uber.hoodie.DataSourceReadOptions;
import com.uber.hoodie.DataSourceUtils;
import com.uber.hoodie.common.model.HoodieRecord;
import com.uber.hoodie.common.util.TypedProperties;
import com.uber.hoodie.common.util.collection.Pair;
import com.uber.hoodie.utilities.schema.SchemaProvider;
import com.uber.hoodie.utilities.sources.helpers.IncrSourceHelper;
import java.util.Arrays;
import java.util.Optional;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class HoodieIncrSource extends RowSource {

  /**
   * Configs supported
   */
  protected static class Config {

    private static final String HUDI_SRC_BASE_PATH = "hoodie.deltastreamer.source.hoodie.path";

    private static final String NUM_INSTANTS_PER_FETCH = "hoodie.deltastreamer.source.hoodie.num_instants";
    private static final Integer DEFAULT_NUM_INSTANTS_PER_FETCH = -1;

    private static final String READ_LATEST_INSTANT_ON_MISSING_CKPT =
        "hoodie.deltastreamer.source.hoodie.read_latest_on_midding_ckpt";
    private static final Boolean DEFAULT_READ_LATEST_INSTANT_ON_MISSING_CKPT = false;
  }

  public HoodieIncrSource(TypedProperties props,
      JavaSparkContext sparkContext,
      SchemaProvider schemaProvider) {
    super(props, sparkContext, schemaProvider);
  }

  @Override
  public Pair<Optional<Dataset<Row>>, String> fetchNewData(Optional<String> lastCkptStr, long sourceLimit) {
    DataSourceUtils.checkRequiredProperties(props, Arrays.asList(Config.HUDI_SRC_BASE_PATH));
    String srcPath = props.getString(Config.HUDI_SRC_BASE_PATH);
    int numInstantsPerFetch = props.getInteger(Config.NUM_INSTANTS_PER_FETCH, Config.DEFAULT_NUM_INSTANTS_PER_FETCH);
    boolean readLatestOnMissingCkpt = props.getBoolean(Config.READ_LATEST_INSTANT_ON_MISSING_CKPT,
        Config.DEFAULT_READ_LATEST_INSTANT_ON_MISSING_CKPT);

    // Use begin Instant if set and non-empty
    Optional<String> beginInstant =
        lastCkptStr.isPresent() ? lastCkptStr.get().isEmpty() ? Optional.empty() : lastCkptStr : Optional.empty();

    Pair<String, Optional<String>> instantEndpts = IncrSourceHelper.calculateBeginAndEndInstants(sparkContext, srcPath,
        numInstantsPerFetch, beginInstant, readLatestOnMissingCkpt);

    // Do Incr pull. Set end instant if available
    DataFrameReader reader = sparkSession.read().format("com.uber.hoodie")
        .option(DataSourceReadOptions.VIEW_TYPE_OPT_KEY(), DataSourceReadOptions.VIEW_TYPE_INCREMENTAL_OPT_VAL())
        .option(DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY(), instantEndpts.getLeft());
    if (instantEndpts.getRight().isPresent()) {
      reader = reader.option(DataSourceReadOptions.END_INSTANTTIME_OPT_KEY(), instantEndpts.getRight().get());
    }

    Dataset<Row> source = reader.load(srcPath);
    Optional<String> maxSeenInstant =
        IncrSourceHelper.validateAndGetMaxSeenInstant(source, instantEndpts.getLeft(),
            instantEndpts.getRight().orElse(null));

    // Remove Hudi meta columns from input source
    final Dataset<Row> src = source.drop(HoodieRecord.HOODIE_META_COLUMNS.toArray(new String[0]));

    return maxSeenInstant.map(s -> Pair.of(Optional.of(src), s)).orElseGet(() -> Pair.of(Optional.of(src), null));
  }
}
