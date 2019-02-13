package com.uber.hoodie.utilities.perf;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Snapshot;
import com.codahale.metrics.UniformReservoir;
import com.uber.hoodie.common.model.FileSlice;
import com.uber.hoodie.common.table.HoodieTableMetaClient;
import com.uber.hoodie.common.table.HoodieView;
import com.uber.hoodie.common.table.view.FileSystemViewStorageConfig;
import com.uber.hoodie.common.table.view.FileSystemViewStorageType;
import com.uber.hoodie.common.table.view.RemoteHoodieTableFileSystemView;
import com.uber.hoodie.common.util.FSUtils;
import com.uber.hoodie.timeline.TimelineServer;
import com.uber.hoodie.utilities.UtilHelpers;
import java.io.IOException;
import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Perf Benchmark to run FileSystem View calls at scale and monitor performance
 */
public class TimelineServerPerf {

  private static volatile Logger logger = LogManager.getLogger(TimelineServerPerf.class);
  private final Config cfg;
  private transient TimelineServer timelineServer;
  private String hostAddr;

  public TimelineServerPerf(Config cfg) throws IOException {
    this.cfg = cfg;
    this.timelineServer = new TimelineServer(cfg.getTimelinServerConfig());
  }

  private void setHostAddrFromSparkConf(SparkConf sparkConf) {
    String hostAddr = sparkConf.get("spark.driver.host", null);
    if (hostAddr != null) {
      logger.info("Overriding hostIp to (" + hostAddr + ") found in spark-conf. It was " + this.hostAddr);
      this.hostAddr = hostAddr;
    } else {
      logger.warn("Unable to find driver bind address from spark config");
    }
  }

  public void run() throws IOException {
    List<String> allPartitionPaths = FSUtils.getAllPartitionPaths(timelineServer.getFs(), cfg.basePath, true);
    Collections.shuffle(allPartitionPaths);
    this.timelineServer.startService();
    List<String> selected = allPartitionPaths.stream().limit(cfg.maxPartitions).collect(Collectors.toList());
    JavaSparkContext jsc = UtilHelpers.buildSparkContext("hudi-view-perf-" + cfg.basePath, cfg.sparkMaster);
    setHostAddrFromSparkConf(jsc.getConf());
    HoodieTableMetaClient metaClient = new HoodieTableMetaClient(timelineServer.getConf(), cfg.basePath, true);
    HoodieView fsView = new RemoteHoodieTableFileSystemView(this.hostAddr, cfg.serverPort, metaClient);
    System.out.println("First Iteration to load all partitions");
    dumpStats(runLookups(jsc, selected, fsView, 1));
    System.out.println("\n\n\n First Iteration is done");
    dumpStats(runLookups(jsc, selected, fsView, cfg.numIterations));

    System.out.println("\n\n\nDumping all File Slices");
    selected.stream().forEach(p -> fsView.getAllFileSlices(p).forEach(s -> System.out.println("\tMyFileSlice=" + s)));
  }

  public List<PerfStats> runLookups(JavaSparkContext jsc, List<String> partitionPaths, HoodieView fsView,
      int numIterations) {
    List<PerfStats> perfStats = jsc.parallelize(partitionPaths, cfg.parallelism).map(p -> {
      Histogram latencyHistogram = new Histogram(new UniformReservoir(10000));

      List<FileSlice> slices = null;
      for (int i = 0; i < numIterations; i++) {
        long beginTs = System.currentTimeMillis();
        slices = fsView.getLatestFileSlices(p).collect(Collectors.toList());
        long endTs = System.currentTimeMillis();
        System.out.println("Latest File Slices for part=" + p + ", count=" + slices.size()
            + ", Time=" + (endTs - beginTs));
        latencyHistogram.update(endTs - beginTs);
      }
      System.out.println("SLICES are=");
      slices.stream().forEach(s -> {
        System.out.println("\t\tFileSlice=" + s);
      });
      return new PerfStats(p, latencyHistogram.getSnapshot());
    }).collect();
    return perfStats;
  }

  public void dumpStats(List<PerfStats> p) {
    p.stream().forEach(x -> {
      System.out.println(String.format("%s %d %d %f %f %f %f", x.partition, x.minTime, x.maxTime, x.meanTime,
          x.medianTime, x.p75, x.p95));
    });
  }

  private static class PerfStats implements Serializable {

    private final String partition;
    private final long minTime;
    private final long maxTime;
    private final double meanTime;
    private final double medianTime;
    private final double p95;
    private final double p75;

    public PerfStats(String partition, Snapshot s) {
      this(partition, s.getMin(), s.getMax(), s.getMean(), s.getMedian(), s.get95thPercentile(), s.get75thPercentile());
    }

    public PerfStats(String partition, long minTime, long maxTime, double meanTime, double medianTime, double p95,
        double p75) {
      this.partition = partition;
      this.minTime = minTime;
      this.maxTime = maxTime;
      this.meanTime = meanTime;
      this.medianTime = medianTime;
      this.p95 = p95;
      this.p75 = p75;
    }
  }

  public static class Config implements Serializable {

    @Parameter(names = {"--base-path", "-b"}, description = "Base Path", required = true)
    public String basePath = "";

    @Parameter(names = {"--max-partitions", "-m"}, description = "Mx partitions to be loaded")
    public Integer maxPartitions = 1000;

    @Parameter(names = {"--parallelism", "-e"}, description = "parallelism")
    public Integer parallelism = 10;

    @Parameter(names = {"--num-iterations", "-i"}, description = "Number of iterations for each partitions")
    public Integer numIterations = 10;

    @Parameter(names = {"--spark-master", "-ms"}, description = "Spark master", required = false)
    public String sparkMaster = "local[2]";

    @Parameter(names = {"--server-port", "-p"}, description = " Server Port")
    public Integer serverPort = 26754;

    @Parameter(names = {"--view-storage", "-st"}, description = "View Storage Type. Defaut - SPILLABLE_DISK")
    public FileSystemViewStorageType viewStorageType = FileSystemViewStorageType.SPILLABLE_DISK;

    @Parameter(names = {"--max-view-mem-per-table", "-mv"},
        description = "Maximum view memory per table in MB to be used for storing file-groups."
            + " Overflow file-groups will be spilled to disk. Used for SPILLABLE_DISK storage type")
    public Integer maxViewMemPerTableInMB = 2048;

    @Parameter(names = {"--mem-overhead-fraction-pending-compaction", "-cf"},
        description = "Memory Fraction of --max-view-mem-per-table to be allocated for managing pending compaction"
            + " storage. Overflow entries will be spilled to disk. Used for SPILLABLE_DISK storage type")
    public Double memFractionForCompactionPerTable = 0.001;

    @Parameter(names = {"--base-store-path", "-sp"},
        description = "Directory where spilled view entries will be stored. Used for SPILLABLE_DISK storage type")
    public String baseStorePathForFileGroups = FileSystemViewStorageConfig.DEFAULT_VIEW_SPILLABLE_DIR;

    @Parameter(names = {"--rocksdb-path", "-rp"},
        description = "Root directory for RocksDB")
    public String rocksDBPath = FileSystemViewStorageConfig.DEFAULT_ROCKSDB_BASE_PATH_PROP;

    @Parameter(names = {"--reset-rocksdb-path", "-rr"},
        description = "reset root directory for RocksDB")
    public Boolean resetRocksDBOnInstantiation = false;

    @Parameter(names = {"--help", "-h"})
    public Boolean help = false;

    public TimelineServer.Config getTimelinServerConfig() {
      TimelineServer.Config c = new TimelineServer.Config();
      c.viewStorageType = viewStorageType;
      c.baseStorePathForFileGroups = baseStorePathForFileGroups;
      c.maxViewMemPerTableInMB = maxViewMemPerTableInMB;
      c.memFractionForCompactionPerTable = memFractionForCompactionPerTable;
      c.resetRocksDBOnInstantiation = resetRocksDBOnInstantiation;
      c.rocksDBPath = rocksDBPath;
      c.serverPort = serverPort;
      return c;
    }
  }

  public static void main(String[] args) throws Exception {
    final Config cfg = new Config();
    JCommander cmd = new JCommander(cfg, args);
    if (cfg.help || args.length == 0) {
      cmd.usage();
      System.exit(1);
    }
    TimelineServerPerf perf = new TimelineServerPerf(cfg);
    perf.run();
  }
}
