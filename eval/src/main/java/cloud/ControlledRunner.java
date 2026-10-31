package cloud;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.execution.ExplainMode;
import org.apache.spark.util.SizeEstimator;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Consumer;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.input_file_name;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.sum;

/**
 * Replays a saved trace against one method in one JVM. Same jar locally and on a cluster.
 * This is NOT the original benchmark: see docs/CHANGES.md for every difference.
 */
public final class ControlledRunner {

    enum Method {
        NO_CACHE, NAIVE_CACHE, PCC, PCC_INLINE, EXACT_FILE;

        /** Containment lookup, as in the paper. */
        boolean containment() { return this == PCC || this == PCC_INLINE; }

        /** Stores coverage, so each scan must also learn which files matched. */
        boolean cachesCoverage() { return containment() || this == EXACT_FILE; }

        /** Learns coverage in the same scan. PCC keeps the original's separate second scan. */
        boolean inlineCoverage() { return this == PCC_INLINE || this == EXACT_FILE; }
    }

    static final class Config {
        Method method;
        String data;
        long capacityBytes = -1;
        double w1 = 1.0;
        double w2 = 0.0;
        double coverageThresholdFraction = 0.5;
    }

    public static void main(String[] args) throws Exception {
        Map<String, String> a = parseArgs(args);
        Config c = new Config();
        c.method = Method.valueOf(a.get("method"));
        c.data = a.get("data");
        c.capacityBytes = Long.parseLong(a.getOrDefault("capacity-bytes", "-1"));
        c.w1 = Double.parseDouble(a.getOrDefault("w1", "1.0"));
        c.w2 = Double.parseDouble(a.getOrDefault("w2", "0.0"));
        c.coverageThresholdFraction = Double.parseDouble(a.getOrDefault("coverage-threshold", "0.5"));
        String out = a.get("out");

        SparkSession spark = SparkSession.builder().appName("pcc-" + c.method).getOrCreate();
        Configuration hadoop = spark.sparkContext().hadoopConfiguration();
        Map<String, Object> runInfo = new LinkedHashMap<>();
        runInfo.put("args", new TreeMap<>(a));
        runInfo.put("capacity_bytes", c.capacityBytes);
        runInfo.put("w1", c.w1);
        runInfo.put("w2", c.w2);
        runInfo.put("coverage_threshold_fraction", c.coverageThresholdFraction);
        runInfo.put("status", "started");
        runInfo.put("started_epoch_ms", System.currentTimeMillis());
        runInfo.put("spark_version", spark.version());
        runInfo.put("java_version", System.getProperty("java.version"));
        runInfo.put("hadoop_version", org.apache.hadoop.util.VersionInfo.getVersion());
        runInfo.put("spark_default_parallelism", spark.sparkContext().defaultParallelism());
        writeJson(out + "/run.json", runInfo, hadoop);

        ObjectMapper mapper = new ObjectMapper();
        Path rowsPath = new Path(out + "/queries.jsonl");
        int exit = 0;
        // rows go out one by one, so a timeout loses at most the query in flight
        try (FSDataOutputStream rows = rowsPath.getFileSystem(hadoop).create(rowsPath, true)) {
            List<String> lines = readLines(a.get("trace"), hadoop);
            runInfo.put("trace_sha256", sha256(String.join("\n", lines) + "\n"));
            run(spark, c, TraceQuery.parse(lines), row -> {
                try {
                    rows.write((mapper.writeValueAsString(row) + "\n").getBytes(StandardCharsets.UTF_8));
                    rows.hflush();
                } catch (IOException e) {
                    throw new IllegalStateException(e);
                }
            }, runInfo);
            runInfo.put("status", "completed");
        } catch (Throwable e) {
            // failures are results too
            runInfo.put("status", "failed");
            runInfo.put("error", e.toString());
            exit = 1;
        } finally {
            runInfo.put("finished_epoch_ms", System.currentTimeMillis());
            writeJson(out + "/run.json", runInfo, hadoop);
            spark.stop();
        }
        if (exit != 0) System.exit(exit);
    }

    /** Copied from the private Benchmark.getQueryCondition. */
    static Column queryCondition(TraceQuery q) {
        return col("l_shipdate").geq(q.shipFrom).and(col("l_shipdate").leq(q.shipTo))
                .and(col("l_discount").geq(q.discountFrom)).and(col("l_discount").leq(q.discountTo))
                .and(col("l_quantity").geq(q.quantityFrom)).and(col("l_quantity").leq(q.quantityTo));
    }

    /** The query's result. Q6 is one global row. Q1 groups by flag and status. byFile adds the file, which gives coverage in the same scan. */
    static Dataset<Row> aggregate(Dataset<Row> filtered, TraceQuery q, boolean byFile) {
        List<Column> keys = new ArrayList<>();
        if (byFile) keys.add(input_file_name().as("pcc_file"));
        List<Column> values = new ArrayList<>();
        values.add(sum(col("l_extendedprice").multiply(col("l_discount"))).as("pcc_sum"));
        if (q.isQ1()) {
            keys.add(col("l_returnflag"));
            keys.add(col("l_linestatus"));
            values.add(sum("l_quantity"));
            values.add(sum("l_extendedprice"));
            values.add(avg("l_quantity"));
            values.add(avg("l_extendedprice"));
            values.add(avg("l_discount"));
        }
        Column[] rest = values.toArray(new Column[0]);
        return keys.isEmpty() ? filtered.agg(count(lit(1)).as("pcc_n"), rest)
                : filtered.groupBy(keys.toArray(new Column[0])).agg(count(lit(1)).as("pcc_n"), rest);
    }

    private static String encodeGroups(Map<String, double[]> groups) {
        StringBuilder out = new StringBuilder();
        for (Map.Entry<String, double[]> g : groups.entrySet()) {
            out.append(g.getKey()).append(':').append(g.getValue()[0]).append(':').append(g.getValue()[1]).append(';');
        }
        return out.toString();
    }

    private static Map<String, double[]> decodeGroups(String text) {
        Map<String, double[]> groups = new TreeMap<>();
        for (String g : text.split(";")) {
            if (g.isEmpty()) continue;
            String[] p = g.split(":");
            groups.put(p[0], new double[]{Double.parseDouble(p[1]), Double.parseDouble(p[2])});
        }
        return groups;
    }

    static String coverageHash(List<String> files) {
        List<String> names = new ArrayList<>();
        for (String f : files) names.add(f.substring(f.lastIndexOf('/') + 1));
        Collections.sort(names);
        return sha256(String.join("\n", names));
    }

    static void run(SparkSession spark, Config c, List<TraceQuery> trace,
                    Consumer<Map<String, Object>> sink, Map<String, Object> runInfo) {
        PhaseMetrics metrics = new PhaseMetrics();
        spark.sparkContext().addSparkListener(metrics);

        Dataset<Row> all = spark.read().parquet(c.data);
        String[] allFiles = all.inputFiles();
        int threshold = (int) (allFiles.length * c.coverageThresholdFraction);
        runInfo.put("data_files", allFiles.length);
        runInfo.put("coverage_threshold_files", threshold);

        warmUp(spark, c, trace, runInfo);

        CacheStore store = new CacheStore(c.capacityBytes, c.w1, c.w2);
        Map<String, String> plans = new LinkedHashMap<>();

        for (TraceQuery q : trace) {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("seq", q.seq);
            row.put("klass", q.klass);
            row.put("method", c.method.name());

            Benchmark.ConditionValues cur = q.toCondition(null);
            long start = System.nanoTime();

            // lookup
            List<String> cachedFiles = null;
            String cachedResult = null;
            if (c.method.containment()) cachedFiles = store.lookupContained(cur);
            else if (c.method == Method.EXACT_FILE) cachedFiles = store.lookupExact(cur);
            else if (c.method == Method.NAIVE_CACHE) {
                List<String> found = store.lookupExact(cur);
                cachedResult = found == null ? null : found.get(0);
            }
            long lookupDone = System.nanoTime();

            boolean hit = cachedFiles != null || cachedResult != null;
            boolean exactHit = hit && store.hasExact(cur);
            row.put("hit_type", !hit ? (c.method == Method.NO_CACHE ? "NONE" : "MISS") : exactHit ? "EXACT" : "CONTAINED");

            long count = 0;
            String sumValue = null;
            Map<String, double[]> groups = new TreeMap<>();
            long readNs = 0, execNs = 0, coverageNs = 0, insertNs = 0;
            int filesPlanned = 0;
            PhaseMetrics.Totals scan = new PhaseMetrics.Totals();
            PhaseMetrics.Totals coverage = new PhaseMetrics.Totals();
            int coverageSize = -1;
            String coverageSha = null;
            int evicted = 0;
            boolean inserted = false;

            if (cachedResult != null) {
                String[] parts = cachedResult.split("\\|", -1);
                count = Long.parseLong(parts[0]);
                sumValue = parts[1].isEmpty() ? null : parts[1];
                groups = decodeGroups(parts[2]);
            } else if (cachedFiles == null || !cachedFiles.isEmpty()) {
                // an empty cached coverage proves the contained query is empty, so that case reads nothing
                String scanGroup = "q" + q.seq + ":scan";
                spark.sparkContext().setJobGroup(scanGroup, scanGroup, false);
                long t0 = System.nanoTime();
                Dataset<Row> lineItem = cachedFiles != null
                        ? spark.read().parquet(cachedFiles.toArray(new String[0]))
                        : spark.read().parquet(c.data);
                Dataset<Row> filtered = lineItem.where(queryCondition(q));
                // an exact hit already has tight coverage, so the one-scan methods have nothing to learn from it
                boolean byFile = c.method.inlineCoverage() && !exactHit;
                Dataset<Row> result = aggregate(filtered, q, byFile);
                readNs = System.nanoTime() - t0;

                long t2 = System.nanoTime();
                List<Row> rows = result.collectAsList();
                Double total = null;
                Set<String> files = new HashSet<>();
                for (Row r : rows) {
                    long n = r.getLong(r.fieldIndex("pcc_n"));
                    count += n;
                    int s = r.fieldIndex("pcc_sum");
                    double value = r.isNullAt(s) ? 0.0 : r.getDouble(s);
                    if (!r.isNullAt(s)) total = (total == null ? 0.0 : total) + value;
                    if (byFile && n > 0) files.add(r.getString(r.fieldIndex("pcc_file")));
                    if (q.isQ1()) {
                        double[] g = groups.computeIfAbsent(r.getString(r.fieldIndex("l_returnflag")) + r.getString(r.fieldIndex("l_linestatus")), k -> new double[2]);
                        g[0] += n;
                        g[1] += value;
                    }
                }
                sumValue = total == null ? null : total.toString();
                execNs = System.nanoTime() - t2;
                scan = drain(spark, metrics, scanGroup);
                filesPlanned = cachedFiles != null ? cachedFiles.size() : allFiles.length;

                String shape = (q.isQ1() ? "q1_" : "q6_") + (cachedFiles != null ? (exactHit ? "exact_hit" : "contained_hit") : "miss");
                if (!plans.containsKey(shape)) plans.put(shape, plan(result));

                if (c.method.cachesCoverage() && (byFile || !c.method.inlineCoverage())) {
                    List<String> curFiles;
                    if (byFile) {
                        curFiles = new ArrayList<>(files);
                    } else {
                        String coverageGroup = "q" + q.seq + ":coverage";
                        spark.sparkContext().setJobGroup(coverageGroup, coverageGroup, false);
                        long t3 = System.nanoTime();
                        Dataset<String> job = filtered.select(input_file_name()).distinct().as(Encoders.STRING());
                        curFiles = job.collectAsList();
                        coverageNs = System.nanoTime() - t3;
                        coverage = drain(spark, metrics, coverageGroup);
                        if (!plans.containsKey("coverage")) plans.put("coverage", plan(job));
                    }
                    coverageSize = curFiles.size();
                    coverageSha = coverageHash(curFiles);

                    // containment keeps the original rule; the exact control stores any miss, as an exact cache would
                    boolean admit = c.method.containment()
                            ? coverageSize <= threshold && (cachedFiles == null || cachedFiles.size() != coverageSize)
                            : cachedFiles == null;
                    long t4 = System.nanoTime();
                    if (admit) {
                        Benchmark.ConditionValues entry = q.toCondition(curFiles);
                        evicted = store.put(entry);
                        inserted = store.holds(entry);
                    }
                    insertNs = System.nanoTime() - t4;
                }
            }

            if (c.method == Method.NAIVE_CACHE && cachedResult == null) {
                long t4 = System.nanoTime();
                Benchmark.ConditionValues entry = q.toCondition(
                        Collections.singletonList(count + "|" + (sumValue == null ? "" : sumValue) + "|" + encodeGroups(groups)));
                evicted = store.put(entry);
                inserted = store.holds(entry);
                insertNs = System.nanoTime() - t4;
            }

            row.put("result_count", count);
            row.put("result_sum", sumValue);
            row.put("result_groups", groups);
            row.put("files_planned", filesPlanned);
            row.put("coverage_size", coverageSize);
            row.put("coverage_sha256", coverageSha);
            row.put("inserted", inserted);
            row.put("evicted", evicted);
            row.put("lookup_ns", lookupDone - start);
            row.put("read_ns", readNs);
            row.put("exec_ns", execNs);
            row.put("coverage_ns", coverageNs);
            row.put("insert_evict_ns", insertNs);
            // sum of the timed phases, so reporting work (classification, plan capture, metric draining) stays out
            row.put("workload_ns", (lookupDone - start) + readNs + execNs + coverageNs + insertNs);
            row.put("latency_ns", (lookupDone - start) + readNs + execNs);
            row.put("scan_bytes_read", scan.bytesRead);
            row.put("scan_records_read", scan.recordsRead);
            row.put("scan_tasks", scan.tasks);
            row.put("scan_executor_ms", scan.executorRunMs);
            row.put("scan_executor_cpu_ms", scan.executorCpuNs / 1_000_000);
            row.put("scan_gc_ms", scan.gcMs);
            row.put("coverage_bytes_read", coverage.bytesRead);
            row.put("coverage_records_read", coverage.recordsRead);
            row.put("coverage_executor_ms", coverage.executorRunMs);
            row.put("cache_entries", store.entries.size());
            row.put("cache_bytes", store.usedBytes());
            row.put("cache_heap_bytes", SizeEstimator.estimate(store.entries));
            sink.accept(row);
        }

        spark.sparkContext().removeSparkListener(metrics);
        runInfo.put("evictions", store.evictions);
        runInfo.put("rejected_too_large", store.rejectedTooLarge);
        runInfo.put("plans", plans);
    }

    private static String plan(Dataset<?> executed) {
        return executed.queryExecution().explainString(ExplainMode.fromString("formatted"));
    }

    static void flushListeners(SparkSession spark) {
        try {
            spark.sparkContext().listenerBus().waitUntilEmpty();
        } catch (java.util.concurrent.TimeoutException e) {
            throw new IllegalStateException("Spark listener bus did not drain", e);
        }
    }

    private static PhaseMetrics.Totals drain(SparkSession spark, PhaseMetrics metrics, String group) {
        flushListeners(spark);
        spark.sparkContext().clearJobGroup();
        return metrics.take(group);
    }

    /** Same unmeasured work for every method: every plan shape any method uses, on one fixed predicate that touches no cache. */
    private static void warmUp(SparkSession spark, Config c, List<TraceQuery> trace, Map<String, Object> runInfo) {
        TraceQuery warm = new TraceQuery(-1, "warmup", "1995-06-01", "1995-06-30", 0.02, 0.04, 1, 10);
        TraceQuery first = trace.isEmpty() ? warm : trace.get(0);
        TraceQuery shaped = new TraceQuery(-1, "warmup", warm.shipFrom, warm.shipTo, warm.discountFrom, warm.discountTo,
                warm.quantityFrom, warm.quantityTo, first.template);
        List<Long> warmupMs = new ArrayList<>();
        long t = System.nanoTime();
        Dataset<Row> filtered = spark.read().parquet(c.data).where(queryCondition(shaped));
        aggregate(filtered, shaped, false).collectAsList();
        warmupMs.add((System.nanoTime() - t) / 1_000_000);
        t = System.nanoTime();
        aggregate(filtered, shaped, true).collectAsList();
        warmupMs.add((System.nanoTime() - t) / 1_000_000);
        t = System.nanoTime();
        filtered.select(input_file_name()).distinct().as(Encoders.STRING()).collectAsList();
        warmupMs.add((System.nanoTime() - t) / 1_000_000);
        runInfo.put("warmup_ms", warmupMs);
    }

    private static Map<String, String> parseArgs(String[] args) {
        Map<String, String> m = new LinkedHashMap<>();
        for (int i = 0; i + 1 < args.length; i += 2) {
            if (!args[i].startsWith("--")) throw new IllegalArgumentException("Expected --name value, got " + args[i]);
            m.put(args[i].substring(2), args[i + 1]);
        }
        for (String required : new String[]{"method", "data", "trace", "out"}) {
            if (!m.containsKey(required)) throw new IllegalArgumentException("Missing --" + required);
        }
        return m;
    }

    private static List<String> readLines(String path, Configuration conf) throws IOException {
        Path p = new Path(path);
        List<String> lines = new ArrayList<>();
        try (BufferedReader r = new BufferedReader(new InputStreamReader(p.getFileSystem(conf).open(p), StandardCharsets.UTF_8))) {
            for (String line = r.readLine(); line != null; line = r.readLine()) lines.add(line);
        }
        return lines;
    }

    static String sha256(String text) {
        try {
            byte[] d = MessageDigest.getInstance("SHA-256").digest(text.getBytes(StandardCharsets.UTF_8));
            StringBuilder hex = new StringBuilder();
            for (byte b : d) hex.append(String.format("%02x", b));
            return hex.toString();
        } catch (java.security.NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }

    private static void writeJson(String path, Object value, Configuration conf) throws IOException {
        Path p = new Path(path);
        FileSystem fs = p.getFileSystem(conf);
        byte[] text = new ObjectMapper().writerWithDefaultPrettyPrinter().writeValueAsString(value).getBytes(StandardCharsets.UTF_8);
        try (FSDataOutputStream out = fs.create(p, true)) {
            out.write(text);
        }
    }
}
