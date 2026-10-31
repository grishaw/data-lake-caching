package cloud;

import org.apache.spark.SparkFiles;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.pmod;
import static org.apache.spark.sql.functions.xxhash64;

/**
 * Generates lineitem with dbgen chunks and writes Parquet layouts.
 * Args: sf chunks stageUri outUri kind:files,kind:files
 * The original layout script is missing, so every layout here is ours:
 *   random    rows spread uniformly across files
 *   sorted    files hold tight ship-date ranges, so Parquet min/max statistics can skip files
 *   skewqty   random, with l_quantity thinned to a Zipf-like distribution (keep with probability 1/quantity)
 */
public final class PrepareData {

    // DOUBLE, not the spec's DECIMAL: the unchanged cloud.Benchmark fails on decimal sums (Encoders.DOUBLE cannot up-cast them)
    private static final String SCHEMA = "l_orderkey BIGINT, l_partkey BIGINT, l_suppkey BIGINT, l_linenumber INT, "
            + "l_quantity DOUBLE, l_extendedprice DOUBLE, l_discount DOUBLE, l_tax DOUBLE, "
            + "l_returnflag STRING, l_linestatus STRING, l_shipdate DATE, l_commitdate DATE, l_receiptdate DATE, "
            + "l_shipinstruct STRING, l_shipmode STRING, l_comment STRING, trailing_field STRING";

    private static void run(File cwd, java.util.Map<String, String> env, String... command) throws IOException, InterruptedException {
        ProcessBuilder b = new ProcessBuilder(command).directory(cwd).inheritIO();
        b.environment().putAll(env);
        int code = b.start().waitFor();
        if (code != 0) throw new IOException("exit " + code + " from " + String.join(" ", command));
    }

    private static void generateChunk(int i, String sf, int chunks, String stage) throws IOException, InterruptedException {
        String dbgen = SparkFiles.get("dbgen");
        new File(dbgen).setExecutable(true);
        java.util.Map<String, String> env = new java.util.HashMap<>();
        env.put("DSS_CONFIG", new File(SparkFiles.get("dists.dss")).getParent());
        Path work = Files.createTempDirectory("dbgen");
        List<String> args = new ArrayList<>(java.util.Arrays.asList(dbgen, "-q", "-f", "-s", sf, "-T", "L"));
        if (chunks > 1) {
            args.addAll(java.util.Arrays.asList("-C", String.valueOf(chunks), "-S", String.valueOf(i)));
        }
        run(work.toFile(), env, args.toArray(new String[0]));
        Path produced = work.resolve(chunks > 1 ? "lineitem.tbl." + i : "lineitem.tbl");
        String target = stage + "/lineitem/" + String.format("part-%05d.tbl", i);
        Files.createDirectories(Paths.get(target).getParent());
        Files.copy(produced, Paths.get(target), StandardCopyOption.REPLACE_EXISTING);
        try (java.util.stream.Stream<Path> s = Files.walk(work)) {
            s.sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
        }
    }

    public static void main(String[] args) {
        String sf = args[0];
        int chunks = Integer.parseInt(args[1]);
        String stage = args[2].replaceAll("/+$", "");
        String out = args[3].replaceAll("/+$", "");
        SparkSession spark = SparkSession.builder().appName("prepare-data").getOrCreate();

        List<Integer> ids = new ArrayList<>();
        for (int i = 1; i <= chunks; i++) ids.add(i);
        new JavaSparkContext(spark.sparkContext()).parallelize(ids, chunks).foreach(i -> generateChunk(i, sf, chunks, stage));

        String source = stage.contains("://") ? stage : "file://" + stage;
        Dataset<Row> df = spark.read().schema(SCHEMA).option("sep", "|").option("dateFormat", "yyyy-MM-dd")
                .csv(source + "/lineitem/").drop("trailing_field");
        Column spread = xxhash64(col("l_orderkey"), col("l_linenumber"));
        for (String token : args[4].split(",")) {
            String kind = token.split(":")[0];
            int files = Integer.parseInt(token.split(":")[1]);
            String target = out + "/" + kind + "_" + files;
            if (kind.equals("sorted")) {
                df.repartitionByRange(files, col("l_shipdate"), col("l_orderkey")).sortWithinPartitions("l_shipdate")
                        .write().mode("overwrite").parquet(target);
            } else if (kind.equals("skewqty")) {
                Column keep = pmod(spread, lit(10000)).divide(10000).lt(lit(1.0).divide(col("l_quantity")));
                df.where(keep).repartition(files, spread).sortWithinPartitions("l_orderkey", "l_linenumber")
                        .write().mode("overwrite").parquet(target);
            } else if (kind.equals("random")) {
                df.repartition(files, spread).sortWithinPartitions("l_orderkey", "l_linenumber")
                        .write().mode("overwrite").parquet(target);
            } else {
                throw new IllegalArgumentException("Unknown layout " + kind);
            }
        }
        spark.stop();
    }
}
