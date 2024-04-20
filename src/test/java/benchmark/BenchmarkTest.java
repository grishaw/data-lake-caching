package benchmark;

import jdk.nashorn.internal.ir.annotations.Ignore;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;

import static benchmark.Benchmark.runFullBenchmark;

public class BenchmarkTest {

    @Test
    @Ignore
    public void myBenchmark(){

        SparkSession sparkSession = TestUtils.initTestSparkSession("myBenchmark");
        sparkSession.sparkContext().setLogLevel("ERROR");

        int benchmarksNum = 3;
        int iterationsNum = 10;
        int coverageUpperThreshold = 5000;

        int quantityToMax = 3;
        double discountToMax = 0.03;

        String datalakePath = "src/test/resources/tpch/lineitem-10k/";

        runFullBenchmark(benchmarksNum, sparkSession, datalakePath, iterationsNum, coverageUpperThreshold, quantityToMax, discountToMax);
    }

}
