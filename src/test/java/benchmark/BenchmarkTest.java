package benchmark;

import jdk.nashorn.internal.ir.annotations.Ignore;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;

import static benchmark.Benchmark.benchmark;

public class BenchmarkTest {

    @Test
    @Ignore
    public void myBenchmark(){

        SparkSession sparkSession = TestUtils.initTestSparkSession("myBenchmark");
        sparkSession.sparkContext().setLogLevel("ERROR");

        int benchmarksNum = 3;
        int iterationsNum = 10;
        int coverageUpperThreshold = 5000;

        long cacheTotal = 0;
        long nonCacheTotal = 0;

        int quantityToMax = 2;
        double discountToMax = 0.02;

        String datalakePath = "src/test/resources/tpch/lineitem-10k/";

        for (int i=1; i<=benchmarksNum; i++) {
            System.out.println("benchmark " + i + " with cache");
            cacheTotal += benchmark(sparkSession, datalakePath, true, iterationsNum, coverageUpperThreshold, quantityToMax, discountToMax);
            System.out.println("benchmark " + i + " without cache");
            nonCacheTotal += benchmark(sparkSession, datalakePath, false, iterationsNum, coverageUpperThreshold, quantityToMax, discountToMax);
        }

        System.out.println("Bottom line: cache = " + cacheTotal / benchmarksNum + ", non-cache = " + nonCacheTotal / benchmarksNum);
    }

}
