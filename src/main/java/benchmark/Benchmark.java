package benchmark;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import java.time.LocalDate;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.spark.sql.functions.*;
import static org.apache.spark.sql.functions.col;

public class Benchmark {

    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder().appName("benchmark").getOrCreate();

        String datalakePath = args[0];

        int iterationsNum = Integer.parseInt(args[1]);
        int coverageUpperThreshold = Integer.parseInt(args[2]);
        int benchmarksNum = Integer.parseInt(args[3]);
        int quantityToMax = Integer.parseInt(args[4]);
        double discountToMax = Double.parseDouble(args[5]);

        System.out.println("iterations = " + iterationsNum);
        System.out.println("coverageUpperThreshold = " + coverageUpperThreshold);
        System.out.println("benchmarksNum = " + benchmarksNum);
        System.out.println("quantityToMax = " + quantityToMax);
        System.out.println("discountToMax = " + discountToMax);

        long cacheTotal = 0;
        long nonCacheTotal = 0;

        for (int i=1; i<=benchmarksNum; i++) {
            System.out.println("benchmark " + i + " with cache");
            cacheTotal += benchmark(sparkSession, datalakePath, true, iterationsNum, coverageUpperThreshold, quantityToMax, discountToMax);
            System.out.println("benchmark " + i + " without cache");
            nonCacheTotal += benchmark(sparkSession, datalakePath, false, iterationsNum, coverageUpperThreshold, quantityToMax, discountToMax);
        }

        System.out.println("Bottom line: cache = " + cacheTotal / benchmarksNum + ", non-cache = " + nonCacheTotal / benchmarksNum);
    }

    static long benchmark(SparkSession sparkSession, String datalakePath, boolean withCache, int iterationsNum, int coverageUpperThreshold,
                          int quantityToMax, double discountToMax){

        LinkedList<Long> queryCoverageSizes = new LinkedList<>();
        LinkedList <Long> queryTimes = new LinkedList<>();
        LinkedList <Integer> usedCachedCoverageSizes = new LinkedList<>();

        LinkedList <ConditionValues> cache = new LinkedList<>();

        int cacheHits = 0;

        // warm up
        sparkSession.read().parquet(datalakePath).show();

        for (int i=0; i<iterationsNum; i++){

            long startTime = System.currentTimeMillis();

            int quantityFrom = 1;
            int quantityTo = ThreadLocalRandom.current().nextInt(1,quantityToMax);

            int year = ThreadLocalRandom.current().nextInt(1993,1998);
            String shipDateFrom = LocalDate.of(year, 1, 1).toString();
            String shipDateTo = LocalDate.of(year + 1, 1, 1).toString();

            double discountFrom = 0.00;
            double discountTo = ThreadLocalRandom.current().nextDouble(0.00, discountToMax);

            ConditionValues cur = new ConditionValues(shipDateFrom, shipDateTo, discountFrom, discountTo, quantityFrom, quantityTo, null);

            List<String> cachedFiles = withCache ? getFilesFromCache(cache, cur) : null;

            Dataset lineItem;
            if (cachedFiles == null) {
                lineItem = sparkSession.read().parquet(datalakePath);
            }else{
                cacheHits++;
                if (cachedFiles.isEmpty()){
                    queryCoverageSizes.add(0L);
                    queryTimes.add((System.currentTimeMillis()-startTime)/1000);
                    usedCachedCoverageSizes.add(0);
                    continue;
                }
                lineItem = sparkSession.read().parquet(cachedFiles.toArray(new String[0]));
            }

            Dataset result = lineItem.where(getQueryCondition(shipDateFrom, shipDateTo, discountFrom, discountTo, quantityFrom, quantityTo));
            result.agg(sum(col("l_extendedprice").multiply(col("l_discount")))).show();

            long endTime = System.currentTimeMillis();
            queryTimes.add((endTime-startTime)/1000);

            List<String> curFiles = result.select(input_file_name()).distinct().as(Encoders.STRING()).collectAsList();
            long currentCoverageSize = curFiles.size();
            queryCoverageSizes.add(currentCoverageSize);

            if (currentCoverageSize <= coverageUpperThreshold && withCache && (cachedFiles == null || cachedFiles.size() != currentCoverageSize)) {
                //TODO don't add duplicate values
                cache.add(new ConditionValues(shipDateFrom, shipDateTo, discountFrom, discountTo, quantityFrom, quantityTo, curFiles));
            }

            usedCachedCoverageSizes.add(cachedFiles == null ? null : cachedFiles.size());
        }

        System.out.println("coverage sizes = " + queryCoverageSizes);
        System.out.println("query times = " + queryTimes);
        System.out.println("cached coverage size used = " + usedCachedCoverageSizes);

        long totalTime = queryTimes.stream().mapToLong(Long::longValue).sum();
        System.out.println("total time : " + totalTime + " sec");
        System.out.println("final cache size : " + cache.size());
        System.out.println("cache hits: " + cacheHits);

        System.out.println("------------------------------");

        return totalTime;
    }

    private static Column getQueryCondition(String shipDateFrom, String shipDateTo,
                                            double discountFrom, double discountTo,
                                            int quantityFrom, int quantityTo){
        return
                col("l_shipdate").geq(shipDateFrom).and(col("l_shipdate").leq(shipDateTo))
                        .and(col("l_discount").geq(discountFrom)).and(col("l_discount").leq(discountTo))
                        .and(col("l_quantity").geq(quantityFrom)).and(col("l_quantity").leq(quantityTo));
    }

    static class ConditionValues{
        String shipDateFrom;
        String shipDateTo;
        double discountFrom;
        double discountTo;
        int quantityFrom;
        int quantityTo;
        List<String> files;

        public ConditionValues(String shipDateFrom, String shipDateTo, double discountFrom, double discountTo, int quantityFrom, int quantityTo, List<String> files) {
            this.shipDateFrom = shipDateFrom;
            this.shipDateTo = shipDateTo;
            this.discountFrom = discountFrom;
            this.discountTo = discountTo;
            this.quantityFrom = quantityFrom;
            this.quantityTo = quantityTo;
            this.files = files;
        }

        boolean contains(ConditionValues other){
            return  other.discountFrom >= this.discountFrom && other.discountTo <= this.discountTo &&
                    other.shipDateFrom.compareTo(this.shipDateFrom) >= 0 && other.shipDateTo.compareTo(this.shipDateTo) <= 0 &&
                    other.quantityFrom >= this.quantityFrom && other.quantityTo <= this.quantityTo;
        }
    }

    static List<String> getFilesFromCache(List<ConditionValues> cache, ConditionValues current){

        List<String> result = null;

        for (ConditionValues cached : cache){
            if (cached.contains(current) && (result == null || result.size() > cached.files.size())){
                result = cached.files;
            }
        }

        return result;
    }


}
