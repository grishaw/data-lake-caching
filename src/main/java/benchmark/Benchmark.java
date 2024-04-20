package benchmark;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;

import java.time.LocalDate;
import java.util.Arrays;
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

        runFullBenchmark(benchmarksNum, sparkSession, datalakePath, iterationsNum, coverageUpperThreshold, quantityToMax, discountToMax);
    }

    static void runFullBenchmark(int benchmarksNum, SparkSession sparkSession, String datalakePath, int iterationsNum, int coverageUpperThreshold, int quantityToMax, double discountToMax){
        long cacheTotal = 0;
        long nonCacheTotal = 0;
        long naiveCacheTotal = 0;

        for (int i=1; i<=benchmarksNum; i++) {
            System.out.println("benchmark " + i + " with coverage cache");
            cacheTotal += benchmark(sparkSession, datalakePath, Benchmark.CacheMode.COVERAGE_CACHE, iterationsNum, coverageUpperThreshold, quantityToMax, discountToMax);
            System.out.println("benchmark " + i + " without cache");
            nonCacheTotal += benchmark(sparkSession, datalakePath, Benchmark.CacheMode.NO_CACHE, iterationsNum, coverageUpperThreshold, quantityToMax, discountToMax);
            System.out.println("benchmark " + i + " with naive cache");
            naiveCacheTotal += benchmark(sparkSession, datalakePath, Benchmark.CacheMode.NAIVE_CACHE, iterationsNum, coverageUpperThreshold, quantityToMax, discountToMax);
        }

        System.out.println("Bottom line: cache = " + cacheTotal / benchmarksNum + ", non-cache = " + nonCacheTotal / benchmarksNum + ", naive-cache = " + naiveCacheTotal / benchmarksNum);
    }

    static long benchmark(SparkSession sparkSession, String datalakePath, CacheMode cacheMode, int iterationsNum, int coverageUpperThreshold,
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

            Dataset lineItem;
            Dataset result;
            List<String> cachedFiles = null;
            Double resultValue = null;
            if (cacheMode == CacheMode.NO_CACHE){
                lineItem = sparkSession.read().parquet(datalakePath);
                result = lineItem.where(getQueryCondition(shipDateFrom, shipDateTo, discountFrom, discountTo, quantityFrom, quantityTo));
                resultValue = (double) result.agg(sum(col("l_extendedprice").multiply(col("l_discount")))).as(Encoders.DOUBLE()).collectAsList().get(0);
                System.out.println("no-cache result = " + resultValue);
            }else if (cacheMode == CacheMode.NAIVE_CACHE){
                lineItem = sparkSession.read().parquet(datalakePath);
                result = lineItem.where(getQueryCondition(shipDateFrom, shipDateTo, discountFrom, discountTo, quantityFrom, quantityTo));
                String cachedResult = getResultFromCache(cache, cur);
                if (cachedResult != null){
                    cacheHits++;
                    System.out.println("naively cached result = " + cachedResult);
                }else{
                    resultValue = (double) result.agg(sum(col("l_extendedprice").multiply(col("l_discount")))).as(Encoders.DOUBLE()).collectAsList().get(0);
                    System.out.println("naively non-cached result = " + resultValue);
                }
            }else if (cacheMode == CacheMode.COVERAGE_CACHE){
                cachedFiles = getFilesFromCache(cache, cur);
                if (cachedFiles != null) {
                    cacheHits++;
                    if (cachedFiles.isEmpty()) {
                        queryCoverageSizes.add(0L);
                        queryTimes.add((System.currentTimeMillis() - startTime) / 1000);
                        usedCachedCoverageSizes.add(0);
                        continue;
                    }
                    lineItem = sparkSession.read().parquet(cachedFiles.toArray(new String[0]));
                }else{
                    lineItem = sparkSession.read().parquet(datalakePath);
                }
                result = lineItem.where(getQueryCondition(shipDateFrom, shipDateTo, discountFrom, discountTo, quantityFrom, quantityTo));
                resultValue = (double) result.agg(sum(col("l_extendedprice").multiply(col("l_discount")))).as(Encoders.DOUBLE()).collectAsList().get(0);
                System.out.println("covered cached result = " + resultValue);

            }else{
                throw new IllegalArgumentException("Invalid cache mode");
            }


            long endTime = System.currentTimeMillis();
            queryTimes.add((endTime-startTime)/1000);

            List<String> curFiles = result.select(input_file_name()).distinct().as(Encoders.STRING()).collectAsList();
            long currentCoverageSize = curFiles.size();
            queryCoverageSizes.add(currentCoverageSize);
            System.out.println("coverage-size=" + currentCoverageSize);

            if (currentCoverageSize <= coverageUpperThreshold && cacheMode == CacheMode.COVERAGE_CACHE && (cachedFiles == null || cachedFiles.size() != currentCoverageSize)) {
                //TODO don't add duplicate values
                cache.add(new ConditionValues(shipDateFrom, shipDateTo, discountFrom, discountTo, quantityFrom, quantityTo, curFiles));
            }else if (cacheMode == CacheMode.NAIVE_CACHE){
                cache.add(new ConditionValues(shipDateFrom, shipDateTo, discountFrom, discountTo, quantityFrom, quantityTo,
                        Arrays.asList(resultValue == null ? null : String.valueOf(resultValue))));
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

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            ConditionValues that = (ConditionValues) o;

            if (Double.compare(discountFrom, that.discountFrom) != 0) return false;
            if (Double.compare(discountTo, that.discountTo) != 0) return false;
            if (quantityFrom != that.quantityFrom) return false;
            if (quantityTo != that.quantityTo) return false;
            if (!shipDateFrom.equals(that.shipDateFrom)) return false;
            return shipDateTo.equals(that.shipDateTo);
        }

        @Override
        public int hashCode() {
            int result;
            long temp;
            result = shipDateFrom.hashCode();
            result = 31 * result + shipDateTo.hashCode();
            temp = Double.doubleToLongBits(discountFrom);
            result = 31 * result + (int) (temp ^ (temp >>> 32));
            temp = Double.doubleToLongBits(discountTo);
            result = 31 * result + (int) (temp ^ (temp >>> 32));
            result = 31 * result + quantityFrom;
            result = 31 * result + quantityTo;
            return result;
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

    static String getResultFromCache(List<ConditionValues> cache, ConditionValues current){

        for (ConditionValues cached : cache){
            if (cached.equals(current)){
                return cached.files.get(0);
            }
        }

        return null;
    }

    enum CacheMode{
        NO_CACHE,
        NAIVE_CACHE,
        COVERAGE_CACHE
    }

}
