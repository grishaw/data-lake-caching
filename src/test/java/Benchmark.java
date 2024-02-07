import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.Test;

import java.time.LocalDate;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.spark.sql.functions.*;

public class Benchmark {


    @Test
    public void myBenchmark(){
        SparkSession sparkSession = TestUtils.initTestSparkSession("myBenchmark");

        LinkedList <Long> listCoverage = new LinkedList<>();
        LinkedList <Long> listTimes = new LinkedList<>();
        LinkedList <ConditionValues> cache = new LinkedList<>();
        LinkedList <Integer> cachedUsed = new LinkedList<>();

        for (int i=0; i<20; i++){

            long startTime = System.currentTimeMillis();

            int p1 = ThreadLocalRandom.current().nextInt(1000, 100000);
            int p2 = ThreadLocalRandom.current().nextInt(1000, 100000);
            int extendedPriceFrom = Math.min(p1, p2);
            int extendedPriceTo = Math.max(p1, p2) + 100;

            int q1 = ThreadLocalRandom.current().nextInt(1, 50);
            int q2 = ThreadLocalRandom.current().nextInt(1, 50);
            int quantityFrom = Math.min(q1, q2);
            int quantityTo = Math.max(q1, q2)+1;

            long minDay = LocalDate.of(1992, 1, 1).toEpochDay();
            long maxDay = LocalDate.of(1998, 1, 1).toEpochDay();
            LocalDate randomDate1 = LocalDate.ofEpochDay( ThreadLocalRandom.current().nextLong(minDay, maxDay));
            LocalDate randomDate2 = LocalDate.ofEpochDay( ThreadLocalRandom.current().nextLong(minDay, maxDay));
            String shipDateFrom = randomDate1.toEpochDay() <= randomDate2.toEpochDay() ? randomDate1.toString() : randomDate2.toString();
            String shipDateTo = randomDate1.toEpochDay() > randomDate2.toEpochDay() ? randomDate1.toString() : randomDate2.toString();

            double d1 = ThreadLocalRandom.current().nextDouble(0.0, 0.1);
            double d2 = ThreadLocalRandom.current().nextDouble(0.0, 0.1);
            double discountFrom = Math.min(d1, d2);
            double discountTo = Math.max(d1, d2) + 0.01;

            ConditionValues cur = new ConditionValues(extendedPriceFrom, extendedPriceTo, shipDateFrom,
                    shipDateFrom, discountFrom, discountTo, quantityFrom, quantityTo, null);

            List<String> cachedFiles = getFilesFromCache(cache, cur);

            Dataset lineItem;
            if (cachedFiles == null) {
                lineItem = sparkSession.read().parquet("src/test/resources/tpch/lineitem-10k/");
            }else{
                System.out.println("using cache in read !!!!");
                System.out.println("cached files size = " + cachedFiles.size());
                lineItem = sparkSession.read().parquet(cachedFiles.toArray(new String[0]));
            }

            Dataset result = lineItem.where(getQueryCondition(extendedPriceFrom, extendedPriceTo,
                    shipDateFrom, shipDateTo, discountFrom, discountTo,
                    quantityFrom, quantityTo));

            List<String> curFiles = result.select(input_file_name()).distinct().as(Encoders.STRING()).collectAsList();
            long currentCoverage = curFiles.size();
            System.out.println("i=" + i + ", coverage = " + currentCoverage);

            listCoverage.add(currentCoverage);

            if (curFiles.size() <= 10000) {
                cache.add(new ConditionValues(extendedPriceFrom, extendedPriceTo, shipDateFrom,
                        shipDateFrom, discountFrom, discountTo, quantityFrom, quantityTo, curFiles));
            }

            long endTime = System.currentTimeMillis();
            listTimes.add((endTime-startTime)/1000);
            cachedUsed.add(cachedFiles == null ? null : cachedFiles.size());
        }

        System.out.println(listCoverage);
        System.out.println(listTimes);
        System.out.println(cachedUsed);

        System.out.println("total time : " + listTimes.stream().mapToLong(Long::longValue).sum());
    }

    private static Column getQueryCondition(int priceFrom, int priceTo,
                                            String shipDateFrom, String shipDateTo,
                                            double discountFrom, double discountTo,
                                            int quantityFrom, int quantityTo){
        return col("l_extendedprice").geq(priceFrom).and(col("l_extendedprice").leq(priceTo))
                .and(col("l_shipdate").geq(shipDateFrom)).and(col("l_shipdate").leq(shipDateTo))
                .and(col("l_discount").geq(discountFrom)).and(col("l_discount").leq(discountTo))
                .and(col("l_quantity").geq(quantityFrom)).and(col("l_quantity").leq(quantityTo));
    }

    static class ConditionValues{
        int priceFrom;
        int priceTo;
        String shipDateFrom;
        String shipDateTo;
        double discountFrom;
        double discountTo;
        int quantityFrom;
        int quantityTo;
        List<String> files;

        public ConditionValues(int priceFrom, int priceTo, String shipDateFrom, String shipDateTo,
                               double discountFrom, double discountTo, int quantityFrom, int quantityTo, List<String> files) {
            this.priceFrom = priceFrom;
            this.priceTo = priceTo;
            this.shipDateFrom = shipDateFrom;
            this.shipDateTo = shipDateTo;
            this.discountFrom = discountFrom;
            this.discountTo = discountTo;
            this.quantityFrom = quantityFrom;
            this.quantityTo = quantityTo;
            this.files = files;
        }
    }

    static List<String> getFilesFromCache(List<ConditionValues> cache, ConditionValues currentValues){

        List<String> result = null;
        for (ConditionValues cond : cache){
            if (currentValues.priceFrom >= cond.priceFrom && currentValues.priceTo <= cond.priceTo &&
                currentValues.discountFrom >= cond.discountFrom && currentValues.discountTo <= cond.discountTo &&
                currentValues.shipDateFrom.compareTo(cond.shipDateFrom) >= 0 && currentValues.shipDateTo.compareTo(cond.shipDateTo) <= 0 &&
                currentValues.quantityFrom >= cond.quantityFrom && currentValues.quantityTo <= cond.quantityTo
            ){
                System.out.println("found something in cache !!!");
                if (result == null || result.size() > cond.files.size()) {
                    result = cond.files;
                    System.out.println("using it !!!!");
                }
            }
        }

        return result;
    }

}
