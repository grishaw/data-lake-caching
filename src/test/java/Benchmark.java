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

        sparkSession.sparkContext().setLogLevel("ERROR");
        LinkedList <Long> listCoverage = new LinkedList<>();
        LinkedList <Long> listTimes = new LinkedList<>();
        LinkedList <ConditionValues> cache = new LinkedList<>();
        LinkedList <Integer> cachedUsed = new LinkedList<>();

        for (int i=0; i<20; i++){

            long startTime = System.currentTimeMillis();

            int extendedPriceFrom = 901;
            int extendedPriceTo = 104950;

            int quantityFrom = 1;
            int quantityTo = ThreadLocalRandom.current().nextInt(2,3);

            int year = ThreadLocalRandom.current().nextInt(1993,1998);
            String shipDateFrom = LocalDate.of(year, 1, 1).toString();
            String shipDateTo = LocalDate.of(year+1, 1, 1).toString();

            double discountFrom = 0.00;
            double discountTo = ThreadLocalRandom.current().nextDouble(0.00, 0.01);

            ConditionValues cur = new ConditionValues(extendedPriceFrom, extendedPriceTo, shipDateFrom,
                    shipDateTo, discountFrom, discountTo, quantityFrom, quantityTo, null);

            List<String> cachedFiles = getFilesFromCache(cache, cur);

            Dataset lineItem;
            if (cachedFiles == null) {
                lineItem = sparkSession.read().parquet("src/test/resources/tpch/lineitem-10k/");
            }else{
                if (cachedFiles.isEmpty()){
                    System.out.println("cached coverage is empty !!!!!!!");
                    listCoverage.add(null);
                    listTimes.add((System.currentTimeMillis()-startTime)/1000);
                    cachedUsed.add(-1);
                    continue;
                }
                lineItem = sparkSession.read().parquet(cachedFiles.toArray(new String[0]));
            }

            Dataset result = lineItem.where(getQueryCondition(extendedPriceFrom, extendedPriceTo,
                    shipDateFrom, shipDateTo, discountFrom, discountTo,
                    quantityFrom, quantityTo));

            List<String> curFiles = result.select(input_file_name()).distinct().as(Encoders.STRING()).collectAsList();
            long currentCoverage = curFiles.size();
            System.out.println("i=" + i + ", coverage = " + currentCoverage);

            listCoverage.add(currentCoverage);

            if (curFiles.size() <= 5000 && (cachedFiles == null || cachedFiles.size() != curFiles.size())) {
                cache.add(new ConditionValues(extendedPriceFrom, extendedPriceTo, shipDateFrom,
                        shipDateTo, discountFrom, discountTo, quantityFrom, quantityTo, curFiles));
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
                    && (result == null || result.size() > cond.files.size())
            ){
                    result = cond.files;
            }
        }

        if (result != null){
            System.out.println("using cache coverage of size: " + result.size());
        }
        return result;
    }

}
