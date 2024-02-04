import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
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

        Dataset lineItem = sparkSession.read().parquet("src/test/resources/tpch/lineitem-10k/");

        LinkedList <Long> list = new LinkedList<>();
        for (int i=0; i<20; i++){

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

            Dataset result = lineItem.where(getQueryCondition(extendedPriceFrom, extendedPriceTo,
                    shipDateFrom, shipDateTo, discountFrom, discountTo,
                    quantityFrom, quantityTo));

            long currentCoverage = result.select(input_file_name()).distinct().count();
            System.out.println("i=" + i + ", coverage = " + currentCoverage);

            list.add(currentCoverage);
        }

        System.out.println(list);
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


}
