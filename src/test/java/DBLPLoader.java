import jdk.nashorn.internal.ir.annotations.Ignore;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.junit.jupiter.api.Test;

public class DBLPLoader {

    @Ignore
    public void testDBLP_Articles(){
        SparkSession spark = TestUtils.initTestSparkSession("myTest");

        Dataset df = spark.read()
                .format("xml")
                .option("rowTag", "article")
                .option("inferSchema", "false")
                .option("excludeAttribute", "true")
                .load("/Users/grishaw/Downloads/dblp.xml.gz");

        df.repartition(10).write().parquet("/Users/grishaw/Downloads/dblp-parquet-10-ignore-schema-exclude-attr/");
    }

    @Ignore
    public void testDBLP_Conferences(){
        SparkSession spark = TestUtils.initTestSparkSession("myTest");

        Dataset df = spark.read()
                .format("xml")
                .option("rowTag", "inproceedings")
                .option("inferSchema", "false")
                .option("excludeAttribute", "true")
                .load("/Users/grishaw/Downloads/dblp.xml.gz");

        df.repartition(10).write().parquet("/Users/grishaw/Downloads/dblp-parquet-10-conf/");
    }

    @Ignore
    public void combineDBLP(){
        SparkSession spark = TestUtils.initTestSparkSession("myTest");

        Dataset conf = spark.read().parquet("src/test/resources/dblp-parquet-10-conf");
        conf = conf.withColumn("type", functions.lit("conference"));
        conf = conf.select("author", "title", "booktitle", "year", "type");
        conf = conf.where("author is not null and title is not null and booktitle is not null and year is not null");

        Dataset journals = spark.read().parquet("src/test/resources/dblp-parquet-10-journals");
        journals = journals.withColumn("type", functions.lit("journal"));
        journals = journals.select("author", "title", "journal", "year", "type");
        journals = journals.withColumnRenamed("journal", "booktitle");
        journals = journals.where("author is not null and title is not null and booktitle is not null and year is not null");

        Dataset result = conf.union(journals);

        result.repartition(1000).write().parquet("target/dblp-1000/");
    }

    @Test
    public void testParquet(){
        SparkSession spark = TestUtils.initTestSparkSession("myTest");

        Dataset conf = spark.read().parquet("src/test/resources/dblp-1000");
        conf.printSchema();
        conf.show();
        System.out.println("total count = " + conf.count());
    }


}
