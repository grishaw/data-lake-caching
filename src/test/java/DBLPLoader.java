import jdk.nashorn.internal.ir.annotations.Ignore;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.junit.jupiter.api.Test;

public class DBLPLoader {

    @Ignore
    public void testDBLP_Articles(){
        SparkSession spark = initTestSparkSession("myTest");

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
        SparkSession spark = initTestSparkSession("myTest");

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
        SparkSession spark = initTestSparkSession("myTest");

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
        SparkSession spark = initTestSparkSession("myTest");

        Dataset conf = spark.read().parquet("src/test/resources/dblp-1000");
        conf.printSchema();
        conf.show();
        System.out.println("total count = " + conf.count());
    }


    public static SparkSession initTestSparkSession(String appName) {

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf sparkConf = new SparkConf()
                .setAppName(appName)
                .setMaster("local[*, 2]")
                .set("spark.driver.host", "localhost")
                .set("spark.sql.shuffle.partitions", "5")
                .set("spark.default.parallelism", "5")
                .set("spark.sql.autoBroadcastJoinThreshold", "-1")
                ;

        return SparkSession.builder()
                .config(sparkConf)
                .getOrCreate();
    }
}
