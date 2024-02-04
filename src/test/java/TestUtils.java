import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

public class TestUtils {

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
