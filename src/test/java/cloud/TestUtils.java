package cloud;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

public class TestUtils {

    public static SparkSession initTestSparkSession(String appName) {

        Logger.getLogger("org.apache").setLevel(Level.WARN);

        SparkConf sparkConf = new SparkConf()
                .setAppName(appName)
                .setMaster("local[4]")
                .set("spark.driver.host", "localhost");

        return SparkSession.builder()
                .config(sparkConf)
                .getOrCreate();
    }


}
