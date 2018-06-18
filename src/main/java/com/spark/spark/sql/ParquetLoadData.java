
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;

public class ParquetLoadData {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("").setMaster("local[*]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);

        DataFrame df = sqlContext.read().load("hdfs://ll/..");
        DataFrame res = df.select(df.col("name"));
        res.save("hdfs://ll/res/", "json",SaveMode.Append);

    }
}
