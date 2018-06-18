
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.Arrays;
import java.util.List;

public class ParallelizeCollection {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setMaster("local").setAppName("ParallelizeCollection");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> list = Arrays.asList(1,2,3,4,5,6,7,8,9,10);
        JavaRDD<Integer> javaRDD = sc.parallelize(list, 2);
        Integer res = javaRDD.reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });

        System.out.println(res);
        sc.close();

    }
}
