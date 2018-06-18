package com.spark.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.List;

public class Distinct {
    public static void main(String[] args) {
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("").setMaster("local[2]"));

        List<String> list1 = Arrays.asList(
                "user1 2018-01-01 23:55:42",
                "user1 2018-01-01 23:56:42",
                "user1 2018-01-01 23:57:42",
                "user1 2018-01-01 23:58:42",
                "user2 2018-01-01 23:50:42",
                "user2 2018-01-01 23:54:42",
                "user2 2018-01-01 23:55:42",
                "user3 2018-01-01 23:56:42"
        );

        JavaRDD<String> studentsRDD1 = sc.parallelize(list1, 2);

        studentsRDD1.map(new Function<String, String>() {
            @Override
            public String call(String s) throws Exception {
                return s.split(" ")[0];
            }
        }).distinct().foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });

        sc.close();
    }
}
