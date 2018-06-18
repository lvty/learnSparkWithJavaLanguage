package com.spark.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.List;

public class Union {
    public static void main(String[] args) {
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("").setMaster("local[2]"));

        List<Integer> list1 = Arrays.asList(1,2,3,4,5);
        List<Integer> list2 = Arrays.asList(6,7,8,9,10);
        JavaRDD<Integer> studentsRDD1 = sc.parallelize(list1, 2);
        JavaRDD<Integer> studentsRDD2 = sc.parallelize(list2, 2);
        studentsRDD1.union(studentsRDD2).foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });



        sc.close();
    }
}
