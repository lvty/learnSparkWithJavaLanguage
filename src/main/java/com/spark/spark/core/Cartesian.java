package com.spark.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class Cartesian {
    public static void main(String[] args) {
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("").setMaster("local[2]"));

        List<Integer> list1 = Arrays.asList(1,2,3);
        List<Integer> list2 = Arrays.asList(5,6,7);
        //Cartesian  两个RDD的每一条数据都会与另外一个RDD的每一条数据执行join操作
        JavaRDD<Integer> studentsRDD1 = sc.parallelize(list1);
        JavaRDD<Integer> studentsRDD2 = sc.parallelize(list2);
        studentsRDD1.cartesian(studentsRDD2).foreach(new VoidFunction<Tuple2<Integer, Integer>>() {
            @Override
            public void call(Tuple2<Integer, Integer> t) throws Exception {
                System.out.println("a: " + t._1 + " b: " + t._2);
            }
        });


        sc.close();
    }
}
