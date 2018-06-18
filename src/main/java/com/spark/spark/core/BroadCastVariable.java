package com.spark.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;

import java.util.Arrays;

public class BroadCastVariable {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf().setMaster("local").setAppName("LineCountLocal");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<Integer> rdd = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5, 6, 7, 8));

        final int factor = 3;
        final Broadcast<Integer> bc = sc.broadcast(factor);
        //集合的每个数字都乘以外部定义的的那个factor
        JavaRDD<Integer> res = rdd.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer integer) throws Exception {
                return integer * (bc.value());
            }
        });

        res.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });


        sc.close();
    }
}
