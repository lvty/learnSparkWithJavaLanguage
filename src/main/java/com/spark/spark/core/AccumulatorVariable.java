package com.spark.spark.core;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.VoidFunction;

import java.util.Arrays;
import java.util.List;

public class AccumulatorVariable {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("LineCountLocal");
        JavaSparkContext sc = new JavaSparkContext(conf);

        List<Integer> list = Arrays.asList(1,2,3,4,5);
        final Accumulator<Integer> accum = sc.accumulator(0);
        JavaRDD<Integer> rdd = sc.parallelize(list,2);
        rdd.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                accum.add(integer);
            }
        });

        System.out.println(accum.value());

        sc.close();
    }
}
