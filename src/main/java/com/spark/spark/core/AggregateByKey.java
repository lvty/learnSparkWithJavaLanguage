package com.spark.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;

public class AggregateByKey {
    public static void main(String[] args) {
        JavaSparkContext sc = new JavaSparkContext(new SparkConf().setAppName("").setMaster("local[2]"));
        JavaRDD<String> inRDD = sc.textFile("F:\\idea\\learnSparkWithJavaLanguage\\src\\data\\wc.txt");
        JavaPairRDD<String, Integer> temp = inRDD.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.split(","));
            }
        }).mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        })
        //aggregateByKey可以自己控制如何对每个partition中的数据进行先聚合，然后才会对所有的partition中的数据进行全局聚合
         .aggregateByKey(0, new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        }, new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer integer, Integer integer2) throws Exception {
                return integer + integer2;
            }
        });

        temp.mapToPair(new PairFunction<Tuple2<String,Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> t) throws Exception {
                return new Tuple2<Integer, String>(t._2,t._1);
            }
        }).sortByKey(false).mapToPair(new PairFunction<Tuple2<Integer,String>, String, Integer>() {
              @Override
              public Tuple2<String, Integer> call(Tuple2<Integer, String> t) throws Exception {
                  return new Tuple2<String, Integer> (t._2,t._1);
              }
          }).foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> t) throws Exception {
                System.out.println("word: " + t._1 + "  count: " + t._2);
            }
        });


        sc.close();
    }
}
