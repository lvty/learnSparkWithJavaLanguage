package com.spark.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.List;

public class Top3 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("LineCountLocal");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> inputRDD = sc.textFile("F:\\idea\\learnSparkWithJavaLanguage\\src\\top.txt");
        JavaPairRDD<Integer, String> pairRDD = inputRDD.mapToPair(new PairFunction<String, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(String s) throws Exception {
                return new Tuple2<Integer, String>(Integer.valueOf(s), s);
            }
        });

        JavaPairRDD<Integer, String> sorted = pairRDD.sortByKey(false);
        JavaRDD<String> res = sorted.map(new Function<Tuple2<Integer, String>, String>() {
            @Override
            public String call(Tuple2<Integer, String> integerStringTuple2) throws Exception {
                return integerStringTuple2._2;
            }
        });

        List<String> list = res.take(3);
        for(String s:list){
            System.out.println(s);
        }


        sc.close();
    }
}
